package org.chemotext

import java.io.File
import java.io.PrintWriter
import java.io.BufferedWriter
import java.io.IOException
import java.io.FileWriter
import java.nio.file.{Paths, Files}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.json4s._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.compat.Platform
import scala.util.matching.Regex
import scala.xml.XML
import org.xml.sax.SAXParseException

/***
 * Processor for searching articles for terms.
 * Implemented as an object to interoperate with
 * Spark serialization and worker semantics.
 */
object Processor {

  val logger = LoggerFactory.getLogger("Processor")

  type WordPair = (String, String, Double, Double, Double, Int)
  type WordTriple = (String, String, String)
  type WordPosition = (String, Int, Int, Int)

  /**
    * Create a concatenated corpus of all of PubMed Central
    */
  def createCorpus (articles : Array[String], corpus : String) = {
    var out : PrintWriter = null
    try {
      logger.info ("Creating PMC text corpus")
      out = new PrintWriter (new BufferedWriter (new FileWriter (corpus)))
      articles.foreach { article =>
        logger.info (s"adding $article")
        try {
          val xml = XML.loadFile (article)
          val paragraphs = (xml \\ "p")
          paragraphs.foreach { paragraph =>
            out.println (paragraph.text)
          }
        } catch {
          case e: SAXParseException =>
            logger.error (s"Failed to parse $article")
        }
      }
    } catch {
      case e: IOException =>
        logger.error (s"Error reading file: $e")
    } finally {
      out.close ()
    }
  }

  /**
    * Calculate the vector space of the corpus as a word2vec model
    */
  def vectorizeCorpus (corpus : RDD[List[String]]) : Word2VecModel = {
    val word2vec = new Word2Vec()
    word2vec.fit (corpus)
  }

  def findTriples (article : String, meshXML : String) : List[WordTriple] = {
    findTriples (findPairs (article, meshXML))
  }

  def findTriples (pairs : List[List[WordPair]]) : List[WordTriple] = {
    var AB = pairs (0)
    var BC = pairs (1)
    logger.debug (s" Finding triples in AB/BC pair lists ${AB.length}/${BC.length} long")
    AB.flatMap { ab =>
      BC.map { bc =>
        if (ab._2.equals (bc._1)) {
          logger.debug (s" ab/bc => ${ab._1} ${ab._2} ${bc._1} ${bc._2}")
          ( ab._1, ab._2, bc._2 )
        } else {
          ( null, null, null )
        }
      }
    }.filter { item =>
      item._1 != null
    }
  }

  /**
    * Derive A->B, B->C, A->C relationships from raw word positions
    */
  def findPairs (article : String, meshXML : String) : List[List[WordPair]] = {
    findPairs (quantifyArticle (article, meshXML))
  }

  def findPairs (words : List[List[WordPosition]]) : List[List[WordPair]] = {
    val threshold = 100
    List (
      findCooccurring (words (0), words (1), threshold, 1), // AB
      findCooccurring (words (1), words (2), threshold, 2), // BC
      findCooccurring (words (0), words (2), threshold, 3)  // AC
    )
  }

  /**
    * Determine pairs based on distance and include an confidence score 
    */
  def findCooccurring (
    L : List[WordPosition],
    R : List[WordPosition],
    threshold : Int,
    code      : Int)
      : List[WordPair] = 
  {
    L.flatMap { left =>
      R.map { right =>
        val docPosDifference = math.abs (left._2 - right._2).toDouble
        if ( docPosDifference < threshold && docPosDifference > 0 ) {
          ( left._1,
            right._1,
            docPosDifference,
            math.abs (left._3 - right._3).toDouble,
            math.abs (left._4 - right._4).toDouble,
            code
          )
        } else {
          ( null, null, 0.0, 0.0, 0.0, 0 )
        }
      }
    }.filter { element =>
      element._3 != 0
    }
  }

  /**
    * Quantify an article, searching for words and producing an output 
    * vector showing locations of A, B, and C terms in the text.
    */ 
  def quantifyArticle (article : String, meshXML : String) : List[List[WordPosition]] = {
    var A : List[WordPosition] = List ()
    var B : List[WordPosition] = List ()
    var C : List[WordPosition] = List ()
    val vocab = VocabFactory.getVocabulary (meshXML)
    logger.debug (s"""Sample vocab:
         A-> ${vocab.A.slice (1, 20)}
         B-> ${vocab.B.slice (1, 20)}
         C-> ${vocab.C.slice (1, 20)}""")
    logger.info (s"@-article: ${article}")
    var docPos = 0
    var paraPos = 0
    var sentPos = 0
    try {
      val xml = XML.loadFile (article)
      val paragraphs = (xml \\ "p")
      paragraphs.foreach { paragraph =>
        val text = paragraph.text.split ("(?i)(?<=[.?!])\\S+(?=[a-z])").map (_.toLowerCase)
        A = A.union (getDocWords (vocab.A.toArray, text, docPos, paraPos, sentPos))
        B = B.union (getDocWords (vocab.B.toArray, text, docPos, paraPos, sentPos))
        C = C.union (getDocWords (vocab.C.toArray, text, docPos, paraPos, sentPos))
        sentPos += text.size
        docPos += paragraph.text.length
        paraPos += 1
      }
    } catch {
      case e: Exception =>
        logger.error (s"Error reading json $e")
    }
    logger.debug (s"B: ===> $B")
    List (A, B, C)
  }

  /**
    * Record locations of words within the document.
    */
  def getDocWords (
    words   : Array[String],
    text    : Array[String],
    docPos  : Int,
    paraPos : Int,
    sentPos : Int) : List[WordPosition] =
  {
    var textPos = 0
    var sentenceIndex = 0
    var result : ListBuffer[WordPosition] = ListBuffer ()
    if (words != null && text != null) {
      text.foreach { sentence =>
        words.foreach { word =>
          if (!word.equals ("for") && !word.equals ("was")) { // raw she ran cough
            val index = sentence.indexOf (word)
            if (index > -1) {
              sentence.split (" ").foreach { token =>
                if (word.equals (token)) {
                  val tuple = ( word, docPos + textPos + index, paraPos, sentPos + sentenceIndex )
                    result.add ( tuple )
                  logger.debug (s"Adding result $tuple")
                }
              }
            }
          }
        }
        sentenceIndex += 1
        textPos += sentence.length ()
      }
    }
    result.toList
  }


  val ordinary=(('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet
  def isOrdinary(s:String) = s.forall (ordinary.contains (_))

  /**
    * Prep vocabulary terms by lower casing, removing non alpha strings and other dross.
    */
  def getDistinctAlpha (terms : RDD[String]) : List[String] = {
    terms.filter { text =>
      val lower = text.replaceAll("-", "")
//      ! (lower forall Character.isDigit) && lower.length > 2
      (! ( lower forall Character.isDigit) ) && 
//      isOrdinary (lower) &&
      lower.length > 2 &&
      ( ( ! lower.equals ("was")) && ( ! lower.equals ("for")) )
    }.map { text =>
      text.toLowerCase ()
    }.distinct().collect ().toList
  }

  /**
    * ChemicalName,ChemicalID,CasRN,DiseaseName,DiseaseID,DirectEvidence,InferenceGeneSymbol,InferenceScore,OmimIDs,PubMedIDs
    * http://ctdbase.org/
    * parse CTD
    * find matching triples
    * create perceptron training set
    */
  def getKnownTriples (triples : Array[(String, String, String, Int)], ctd : RDD[Array[String]])
      : RDD[( String, String, String, Int )] =
  {
    ctd.map { tuple =>
      ( tuple(0).toLowerCase ().replaceAll ("\"", ""),
        tuple(3).toLowerCase ().replaceAll ("\"", "") )
    }.flatMap { tuple =>
      triples.filter {  w => 
        tuple._1.equals (w._1) && tuple._2.equals (w._3)
      }
    }.distinct().cache ()
  }

  def getTSVRecords (rdd : RDD[String]) : RDD[Array[String]] = {
    rdd.filter { line =>
      ! line.startsWith ("#")
    }.map { line =>
      line.split(",").map { elem =>
        elem.trim.replaceAll ("\"", "")
      }
    }
  }

  /*
   * The approach below finds pairs only if they occur within an article.
   * More cohesive, less hits, etc.
   */
  def generatePairs (articlePaths : RDD[String], meshXML : String, sampleSize : Double) = {
    logger.info ("== Analyze articles; calculate binaries.")
    articlePaths.map { a =>
      ( a, meshXML )
    }.sample (false, sampleSize, 1234).map { article =>
      findPairs (article._1, article._2)
    }.cache ()
  }

  def formKey (a : String, b : String) : String = {
    s"$a-$b"
  }

  def splitKey (k : String) : Array[String] = {
    k.split ("-")
  }

  def joinBinaries (
    L : RDD[(String, String, Int)],
    R : RDD[(String, (Double, Double, Double, Int))]) :
      RDD[WordPair] =
  {
    logger.info ("Joining binaries")
    val left = L.map { item =>
      ( formKey (item._1, item._2), (0) )
    }
    R.join (left).map { item =>
      val k = splitKey (item._1)
      ( k(0), k(1), item._2._1._1, item._2._1._2, item._2._1._3, item._2._1._4 )
    }.distinct().cache ()
  }

  def trainLRM (samples : RDD[WordPair]) = {
    logger.info (s"Training LRM with ${samples.count} binaries")
    new LogisticRegressionWithLBFGS().run(
      samples.sample (false, 0.01, 1234).map { item =>
        new LabeledPoint (
          label    = 1,
          features = new DenseVector (Array( item._3, item._4, item._5 ))
        )
      })
  }

/*
  def trainLRM (samples : RDD[(String, String, Double, Double, Double)], name : String) = {
    var model : LogisticRegressionModel = null
    if (Files.exists(Paths.get(name))) {
      model = LogisticRegressionModel.load(sc, name)
    } else {
      logger.info (s"Training LRM with ${samples.count} binaries")
      model = new LogisticRegressionWithLBFGS().run(
        samples.map { item =>
          new LabeledPoint (
            label    = 1,
            features = new DenseVector (Array( item._3, item._4, item._5 ))
          )
        })
      model.save (name)
    }
    model
  }
 */

  def modelProbabilities (
    binaries: RDD[WordPair],
    lrm : LogisticRegressionModel) =
  {
    logger.info ("== Use LogRM to associate probabilities with each binary.")
    val prob = binaries.map { binary =>
      ( binary._1, binary._2,
        lrm.predict (new DenseVector (Array ( binary._3, binary._4, binary._5 ))) )
    }.distinct().cache ()
    prob.collect().foreach { item =>
      logger.info (s"binaries with prob: $item")
    }
    prob
  }

  def extendVocabulary (
    AB      : RDD[(String, String, Int)],
    BC      : RDD[(String, String, Int)],
    AC      : RDD[(String, String, Int)],
    meshXML : String
  ) = {

    logger.info ("Checking vocabulary...")
    val vocab = VocabFactory.getVocabulary (meshXML)
    if (! vocab.extended) {
      /*
      logger.info ("Extending basic (MeSH) vocabulary with terms from CTD...")
      // AB -> _1, _4
      // BC -> _1, _3
      // AC -> _1, _4

      val A = List.concat (vocab.A, getDistinctAlpha (AB.map { e => e._1 })).to[ListBuffer]

      val B = vocab.B :::
      getDistinctAlpha (AB.map { e => e._2 }) :::
      getDistinctAlpha (BC.map { e => e._1 }).filter { lower =>
        (! ( lower forall Character.isDigit) ) &&
        lower.length > 2 &&
        ( ( ! lower.equals ("was")) && ( ! lower.equals ("for")) )
      }

      val C = List.concat (vocab.C, getDistinctAlpha (AC.map { e => e._2 })).to[ListBuffer]

      Array( A, B.to[ListBuffer], C ).map { v =>
        v -= "was"
        v -= "for"
      }

      VocabFactory.writeJSON (new Vocabulary (
        A.toList,
        B.toList,
        C.toList,
        true))
       */
    }
    vocab
    /*
    val a = vocab.A.to[ListBuffer]
    a -= "was"
    a -= "for"

    val b = vocab.B.to[ListBuffer]
    b -= "was"
    b -= "for"

    val c = vocab.C.to[ListBuffer]
    c -= "was"
    c -= "for"

    new Vocabulary (
      a.toList,
      b.toList,
      c.toList
    )
 */
  }

  def getFlatBinaries (binaries : RDD[List[List[WordPair]]]) 
      : RDD[WordPair] =
  {
    logger.info ("== Label known binaries based on CTD binaries...")
    binaries.flatMap { item =>
      item
    }.flatMap { item =>
      item
    }.cache ()
  }

  def genHypothesesWithLRM (
    binaries : RDD[List[List[WordPair]]],
    AB       : RDD[(String, String, Int)],
    BC       : RDD[(String, String, Int)]
  ) = {

    val fb = getFlatBinaries (binaries).map { item =>
      ( formKey (item._1, item._2), ( item._3, item._4, item._5, item._6 ))
    }.cache ()

    /*
     *  Get AB and BC pairs from CTD and associate with feature vectors
     */
    val abBinaries = joinBinaries (AB, fb)
    val bcBinaries = joinBinaries (BC, fb)

    val abLRM = trainLRM (abBinaries.union(bcBinaries))
    val bcLRM = trainLRM (bcBinaries)

    val abProb = modelProbabilities (abBinaries, abLRM)
    val bcProb = modelProbabilities (bcBinaries, bcLRM)

    logger.info ("== Hypotheses:")
    val join = abProb.map { i =>
      (i._2, (i._1, i._3, 1 ))
    }.cogroup (
      bcProb.map { j =>
        (j._1, (j._2, j._3, 2 ))
      }
    ).cache ()

    join.foreach { t =>
      logger.info (s"join > $t")
    }

    join.flatMap { m =>
      m._2._1.flatMap { n =>
        m._2._2.map { p =>
          (n._1, m._1, p._1, n._2 * p._2 )
        }
      }
    }.cache ().foreach { triple =>
      logger.info (s"hypothesis => $triple")
    }

  }

  def calculateTriples (binaries : RDD[List[List[WordPair]]]) = {
    logger.info ("== Calculate triples.")
    val triples = binaries.flatMap { item =>
      Processor.findTriples (item)
    }.map { triple =>
      ( triple, 1 )
    }.reduceByKey (_ + _).map { tuple =>
      ( tuple._1._1, tuple._1._2, tuple._1._3, tuple._2 )
    }.distinct().sortBy (elem => -elem._4).cache ()

    triples.foreach { a =>
      logger.info (s"triple :-- ${a}")
    }
  }

  /*
   * predicted and ctd
   * predicted and not ctd
   * true negatives 
   * 
   * not predicted and ctd (false neg)
   * not predicted and not ctd (true negatives)
   * 
   * precision and accuracy.
   * 
   */
  def evaluateBinaries (
    binaries : RDD[List[List[WordPair]]],
    AB       : RDD[(String, String, Int)],
    BC       : RDD[(String, String, Int)],
    AC       : RDD[(String, String, Int)]
  ) = {

    logger.info ("Evaluate binaries")

    val fb = getFlatBinaries (binaries).map { item =>
      ( formKey (item._1, item._2), ( item._3, item._4, item._5, item._6 ))
    }.cache ()

    
    logger.info (" -- joining binaries")
    val abBinaries = joinBinaries (AB, fb)
    val bcBinaries = joinBinaries (BC, fb)

    logger.info (" -- counting predicted")
    val abPredicted = fb.filter { e => e._2._4 == 1 }
    val bcPredicted = fb.filter { e => e._2._4 == 2 }
    val abPredictedCount = abPredicted.count ()
    val bcPredictedCount = bcPredicted.count ()

    logger.info (" -- counting ab binaries")
    val abPredictedInCTDCount = abBinaries.count ()
    logger.info (" -- counting bc binaries")
    val bcPredictedInCTDCount = bcBinaries.count ()

    bcPredicted.collect().foreach { x =>
      logger.info (s"bc binary predicted: $x")
    }
    abPredicted.collect().foreach { x =>
      logger.info (s"ab binary predicted: $x")
    }

    bcBinaries.collect().foreach { x =>
      logger.info (s"bc binary in ctd: $x")
    }
    abBinaries.collect().foreach { x =>
      logger.info (s"ab binary in ctd: $x")
    }

    println (s" ab predicted count:       $abPredictedCount")
    println (s" bc predicted count:       $bcPredictedCount")
    println (s" ab predicted and in ctd:  ${abBinaries.count ()}")
    println (s" bc predicted and in ctd:  ${bcBinaries.count ()}")

    val predictedAndNotInCTD = abBinaries
    println (s"AB: predicted and in CTD: ")
  }

  def executeChemotextPipeline (
    articlePaths : RDD[String],
    meshXML      : String,
    AB           : RDD[(String, String, Int)],
    BC           : RDD[(String, String, Int)],
    AC           : RDD[(String, String, Int)],
    sampleSize   : Double) =
  {

    val vocab = extendVocabulary (AB, BC, AC, meshXML)

    val binaries = generatePairs (articlePaths, meshXML, sampleSize)

    evaluateBinaries (binaries, AB, BC, AC)

    calculateTriples (binaries)

    //genHypothesesWithLRM (binaries, AB, BC)
  }

}

/***
 * An API for chemotext. Abstracts chemotext automation.
 */
class PipelineContext (
  sparkContext    : SparkContext,
  appHome         : String = "../data/pubmed",
  meshXML         : String = "../data/pubmed/mesh/desc2016.xml",
  articleRootPath : String = "../data/pubmed/articles",
  ctdACPath       : String = "../data/pubmed/ctd/CTD_chemicals_diseases.csv",
  ctdABPath       : String = "../data/pubmed/ctd/CTD_chem_gene_ixns.csv",
  ctdBCPath       : String = "../data/pubmed/ctd/CTD_genes_diseases.csv",
  sampleSize      : String = "0.01")
{
  val logger = LoggerFactory.getLogger ("PipelineContext")

  def recursiveListFiles(f : File, r : Regex): Array[File] = {
    val these = f.listFiles
    val good = these.filter(f => r.findFirstIn(f.getName).isDefined)
    good ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_, r))
  }

  def getFileList (articleRootDir : File, articleRegex : Regex) : Array[String] = {
    var fileList : Array[String] = null
    val fileListPath = "filelist.json"

    val json = JSONUtils.readJSON (fileListPath)
    if (json != null) {
      implicit val formats = DefaultFormats
      json.extract[Array[String]]
    } else {
      fileList = recursiveListFiles (articleRootDir, articleRegex).map (_.getCanonicalPath)
      JSONUtils.writeJSON (fileList, fileListPath)
      fileList
    }
  }

  def execute () = {
    val articleRootDir = new File (articleRootPath)
    val articleRegex = new Regex (".*.fxml")
    val articleList = getFileList (articleRootDir, articleRegex)

    val corpusPath = "pmc_corpus.txt"
    val vectorModelPath = "pmc_w2v.model"

    /*
    var model : Word2VecModel = null
    if (Files.exists(Paths.get(vectorModelPath))) {
      model = Word2VecModel.load(sparkContext, vectorModelPath)
    } else {
      if (! Files.exists (Paths.get (corpusPath))) {
        Processor.createCorpus (articleList, corpusPath)
      }
      val words = sparkContext.textFile (corpusPath).map { e => e.split (" ").toList }
      model = Processor.vectorizeCorpus (words)
      model.save(sparkContext, vectorModelPath)
    }
     */


    val AB = Processor.getTSVRecords (sparkContext.textFile (ctdABPath)).map { row =>
      ( row(0), row(3), 1 )
    }.sample (false, 0.01, 1234)

    val BC = Processor.getTSVRecords (sparkContext.textFile (ctdBCPath)).map { row =>
      ( row(0), row(2), 2 )
    }.sample (false, 0.01, 1234)

    val AC = Processor.getTSVRecords (sparkContext.textFile (ctdACPath)).map { row =>
      ( row(0), row(3), 3 )
    }.sample (false, 0.01, 1234)

    Processor.executeChemotextPipeline (
      articlePaths = sparkContext.parallelize (articleList),
      meshXML      = meshXML,
      AB           = AB,
      BC           = BC,
      AC           = AC,
      sampleSize   = sampleSize.toDouble)
  }

}

object PipelineApp {

  val logger = LoggerFactory.getLogger ("PipelineApp")

  def main(args: Array[String]) {

    val appName = args(0)
    val appHome = args(1)
    val articleRootPath = args(2)
    val meshXML = args(3)
    val sampleSize = args(4)
    val ctdACPath = args(5)
    val ctdABPath = args(6)
    val ctdBCPath = args(7)

    logger.info (s"appName        : $appName")
    logger.info (s"appHome        : $appHome")
    logger.info (s"articleRootPath: $articleRootPath")
    logger.info (s"meshXML        : $meshXML")
    logger.info (s"sampleSize     : $sampleSize")
    logger.info (s"ctdACPath      : $ctdACPath")
    logger.info (s"ctdABPath      : $ctdABPath")
    logger.info (s"ctdBCPath      : $ctdBCPath")

    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val pipeline = new PipelineContext (
      sc,
      appHome,
      meshXML,
      articleRootPath,
      ctdACPath,
      ctdABPath,
      ctdBCPath,
      sampleSize)
    pipeline.execute ()    
  }
}

