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
import java.text.BreakIterator
import java.util.Locale
import java.util.Date

/**

  ftp://ftp.ncbi.nlm.nih.gov/pub/lu/PubTator/

  // pubchem
  // tmChem - use their sourcecode
  // pubtator
  // dnorm
  // chebi
  // ftp://ftp.ncbi.nih.gov/pubchem/Substance/Daily/2016-03-17/XML/

  */

/***
 * Processor for searching articles for terms.
 * Implemented as an object to interoperate with
 * Spark serialization and worker semantics.
 */
object Processor {

  val logger = LoggerFactory.getLogger("Processor")

  case class Triple (
    A : String,
    B : String,
    C : String
  )
  case class Binary (
    L        : String,
    R        : String,
    docDist  : Double,
    paraDist : Double,
    sentDist : Double,
    code     : Int
  )
  case class WordFeature (
    word    : String,
    docPos  : Int,
    paraPos : Int,
    sentPos : Int
  )
  case class Paragraph (
    sentences : List[String]
  )
  case class QuantifiedArticle (
    fileName   : String,
    date       : String,
    generator  : String,
    raw        : String,
    paragraphs : List[Paragraph],
    A          : List[WordFeature],
    B          : List[WordFeature],
    C          : List[WordFeature],
    AB         : List[Binary] = null,
    BC         : List[Binary] = null,
    AC         : List[Binary] = null,
    ABC        : List[Triple] = null
  )

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

  def findTriples (article : QuantifiedArticle) : QuantifiedArticle = {
    logger.debug (s" Finding triples in AB/BC pair lists ${article.AB.length}/${article.BC.length} long")
    val ABC = article.AB.flatMap { ab =>
      article.BC.map { bc =>
        if (ab.R.equals (bc.L)) {
          logger.debug (s" ab/bc => ${ab.L} ${ab.R} ${bc.L} ${bc.R}")
          Triple ( ab.L, ab.R, bc.R )
        } else {
          Triple ( null, null, null )
        }
      }
    }.filter { item =>
      item.A != null
    }
    val quantified = QuantifiedArticle (
      fileName   = article.fileName,
      date       = article.date,
      generator  = article.generator,
      raw        = article.raw,
      paragraphs = article.paragraphs,
      A          = article.A,
      B          = article.B,
      C          = article.C,
      AB         = article.AB,
      BC         = article.BC,
      AC         = article.AC,
      ABC        = ABC)

    JSONUtils.writeJSON (quantified, "x/" + quantified.fileName + ".json")
    quantified
  }

  /**
    * Derive A->B, B->C, A->C relationships from raw word positions
    */
  def findPairs (article : String, meshXML : String) : QuantifiedArticle = {
    findPairs (quantifyArticle (article, meshXML))
  }

  def findPairs (article : QuantifiedArticle) : QuantifiedArticle = {
    val threshold = 100
    QuantifiedArticle (
      fileName   = article.fileName,
      date       = article.date,
      generator  = article.generator,
      raw        = article.raw,
      paragraphs = article.paragraphs,
      A          = article.A,
      B          = article.B,
      C          = article.C,
      AB         = findCooccurring (article.A, article.B, threshold, 1), // AB,
      BC         = findCooccurring (article.B, article.C, threshold, 2), // BC,
      AC         = findCooccurring (article.A, article.C, threshold, 3))  // AC
  }

  /**
    * Determine pairs based on distance and include an confidence score 
    */
  def findCooccurring (
    L         : List[WordFeature],
    R         : List[WordFeature],
    threshold : Int,
    code      : Int)
      : List[Binary] = 
  {
    L.flatMap { left =>
      R.map { right =>
        logger.debug (s"cooccurring: $left._1 and $right._1 $code")
        val docPosDifference = math.abs (left.docPos - right.docPos).toDouble
        if ( docPosDifference < threshold && docPosDifference > 0 ) {
          new Binary ( left.word,
            right.word,
            docPosDifference,
            math.abs (left.paraPos - right.paraPos).toDouble,
            math.abs (left.sentPos - right.sentPos).toDouble,
            code
          )
        } else {
          new Binary ( null, null, 0.0, 0.0, 0.0, 0 )
        }
      }
    }.filter { element =>
      element.docDist != 0
    }
  }

  /**
    * Quantify an article, searching for words and producing an output 
    * vector showing locations of A, B, and C terms in the text.
    */
  def quantifyArticle (article : String, meshXML : String) : QuantifiedArticle = {
    var A : List[WordFeature] = List ()
    var B : List[WordFeature] = List ()
    var C : List[WordFeature] = List ()
    val vocab = VocabFactory.getVocabulary (meshXML)
    logger.debug (s"""Sample vocab:
         A-> ${vocab.A.slice (1, 20)}
         B-> ${vocab.B.slice (1, 20)}
         C-> ${vocab.C.slice (1, 20)}""")
    logger.info (s"@-article: ${article}")
    var docPos = 0
    var paraPos = 0
    var sentPos = 0

    val paraBuf = new ListBuffer [Paragraph] ()
    val rawBuf = StringBuilder.newBuilder
    try {
      val xml = XML.loadFile (article)
      val paragraphs = (xml \\ "p")

      paragraphs.foreach { paragraph =>
        val text = getSentences (paragraph.text)
        A = A.union (getDocWords (vocab.A.toArray, text.toArray, docPos, paraPos, sentPos))
        B = B.union (getDocWords (vocab.B.toArray, text.toArray, docPos, paraPos, sentPos))
        C = C.union (getDocWords (vocab.C.toArray, text.toArray, docPos, paraPos, sentPos))

        sentPos += text.size
        docPos += paragraph.text.length
        paraPos += 1

        rawBuf.append (paragraph.text)
        rawBuf.append ("\n")
        paraBuf += new Paragraph (text)
      }

    } catch {
      case e: Exception =>
        logger.error (s"Error reading json $e")
    }
    QuantifiedArticle (
      fileName   = article.replaceAll (".*/", ""),
      date       = new Date().toString (),
      generator  = "ChemoText2",
      raw        = rawBuf.toString,
      paragraphs = paraBuf.toList,
      A          = A,
      B          = B,
      C          = C)
  }

  def getSentences (text : String) = {
    val buf = new ListBuffer[String] ()
    val boundary = BreakIterator.getSentenceInstance(Locale.ENGLISH)
    boundary.setText (text)
    var lastIndex = boundary.first()
    while (lastIndex != BreakIterator.DONE) {
      var firstIndex = lastIndex
      lastIndex = boundary.next ()
      if (lastIndex != BreakIterator.DONE) {
        val sentence = text.substring (firstIndex, lastIndex);
        buf += sentence.toLowerCase ()
      }
    }
    buf.toList
  }

  /**
    * Record locations of words within the document.
    */
  def getDocWords (
    words   : Array[String],
    text    : Array[String],
    docPos  : Int,
    paraPos : Int,
    sentPos : Int) : List[WordFeature] =
  {
    logger.debug ("text-> " + text.mkString (" "))
    var textPos = 0
    var sentenceIndex = 0
    var result : ListBuffer[WordFeature] = ListBuffer ()
    if (words != null && text != null) {
      text.foreach { sentence =>
        words.foreach { word =>
          val index = sentence.indexOf (word)
          if (index > -1) {
            val features = new WordFeature (word, docPos + textPos + index, paraPos, sentPos + sentenceIndex )
            result.add ( features )

            logger.debug (
              s"**adding word:$word dpos:$docPos tpos:$textPos" +
              s"idx:$index ppos:$paraPos spos:$sentPos sidx:$sentenceIndex")
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
      (! ( lower forall Character.isDigit) ) && 
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

  def formKey (a : String, b : String) : String = {
    s"$a-$b"
  }

  def splitKey (k : String) : Array[String] = {
    k.split ("-")
  }

  def joinBinaries (
    L : RDD[(String, String, Int)],
    R : RDD[(String, (Double, Double, Double, Int))]) :
      RDD[Binary] =
  {
    logger.info ("Joining binaries")
    val left = L.map { item =>
      ( formKey (item._1, item._2), (0) )
    }
    R.join (left).map { item =>
      val k = splitKey (item._1)
      Binary ( k(0), k(1), item._2._1._1, item._2._1._2, item._2._1._3, item._2._1._4 )
    }.distinct().cache ()
  }

  def trainLRM (samples : RDD[Binary]) = {
    logger.info (s"Training LRM with ${samples.count} binaries")
    new LogisticRegressionWithLBFGS().run(
      samples.sample (false, 0.01, 1234).map { item =>
        new LabeledPoint (
          label    = 1,
          features = new DenseVector (Array( item.docDist, item.paraDist, item.sentDist ))
        )
      })
  }

  def modelProbabilities (
    binaries: RDD[Binary],
    lrm : LogisticRegressionModel) =
  {
    logger.info ("== Use LogRM to associate probabilities with each binary.")
    val prob = binaries.map { binary =>
      ( binary.L, binary.R,
        lrm.predict (new DenseVector (Array ( binary.docDist, binary.paraDist, binary.sentDist ))) )
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

  def getFlatBinaries (binaries : RDD[QuantifiedArticle]) : RDD[Binary] = {
    binaries.flatMap { item =>
      item.AB.union (item.BC).union (item.AC)
    }.cache ()
  }

  def genHypothesesWithLRM (
    binaries : RDD[QuantifiedArticle],
    AB       : RDD[(String, String, Int)],
    BC       : RDD[(String, String, Int)]
  ) = {

    val fb = getFlatBinaries (binaries).map { item =>
      ( formKey (item.L, item.R), ( item.docDist, item.paraDist, item.sentDist, item.code ))
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

  def calculateTriples (articles : RDD[QuantifiedArticle]) = {
    logger.info ("== Calculate triples.")
    val triples = articles.map { article =>
      Processor.findTriples (article)
    }.flatMap { article =>
      article.ABC
    }.map { triple =>
      ( triple, 1 )
    }.reduceByKey (_ + _).map { tuple =>
      ( tuple._1.A, tuple._1.B, tuple._1.C, tuple._2 )
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
    articles : RDD[QuantifiedArticle],
    AB       : RDD[(String, String, Int)],
    BC       : RDD[(String, String, Int)],
    AC       : RDD[(String, String, Int)]
  ) = {

    logger.info ("Evaluate binaries")

    val fb = getFlatBinaries (articles).map { item =>
      ( formKey (item.L, item.R), ( item.docDist, item.paraDist, item.sentDist, item.code ))
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

  def executeChemotextPipeline (
    articlePaths : RDD[String],
    meshXML      : String,
    AB           : RDD[(String, String, Int)],
    BC           : RDD[(String, String, Int)],
    AC           : RDD[(String, String, Int)],
    sampleSize   : Double) =
  {

    val vocab = extendVocabulary (AB, BC, AC, meshXML)

    logger.debug (s"article path count: ${articlePaths.count}")
    articlePaths.collect.foreach { a =>
      logger.debug (s"------------> $a")
    }

    val articles = generatePairs (articlePaths, meshXML, sampleSize)

    evaluateBinaries (articles, AB, BC, AC)

    calculateTriples (articles)

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

    logger.debug (s"Article List: $articleList")

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

