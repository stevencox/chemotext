package org.chemotext

import banner.types.Sentence
import java.io.File
import java.io.PrintWriter
import java.io.BufferedWriter
import java.io.IOException
import java.io.FileReader
import java.io.FileWriter
import java.io.InputStream
import java.io.FileInputStream
import java.io.BufferedReader
import java.nio.file.{Paths, Files}
import java.text.BreakIterator
import java.util.Collections
import java.util.Locale
import java.util.Date
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
import org.apache.spark.sql.SQLContext
import org.json4s._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.xml.sax.SAXParseException
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.compat.Platform
import scala.util.matching.Regex
import scala.xml.XML
import scala.util.control.Breaks._

case class Position (
  var document  : Int = 0,
  var text      : Int = 0,
  var paragraph : Int = 0,
  var sentence  : Int = 0
)

/***
 * Processor for searching articles for terms.
 * Implemented as an object to interoperate with
 * Spark serialization and worker semantics.
 */
object Processor {

  val logger = LoggerFactory.getLogger("Chemotext2")

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
  case class Paragraph (
    sentences : List[String]
  )
  case class QuantifiedArticle (
    fileName   : String,
    date       : String,
    id         : String,
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

  case class QuantifierConfig (
    article   : String,
    meshXML   : String,
    lexerConf : TmChemLexerConf
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

  def findTriples (article : QuantifiedArticle, outputPath : String) : QuantifiedArticle = {
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
      id         = article.id,
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

    val outputDir = new File (outputPath)
    if (! outputDir.isDirectory ()) {
      logger.debug (s"Creating directory $outputPath")
      outputDir.mkdirs ()
    }
    JSONUtils.writeJSON (quantified, outputPath + File.separator + quantified.fileName + ".json")
    quantified
  }

  /**
    * Derive A->B, B->C, A->C relationships from raw word positions
    */
  def findPairs (config : QuantifierConfig) : QuantifiedArticle = {
    findPairs (quantifyArticle (config))
  }

  def findPairs (article : QuantifiedArticle) : QuantifiedArticle = {
    val threshold = 100
    QuantifiedArticle (
      fileName   = article.fileName,
      date       = article.date,
      id         = article.id,
      generator  = article.generator,
      raw        = article.raw,
      paragraphs = article.paragraphs,
      A          = article.A,
      B          = article.B,
      C          = article.C,
      AB         = findCooccurring (article.A, article.B, threshold, 1), // AB,
      BC         = findCooccurring (article.B, article.C, threshold, 2), // BC,
      AC         = findCooccurring (article.A, article.C, threshold, 3)) // AC
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
        val distance = math.abs (left.docPos - right.docPos).toDouble
        if ( distance < threshold && distance > 0 ) {
          logger.debug (s"cooccurring: $left._1 and $right._1 code:$code distance:$distance")
          new Binary ( left.word,
            right.word,
            distance,
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

  def quantifyArticle (config : QuantifierConfig) : QuantifiedArticle = {
    var A : List[WordFeature] = List ()
    var B : List[WordFeature] = List ()
    var C : List[WordFeature] = List ()
    val vocab = VocabFactory.getVocabulary (config.meshXML)
    logger.debug (s"""Sample vocab:
         A-> ${vocab.A.slice (1, 10)}...
         B-> ${vocab.B.slice (1, 10)}...
         C-> ${vocab.C.slice (1, 10)}...""")
    logger.info (s"@-article: ${config.article}")
    var position = Position ()
    val paraBuf = new ListBuffer [Paragraph] ()
    val rawBuf = StringBuilder.newBuilder
    var date : String = null
    var id   : String = null
    try {
      val A_lexer = new AMeshLexer (config.meshXML)
      //val A_lexer = new TmChemLexer (config.lexerConf)
      val B_lexer = new BMeshLexer (config.meshXML)
      val C_lexer = new CMeshLexer (config.meshXML)
      val parser = new PubMedArticleParser (config.article)
      id = parser.getId ()
      date = parser.getDate ()
      val paragraphs = parser.getParagraphs ()
      paragraphs.foreach { paragraph =>
        // replace non ascii characters
        val raw = paragraph.text.replaceAll ("[^\\x00-\\x7F]", "");
        val text = getSentences (raw)

        A = A.union (getDocWords (vocab.A.toArray, text.toArray, position, "A", A_lexer))
        B = B.union (getDocWords (vocab.B.toArray, text.toArray, position, "B", B_lexer))
        C = C.union (getDocWords (vocab.C.toArray, text.toArray, position, "C", C_lexer))

        List( A_lexer, B_lexer, C_lexer ).map { lexer =>
          lexer.update (paragraph.text.length, text.size (), 1)
        }

        rawBuf.append (raw)
        rawBuf.append ("\n")
        paraBuf += new Paragraph (text)
      }
    } catch {
      case e: Exception =>
        logger.error (s"Error in quantify article  $e")
        e.printStackTrace ()
    }
    QuantifiedArticle (
      fileName   = config.article.replaceAll (".*/", ""),
      date       = date,
      id         = id,
      generator  = "ChemoText2[Scala]",
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
    words    : Array[String],
    text     : Array[String],
    position : Position,
    listId   : String,
    lexer    : Lexer) : List[WordFeature] =
  {
    var result : ListBuffer[WordFeature] = ListBuffer ()
    text.foreach { sentence =>
      lexer.findTokens (sentence, result)
    }
    result.toList
  }

  val ordinary=(('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet
  def isOrdinary(s:String) = s.forall (ordinary.contains (_))

  /**
    * Prep vocabulary terms by lower casing, removing non alpha strings and other dross.
    */
  def cleanWord (terms : RDD[String]) : List[String] = {
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

  def getCSV (sc : SparkContext, fileName : String, sampleSize : Double = 0.1) : RDD[Array[String]] = {
    val sqlContext = new SQLContext(sc)
    sqlContext.read
      .format("com.databricks.spark.csv")
      .load(fileName).rdd.sample (false, sampleSize, 1234)
      .map { row =>
        Array( row.getString(0), row.getString(1), row.getString(2), row.getString(3) )
    }
  }

  def getCSVFields (sc : SparkContext, fileName : String, sampleSize : Double = 0.1, a : Int, b : Int, code : Int)
      : RDD[(String,String,Int)] = 
  {
    getCSV (sc, fileName, sampleSize).map { row => ( row(a), row(b), code ) }
  }

  def formKey (a : String, b : String) : String = {
    // Must use a character that's not part of IUPAC codes.
    s"${a.toLowerCase()}@${b.toLowerCase()}"
  }

  def splitKey (k : String) : Array[String] = {
    k.split ("@")
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
    left.foreach { l =>
      //println (s"  ------88> $l")
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
    var vocab = VocabFactory.getVocabulary (meshXML)
    if (! vocab.extended) {

      logger.info ("Extending basic (MeSH) vocabulary with terms from CTD...")
      vocab = new Vocabulary (

        A = vocab.A.union (cleanWord (AB.map { e => e._1 })).
          distinct.filter (!_.equals ("apoptosis")),

        B = vocab.B.
          union (cleanWord (AB.map { e => e._2 })).
          union (cleanWord (BC.map { e => e._1 })).
          distinct,

        C = vocab.C.union (cleanWord (AC.map { e => e._2 })).
          distinct,

        extended = true)

      // Cache the extended vocabulary.
      VocabFactory.writeJSON (vocab)
    }
    vocab
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
    logger.debug ("Join AB binaries")
    val abBinaries = joinBinaries (AB, fb)
    logger.debug ("Join BC binaries")
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

  def calculateTriples (articles : RDD[QuantifiedArticle], outputPath : String) = {
    logger.info ("== Calculate triples.")
    val triples = articles.map { article =>
      Processor.findTriples (article, outputPath)
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
    logger.info ("Joining AB binaries to generated binary predictions")
    val abBinaries = joinBinaries (AB, fb)
    AB.foreach { b =>
      //logger.debug (s" ------> $b")
    }

    logger.debug ("Joining BC binaries to generated binary predictions")
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
  }

  /*
   * The approach below finds pairs only if they occur within an article.
   * More cohesive, less hits, etc.
   */
  def generatePairs (
    articlePaths : RDD[String],
    meshXML      : String,
    sampleSize   : Double,
    lexerConf    : TmChemLexerConf) = 
  {
    logger.info ("== Analyze articles; calculate binaries.")
    articlePaths.map { a =>
      ( a, meshXML )
    }.sample (false, sampleSize, 1234).map { article =>
      findPairs (QuantifierConfig (
        article   = article._1,
        meshXML   = article._2,
        lexerConf = lexerConf))
      //findPairs (article._1, article._2)
    }.cache ()
  }

  def executeChemotextPipeline (
    articlePaths : RDD[String],
    meshXML      : String,
    lexerConf    : TmChemLexerConf,
    AB           : RDD[(String, String, Int)],
    BC           : RDD[(String, String, Int)],
    AC           : RDD[(String, String, Int)],
    sampleSize   : Double,
    outputPath   : String) =
  {
    val vocab = extendVocabulary (AB, BC, AC, meshXML)

    logger.debug (s"article path count: ${articlePaths.count}")
    articlePaths.collect.foreach { a =>
      logger.debug (s"--> $a")
    }

    val articles = generatePairs (articlePaths, meshXML, sampleSize, lexerConf)

    evaluateBinaries (articles, AB, BC, AC)

    calculateTriples (articles, outputPath)

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
  lexerConf       : TmChemLexerConf,
  articleRootPath : String = "../data/pubmed/articles",
  ctdACPath       : String = "../data/pubmed/ctd/CTD_chemicals_diseases.csv",
  ctdABPath       : String = "../data/pubmed/ctd/CTD_chem_gene_ixns.csv",
  ctdBCPath       : String = "../data/pubmed/ctd/CTD_genes_diseases.csv",
  sampleSize      : Double = 0.01,  
  outputPath      : String = "output"
)
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

    val ctdSampleSize = 1.0
    val AB = Processor.getCSVFields (sparkContext, ctdABPath, ctdSampleSize, 0, 3, 1)
    val BC = Processor.getCSVFields (sparkContext, ctdBCPath, ctdSampleSize, 0, 2, 2)
    val AC = Processor.getCSVFields (sparkContext, ctdACPath, ctdSampleSize, 0, 3, 3)

    Processor.executeChemotextPipeline (
      articlePaths   = sparkContext.parallelize (articleList),
      meshXML        = meshXML,
      lexerConf      = lexerConf,
      AB             = AB,
      BC             = BC,
      AC             = AC,
      sampleSize     = sampleSize,
      outputPath     = outputPath)
  }
}

object PipelineApp {

  val logger = LoggerFactory.getLogger ("PipelineApp")

  def main(args: Array[String]) {

    val appName = args (0)
    val appHome = args (1)
    val articleRootPath = args (2)
    val meshXML = args (3)
    val sampleSize = args (4)
    val ctdACPath = args (5)
    val ctdABPath = args (6)
    val ctdBCPath = args (7)
    val outputPath = args (8)

    val lexerConfigPath    = args (9)
    val lexerCacheFile     = args (10)
    val lexerDictPath      = args (11)

    logger.info (s"appName        : $appName")
    logger.info (s"appHome        : $appHome")
    logger.info (s"articleRootPath: $articleRootPath")
    logger.info (s"meshXML        : $meshXML")
    logger.info (s"sampleSize     : $sampleSize")
    logger.info (s"ctdACPath      : $ctdACPath")
    logger.info (s"ctdABPath      : $ctdABPath")
    logger.info (s"ctdBCPath      : $ctdBCPath")
    logger.info (s"outputPath     : $outputPath")

    logger.info (s"lexerConfigPath    : $lexerConfigPath")
    logger.info (s"lexerCacheFile     : $lexerCacheFile")
    logger.info (s"lexerDictPath      : $lexerDictPath")

    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val pipeline = new PipelineContext (
      sc,
      appHome,
      meshXML,
      TmChemLexerConf (
        configPath     = lexerConfigPath,
        cacheFileName  = lexerCacheFile,
        dictionaryPath = lexerDictPath
      ),
      articleRootPath,
      ctdACPath,
      ctdABPath,
      ctdBCPath,
      sampleSize.toDouble,
      outputPath)
    pipeline.execute ()    
  }
}

