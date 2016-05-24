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
import java.io.PrintStream
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

import opennlp.tools.sentdetect.SentenceModel
import opennlp.tools.sentdetect.SentenceDetectorME

/**
  *  Add OpenNLP as a strategy for sentence segmentation.
  */
class SentenceSegmenter {

  val stream : InputStream = getClass.getResourceAsStream ("/models/en-sent.bin")
  val model = new SentenceModel (stream);
  val sentenceDetector = new SentenceDetectorME(model);

  def segment (text : String) = {
    sentenceDetector.sentDetect (text)
  }

}

/**
  * Defeat the fact that the version of banner used by tmChem uses System.out for logging.
  * There's no way to configure it, so we'll intercept System.out and turn it off.
  * Without this, processes fill up the disks on cluster worker nodes, killing the job.
  */
class BannerFilterPrintStream (stream: PrintStream) extends PrintStream (stream) {  
  override def println (s : String) {
    if (! s.startsWith ("WARNING:") && s.contains ("lists no concept")) {
      super.print (s)
    }
  }
}

object UniqueID {
  private var id : Long = 0
  def inc () = { id += 1; id }
}

/**
  *  Track a place in a document
  */
case class Position (
  var document  : Int = 0,
  var text      : Int = 0,
  var paragraph : Int = 0,
  var sentence  : Int = 0
)

/**
  * Represenation of known propositions from authoratative databases.
  */
case class Fact (
  L     : String,
  R     : String,
  code  : Int,
  PMIDs : Array[String]
)

/***
 * Processor for searching articles for terms.
 * Implemented as an object to interoperate with
 * Spark serialization and worker semantics.
 */
object Processor {

  val logger = LoggerFactory.getLogger("Chemotext2")

  val jython = new JythonInterpreter ()

  case class Triple (
    A : String,
    B : String,
    C : String
  )
  case class Binary (
    id        : Long,
    L         : String,
    R         : String,
    docDist   : Double,
    paraDist  : Double,
    sentDist  : Double,
    code      : Int,
    var fact  : Boolean = false,
    var refs  : Array[String] = Array[String]()
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
    val quantified = article.copy (ABC = ABC)

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
    article.copy (
      AB = findCooccurring (article.A, article.B, threshold, 1),
      BC = findCooccurring (article.B, article.C, threshold, 2),
      AC = findCooccurring (article.A, article.C, threshold, 3))
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
          Binary (
            id       = UniqueID.inc (),
            L        = left.word,
            R        = right.word,
            docDist  = distance,
            paraDist = math.abs (left.paraPos - right.paraPos).toDouble,
            sentDist = math.abs (left.sentPos - right.sentPos).toDouble,
            code     = code)
        } else {
          Binary ( UniqueID.inc (), null, null, 0.0, 0.0, 0.0, 0 )
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
      //val A_lexer = new AMeshLexer (config.meshXML)
      val A_lexer = new TmChemLexer (config.lexerConf)
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

  def getSentences0 (text : String) = {
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

  def getSentences (text : String) = {
    val buf = new ListBuffer[String] ()
    val sentenceSegmenter = new SentenceSegmenter ()
    val sentences = sentenceSegmenter.segment (text)
    sentences.foreach { sentence =>
      buf += sentence.toLowerCase ()
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

  def getCSV (sc : SparkContext, fileName : String, sampleSize : Double = 0.1) : RDD[Array[String]] = {
    val sqlContext = new SQLContext(sc)
    sqlContext.read
      .format("com.databricks.spark.csv")
      .load(fileName).rdd.sample (false, sampleSize, 1234)
      .map { row =>
      var buf = new ArrayBuffer[String] ()
      for (c <- 0 to row.length - 1) {
        buf += row.getString (c)
      }
      buf.toArray
    }
  }

  def getCSVFields (sc : SparkContext, fileName : String, sampleSize : Double = 0.1, a : Int, b : Int, code : Int)
      : RDD[(String,String,Int)] = 
  {
    getCSV (sc, fileName, sampleSize).map { row => ( row(a), row(b), code ) }
  }

  def getFacts (sc : SparkContext, fileName : String, sampleSize : Double = 0.1, a : Int, b : Int, code : Int, pmids : Int = 0)
      : RDD[Fact] =
  {
    getCSV (sc, fileName, sampleSize).map { row =>
      Fact (
        L     = row (a),
        R     = row (b),
        code  = code,
        PMIDs = row (pmids).split ("\\|")
      )
    }
  }

  def formKey (a : String, b : String) : String = {
    // Must use a character that's not part of IUPAC codes.
    s"${a.toLowerCase()}@${b.toLowerCase()}"
  }

  def splitKey (k : String) : Array[String] = {
    k.split ("@")
  }

  def joinBinaries (
    L : RDD[Fact],
    R : RDD[(String, (Long, Double, Double, Double, Int))]) :
      RDD[Binary] =
  {
    logger.info ("Joining binaries")
    val left = L.map { fact =>
      ( formKey (fact.L, fact.R), fact.PMIDs )
    }
    R.join (left).map { item =>
      val k = splitKey (item._1)
      Binary (
        id       = item._2._1._1,
        L        = k(0),
        R        = k(1),
        docDist  = item._2._1._2,
        paraDist = item._2._1._3,
        sentDist = item._2._1._4,
        code     = item._2._1._5,
        refs     = item._2._2)
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
    AB      : RDD[Fact],
    BC      : RDD[Fact],
    AC      : RDD[Fact],
    meshXML : String
  ) = {
    logger.info ("Checking vocabulary...")
    var vocab = VocabFactory.getVocabulary (meshXML)
    if (! vocab.extended) {

      logger.info ("Extending basic (MeSH) vocabulary with terms from CTD...")

      vocab = new Vocabulary (

        A = vocab.A.union (cleanWord (AB.map { e => e.L })).
          distinct.filter (!_.equals ("apoptosis")),

        B = vocab.B.
          union (cleanWord (AB.map { e => e.R })).
          union (cleanWord (BC.map { e => e.L })).
          distinct,

        C = vocab.C.
          union (cleanWord (AC.map { e => e.R })).
          union (cleanWord (BC.map { e => e.R })).
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
    AB       : RDD[Fact],
    BC       : RDD[Fact]
  ) = {

    val fb = getFlatBinaries (binaries).map { item =>
      ( formKey (item.L, item.R), ( item.id, item.docDist, item.paraDist, item.sentDist, item.code ))
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

  case class PMIDToDate (
    map : Map[String, String]
  )

  def calculateTriples (articlesIn : RDD[QuantifiedArticle], outputPath : String) = {
    logger.info ("== Calculate triples.")
    val articles = articlesIn.map { article =>
      Processor.findTriples (article, outputPath)
    }

    val map = new HashMap[String, String] ()
    articles.map { article =>
      (article.id, article.date)
    }.collect.foreach { i =>
      map.put (i._1, i._2)
    }

    val pmidToDate = PMIDToDate (scala.collection.immutable.HashMap () ++ map)

    JSONUtils.writeJSON (pmidToDate, outputPath + File.separator + "pmid_date.json")

    /** test we can read the json we just wrote.
    val json = JSONUtils.readJSON (outputPath + File.separator + "pmid_date.json")
    implicit val formats = DefaultFormats
    val pmidToDate2 = json.extract[PMIDToDate]
    println (s" ${pmidToDate2.map}")
      */

    val triples = articles.flatMap { article =>
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
  def annotateBinaries (
    articles : RDD[QuantifiedArticle],
    AB       : RDD[Fact],
    BC       : RDD[Fact],
    AC       : RDD[Fact]) : RDD[QuantifiedArticle] =
  {
    logger.info ("Evaluate binaries")
    val fb = getFlatBinaries (articles).map { item =>
      ( formKey (item.L, item.R), ( item.id, item.docDist, item.paraDist, item.sentDist, item.code ))
    }.cache ()

    logger.info (" -- joining binaries")
    logger.info ("Joining AB binaries to generated binary predictions")
    val abBinaries = joinBinaries (AB, fb)
    logger.debug ("Joining BC binaries to generated binary predictions")
    val bcBinaries = joinBinaries (BC, fb)

    logger.info (" -- counting predicted")
    val abPredicted = fb.filter { e => e._2._5 == 1 }
    val bcPredicted = fb.filter { e => e._2._5 == 2 }

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

    val bcBinaryFacts = bcBinaries.collect ()
    bcBinaryFacts.foreach { x =>
      logger.info (s"bc binary in ctd: $x")
    }
    val abBinaryFacts = abBinaries.collect ()
    abBinaryFacts.foreach { x =>
      logger.info (s"ab binary in ctd: $x")
    }
    println (s" ab predicted count:       ${abPredicted.count}")
    println (s" bc predicted count:       ${bcPredicted.count}")
    println (s" ab predicted and in ctd:  ${abBinaries.count}")
    println (s" bc predicted and in ctd:  ${bcBinaries.count}")

    // Annotate binaries regarding their CTD status.
    articles.map { article =>
      article.copy (AB = tagFacts (article.AB, abBinaryFacts))
    }.map { article =>
      article.copy (BC = tagFacts (article.BC, bcBinaryFacts))
    }

  }

  def tagFacts (assertions : List[Binary], facts : Array[Binary]) : List[Binary] = {
    assertions.foreach { assertion =>
      facts.foreach { fact =>
        if (assertion.id == fact.id) {
          assertion.fact = true
          assertion.refs = fact.refs
        }
      }
    }
    assertions
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
    AB           : RDD[Fact],
    BC           : RDD[Fact],
    AC           : RDD[Fact],
    sampleSize   : Double,
    outputPath   : String) =
  {
    val vocab = extendVocabulary (AB, BC, AC, meshXML)
    logger.debug (s"article path count: ${articlePaths.count}")
    articlePaths.collect.foreach { a =>
      logger.debug (s"--> $a")
    }
    val articles = generatePairs (articlePaths, meshXML, sampleSize, lexerConf)
    val annotatedArticles = annotateBinaries (articles, AB, BC, AC)
    calculateTriples (annotatedArticles, outputPath)
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
    val AB = Processor.getFacts (sparkContext, ctdABPath, ctdSampleSize, a = 0, b = 3, code = 1, pmids = 10)
    val BC = Processor.getFacts (sparkContext, ctdBCPath, ctdSampleSize, a = 0, b = 2, code = 2, pmids = 8)
    val AC = Processor.getFacts (sparkContext, ctdACPath, ctdSampleSize, a = 0, b = 3, code = 3, pmids = 9)

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


    System.setOut (new BannerFilterPrintStream (System.out))


    pipeline.execute ()    
  }
}

