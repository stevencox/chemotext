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
import org.rogach.scallop.Scallop
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
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

/***
 * Processor for searching articles for terms.
 * Implemented as an object to interoperate with
 * Spark serialization and worker semantics.
 */
object ChemotextProcessor {

  val logger = LoggerFactory.getLogger("Chemotext2")

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
    article        : String,
    chemotextConfig : ChemotextConfig,
    lexerConf      : TmChemLexerConf
  )

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
    val articleDigestFileName = formArticleDigestFileName (outputPath, quantified.fileName)
    logger.info (s"*Writing> : $articleDigestFileName")
    JSONUtils.writeJSON (quantified, articleDigestFileName)
    quantified
  }

  def formArticleDigestPath (outputPath : String, articleName : String) = {
    if (articleName != null) {
      var basename = articleName
      if (articleName.indexOf (File.separator) > -1) {
        basename = articleName.replaceAll (".*" + File.separator, "")
      }
      Paths.get (outputPath, basename + ".json")
    } else {
      null
    }
  }
  def formArticleDigestFileName (outputPath : String, articleName : String) = {
    formArticleDigestPath(outputPath, articleName).toFile().getCanonicalPath ()
  }

  /**
    * Derive A->B, B->C, A->C relationships from raw word positions
    */
  def findPairs (config : QuantifierConfig) : QuantifiedArticle = {
    val outputFilePath = formArticleDigestPath (config.chemotextConfig.outputPath, config.article)
    if (! Files.exists (outputFilePath)) {
      findPairs (config, quantifyArticle (config))
    } else {
      logger.info (s"Skip> ${config.article}")
      null
    }
  }

  def findPairs (config : QuantifierConfig, article : QuantifiedArticle) : QuantifiedArticle = {
    article.copy (
      AB = findCooccurring (article.A, article.B, config.chemotextConfig.distanceThreshold, 1),
      BC = findCooccurring (article.B, article.C, config.chemotextConfig.distanceThreshold, 2),
      AC = findCooccurring (article.A, article.C, config.chemotextConfig.distanceThreshold, 3))
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
    val meshXML = config.chemotextConfig.meshXML
    val dataHome = config.chemotextConfig.dataHome
    val vocab = VocabFactory.getVocabulary (dataHome, meshXML)
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
      val A_lexer =
        if (config.chemotextConfig.chemlex == "mesh") new AMeshLexer (dataHome, meshXML)
        else TmChemLexer.getInstance (config.lexerConf)
      val B_lexer = new BMeshLexer (dataHome, meshXML)
      val C_lexer = new CMeshLexer (dataHome, meshXML)
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
      val pmidString = row(pmids)
      var pmidList = Array[String] ()
      if (pmidString != null) {
        pmidList = pmidString.split("\\|")
      }
      Fact (
        L     = row (a),
        R     = row (b),
        code  = code,
        PMIDs = pmidList
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
    chemotextConfig : ChemotextConfig,
    AB             : RDD[Fact],
    BC             : RDD[Fact],
    AC             : RDD[Fact]
  ) = {
    logger.info ("Checking vocabulary...")
    var vocab = VocabFactory.getVocabulary (chemotextConfig.dataHome, chemotextConfig.meshXML)
    if (! vocab.extended) {

      logger.info ("Extending basic (MeSH) vocabulary with terms from CTD...")

      vocab = new Vocabulary (

        A = vocab.A.union (cleanWord (AB.map { e => e.L })).
          distinct.
          filter { w => !w.equals("was") && !w.equals("for") && !w.equals ("large") && !w.equals ("apotosis") },

        B = vocab.B.
          union (cleanWord (AB.map { e => e.R })).
          union (cleanWord (BC.map { e => e.L })).
          filter { w => !w.equals("was") && !w.equals("for") && !w.equals ("large") }.
          distinct,

        C = vocab.C.
          union (cleanWord (AC.map { e => e.R })).
          union (cleanWord (BC.map { e => e.R })).
          filter { w => !w.equals("was") && !w.equals("for") && !w.equals ("large") }.
          distinct,

        extended = true)

      // Cache the extended vocabulary.
      VocabFactory.writeJSON (chemotextConfig.dataHome, vocab)
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

  def calculateTriples (articlesIn : RDD[QuantifiedArticle], outputPath : String) = {
    logger.info ("== Calculate triples.")
    val articles = articlesIn.map { article =>
      findTriples (article, outputPath)
    }

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

    val bcBinaryFacts = bcBinaries.collect ()
    val abBinaryFacts = abBinaries.collect ()
    if(logger.isDebugEnabled()) {
      bcPredicted.collect().foreach { x =>
        logger.info (s"bc binary predicted: $x")
      }
      abPredicted.collect().foreach { x =>
        logger.info (s"ab binary predicted: $x")
      }
      bcBinaryFacts.foreach { x =>
        logger.debug (s"bc binary in ctd: $x")
      }
      abBinaryFacts.foreach { x =>
        logger.debug (s"ab binary in ctd: $x")
      }
    }
    logger.info (s" ab predicted count:       ${abPredicted.count}")
    logger.info (s" bc predicted count:       ${bcPredicted.count}")
    logger.info (s" ab predicted and in ctd:  ${abBinaries.count}")
    logger.info (s" bc predicted and in ctd:  ${bcBinaries.count}")

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

  def generatePairs (
    articlePaths   : RDD[String],
    chemotextConfig : ChemotextConfig,
    lexerConf    : TmChemLexerConf) =
  {
    logger.info ("== Analyze articles; calculate binaries.")
    articlePaths.map { a =>
      ( a, chemotextConfig.meshXML )
    }.sample (false, chemotextConfig.sampleSize, 1234).map { article =>
      findPairs (
        config = QuantifierConfig (
          article         = article._1,
          chemotextConfig = chemotextConfig,
          lexerConf       = lexerConf))
    }.filter { p =>
      p != null
    }.cache ()
  }

  def execute (
    articlePaths    : RDD[String],
    chemotextConfig : ChemotextConfig,
    lexerConf       : TmChemLexerConf,
    AB              : RDD[Fact],
    BC              : RDD[Fact],
    AC              : RDD[Fact]) =
  {
    logger.debug (s"** Chemotext execute: Analyzing ${articlePaths.count} articles.")
    //val vocab = extendVocabulary (chemotextConfig, AB, BC, AC)
    val articles = generatePairs (articlePaths, chemotextConfig, lexerConf)
    val annotatedArticles = annotateBinaries (articles, AB, BC, AC)
    calculateTriples (annotatedArticles, chemotextConfig.outputPath)
  }
}

