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

  type WordPair = (String, String, Double, Double, Double)
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
      findCooccurring (words (0), words (1), threshold), // AB
      findCooccurring (words (1), words (2), threshold), // BC
      findCooccurring (words (0), words (2), threshold)  // AC
    )
  }

  /**
    * Determine pairs based on distance and include an confidence score 
    */
  def findCooccurring (L : List[WordPosition], R : List[WordPosition], threshold : Int) : List[WordPair] = {
    L.flatMap { left =>
      R.map { right =>
        val docPosDifference = math.abs (left._2 - right._2).toDouble
        if ( docPosDifference < threshold && docPosDifference > 0 ) {
          ( left._1,
            right._1,
            docPosDifference,
            math.abs (left._3 - right._3).toDouble,
            math.abs (left._4 - right._4).toDouble
          )
        } else {
          ( null, null, 0.0, 0.0, 0.0 )
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
    val mesh = MeSHFactory.getMeSH (meshXML)
    logger.info (s"@-article: ${article}")
    var docPos = 0
    var paraPos = 0
    var sentPos = 0
    try {
      val xml = XML.loadFile (article)
      val paragraphs = (xml \\ "p")
      paragraphs.foreach { paragraph =>
        val text = paragraph.text.split ("(?i)(?<=[.?!])\\S+(?=[a-z])").map (_.toLowerCase)
        A = A.union (getDocWords (mesh.chemicals.toArray, text, docPos, paraPos, sentPos))
        B = B.union (getDocWords (mesh.proteins.toArray, text, docPos, paraPos, sentPos))
        C = C.union (getDocWords (mesh.diseases.toArray, text, docPos, paraPos, sentPos))
        sentPos += text.size
        docPos += paragraph.text.length
        paraPos += 1
      }
    } catch {
        case e: Exception =>
          logger.error (s"Error reading json $e")
    }
    List (A, B, C)
  }

  /**
    * Record locations of words within the document.
    */
  def getDocWords (words : Array[String], text : Array[String], docPos : Int, paraPos : Int, sentPos : Int) :
      List[WordPosition] =
  {
    var textPos = 0
    var sentenceIndex = 0
    var result : ListBuffer[WordPosition] = ListBuffer ()
    if (words != null && text != null) {
      text.foreach { sentence =>
        words.foreach { word =>
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
        sentenceIndex += 1
        textPos += sentence.length ()
      }
    }
    result.toList
  }

  def executeChemotextPipeline (articlePaths : RDD[String], meshXML : String, ctd : RDD[String], sampleSize : Double) = {

    logger.info ("== Analyze articles; calculate binaries.")
    val binaries = articlePaths.map { a =>
      ( a, s"$meshXML" )
    }.sample (false, sampleSize).map { article =>
      Processor.quantifyArticle (article._1, article._2)
    }.map { item =>
      Processor.findPairs (item)
    }.cache ()

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

    logger.info ("== Get known triples from CTD.")
    val knownTriples = getKnownTriples (triples.collect (), ctd).collect ()

    knownTriples.foreach { a =>
      logger.info (s"known-triple :-- ${a}")
    }

    logger.info ("== Label known binaries based on known triples...")
    logger.info ("== Calculate logistic regression model.")
    val flatBinaries = binaries.flatMap { item =>
      item
    }.flatMap { item =>
      item
    }.cache ()

    logger.info ("== Calculate ab binaries")
    val abBinaries = flatBinaries.flatMap { item =>
      // Find known A->B binaries and associate their features.
      knownTriples.filter { known =>
        known._1.equals (item._1) && known._2.equals (item._2)
      }.map { k =>
        ( k._1, k._2, item._3, item._4, item._5 )
      }
    }.distinct().cache ()

    logger.info ("== Calculate bc binaries")
    val bcBinaries = flatBinaries.flatMap { item =>
      // Find known B->C binaries and associate their features.
      knownTriples.filter { known =>
        known._2.equals (item._1) && known._3.equals (item._2)
      }.map { k =>
        ( k._2, k._3, item._3, item._4, item._5 )
      }
    }.distinct().cache ()

    logger.info ("== Calculating log regression model.")
    val LogRM = new LogisticRegressionWithLBFGS().run(
      abBinaries.union (bcBinaries).map { item =>
        new LabeledPoint (
          label    = 1,
          features = new DenseVector (Array( item._3, item._4, item._5 ))
        )
      })

    logger.info ("== Use LogRM to associate probabilities with each binary.")
    val abProb = abBinaries.map { binary =>
      ( binary._1, binary._2,
        LogRM.predict (new DenseVector (Array ( binary._3, binary._4, binary._5 ))) )
    }.distinct().cache ()
    abProb.collect().foreach { item =>
      logger.info (s"AB with prob: $item")
    }

    val bcProb = bcBinaries.map { binary =>
      ( binary._1, binary._2,
        LogRM.predict (new DenseVector (Array ( binary._3, binary._4, binary._5 ))) )
    }.distinct().cache ()
    bcProb.collect ().foreach { item =>
      logger.info (s"BC with prob: $item")
    }

    logger.info ("== Hypotheses:")
    val join = abProb.map { i =>
      (i._2, (i._1, i._3, 1 ))
    }.cogroup (
      bcProb.map { j =>
        (j._1, (j._2, j._3, 2 ))
      }
    ).cache ()

    join.collect().foreach { t =>
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

  /**
    * ChemicalName,ChemicalID,CasRN,DiseaseName,DiseaseID,DirectEvidence,InferenceGeneSymbol,InferenceScore,OmimIDs,PubMedIDs
    * http://ctdbase.org/
    * parse CTD
    * find matching triples
    * create perceptron training set
    */
  def getKnownTriples (triples : Array[(String, String, String, Int)], ctd : RDD[String])
      : RDD[( String, String, String, Int )] =
  {
    ctd.filter { line =>
      ! line.startsWith ("#")
    }.map { line =>
      line.split(",").map { elem => elem.trim }
    }.map { tuple =>
      ( tuple(0).toLowerCase ().replaceAll ("\"", ""),
        tuple(3).toLowerCase ().replaceAll ("\"", "") )
    }.flatMap { tuple =>
      triples.filter {  w => 
        tuple._1.equals (w._1) && tuple._2.equals (w._3)
      }
    }.distinct().cache ()
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
  ctdPath         : String = "../data/pubmed/ctd/CTD_chemicals_diseases.csv",
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

    var model : Word2VecModel = null
    if (Files.exists(Paths.get(vectorModelPath))) {
      model = Word2VecModel.load(sparkContext, vectorModelPath)
    } else {
      if (! Files.exists (Path.get (corpusPath))) {
        Processor.createCorpus (articleList, corpusPath)
      }
      val words = sparkContext.textFile (corpusPath).map { e => e.split (" ").toList }
      model = Processor.vectorizeCorpus (words)
      model.save(sparkContext, vectorModelPath)
    }

    Processor.executeChemotextPipeline (
      articlePaths = sparkContext.parallelize (articleList),
      meshXML      = meshXML,
      ctd          = sparkContext.textFile(ctdPath),
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
    val ctdPath = args(5)

    logger.info (s"appName: $appName")
    logger.info (s"appHome: $appHome")
    logger.info (s"articleRootPath: $articleRootPath")
    logger.info (s"meshXML: $meshXML")
    logger.info (s"sampleSize: $sampleSize")
    logger.info (s"ctdPath: $ctdPath")

    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val pipeline = new PipelineContext (sc, appHome, meshXML, articleRootPath, ctdPath, sampleSize)
    pipeline.execute ()    
  }
}

