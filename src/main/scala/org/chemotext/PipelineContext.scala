package org.chemotext

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.compat.Platform
import java.io.File
import scala.util.matching.Regex
import scala.xml.XML

import org.deeplearning4j.models.word2vec.{ Word2Vec }
import org.deeplearning4j.text.sentenceiterator.{ SentenceIterator, CollectionSentenceIterator }
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory
import org.deeplearning4j.text.tokenization.tokenizer.Tokenizer

object Spark {
  val ctx = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
}

/***
 * Processor for searching articles for terms.
 * Implemented as an object to interoperate with
 * Spark serialization and worker semantics.
 */
object Processor {

  val logger = LoggerFactory.getLogger("Processor")

  /**
    * Derive A->B, B->C, A->C relationships from raw word positions
    */
  def quantifyPairs (article : String, meshXML : String) : List[List[(String, String, Float)]] = {
    val result : ListBuffer[List[(String, String, Float)]] = new ListBuffer ()
    val words = quantifyArticle (article, meshXML)
    val threshold = 20
    if (words.length == 3) {
      val AB = findCooccurring (words (0), words (1), threshold)
      val BC = findCooccurring (words (1), words (2), threshold)
      val AC = findCooccurring (words (0), words (2), threshold)
      result += AC
      result += BC
      result += AB
    }
    result.toList
  }

  /**
    * Determine pairs based on distance and include an confidence score 
    */
  def findCooccurring (
    L : List[(String, Int, Int, Int)],
    R : List[(String, Int, Int, Int)],
    threshold : Int)
      : List[(String, String, Float)] =
  {
    L.flatMap { left =>
      R.map { right =>
        val difference = math.abs (left._2 - right._2)
        if ( difference < threshold ) {
          ( left._1, right._1, difference / threshold.toFloat )
        } else {
          ( null, null, 0.0f )
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
  def quantifyArticle (article : String, meshXML : String) : ListBuffer[List[(String, Int, Int, Int)]] = {
    val results : ListBuffer[List[(String, Int, Int, Int)]] = ListBuffer ()
    val mesh = MeSHFactory.getMeSH (meshXML)
    logger.info (s"@-article: ${article}")
    var docPos = 0
    var paraPos = 0
    var sentPos = 0
    val xml = XML.loadFile (article)
    val paragraphs = (xml \\ "p")
    paragraphs.foreach { paragraph =>
      val text = paragraph.text.split ("(?i)(?<=[.?!])\\S+(?=[a-z])").map (_.toLowerCase)

      getWordList (mesh.diseases.toArray, text, docPos, paraPos, sentPos, results)
      getWordList (mesh.proteins.toArray, text, docPos, paraPos, sentPos, results)
      getWordList (mesh.chemicals.toArray, text, docPos, paraPos, sentPos, results)

      sentPos += text.size
      docPos += paragraph.length
      paraPos += 1
    }
    results
  }

  /**
    *  Get a list of words in a piece of text.
    *  Add (word, location) tuples to an output vector.
    */
  def getWordList (
    words   : Array[String],
    text    : Array[String],
    docPos  : Int,
    paraPos : Int,
    sentPos : Int,
    results : ListBuffer[List[(String, Int, Int, Int)]]) =
  {
    val buf : ListBuffer[(String, Int, Int, Int)] = new ListBuffer ()
    val result = getDocWords (words, text, docPos, paraPos, sentPos)
    result.foreach { item =>
      buf += item
      logger.debug (s"Adding item $item")
    }
    if (! buf.isEmpty)
      results += buf.toList
  }

  /**
    * Record locations of words within the document.
    */
  def getDocWords (words : Array[String], text : Array[String], docPos : Int, paraPos : Int, sentPos : Int) = {
    var textPos = 0
    var sentenceIndex = 0
    var result : ListBuffer[(String, Int, Int, Int)] = ListBuffer ()
    if (words != null && text != null) {
      text.foreach { sentence =>
        words.foreach { word =>
          val index = sentence.indexOf (word)
          if (index > -1) {
            sentence.split (" ").foreach { token =>
              if (word.equals (token)) {
                result.add ( ( word, docPos + textPos + index, paraPos, sentPos + sentenceIndex ) )
              }
            }
          }
          sentenceIndex += 1
          textPos += sentence.length ()
        }
      }
    }
    result
  }

}

/***
 * An API for chemotext. Abstracts chemotext automation.
 */
class PipelineContext (
  appHome         : String = "../data/pubmed",
  meshXML         : String = s"../data/pubmed/mesh/desc2016.xml",
  articleRootPath : String = s"../data/pubmed/articles")
    extends Serializable 
{
  val logger = LoggerFactory.getLogger("PipelineContext")

  def recursiveListFiles(f: File, r: Regex): Array[File] = {
    val these = f.listFiles
    val good = these.filter(f => r.findFirstIn(f.getName).isDefined)
    good ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_, r))
  }

  def analyzeDocuments (
    appHome         : String,
    articleRootPath : String,
    meshXML         : String) =
  {
    val extendedMesh = "---"
    val articleRootDir = new File (articleRootPath)
    val articleRegex = new Regex (".*.fxml")
    val articleList = recursiveListFiles (articleRootDir, articleRegex)

    var articles = Spark.ctx.parallelize (articleList).map { a =>
      ( a.getCanonicalPath (), s"$meshXML" )
    }.sample (false, 0.0001).cache ()

    logger.info (s"Processing ${articles.count} articles")
    val words = articles.flatMap { article =>
      Processor.quantifyPairs (article._1, article._2)
    }.cache ()

    words.collect ().foreach { a =>
      logger.info (s"${a}")
    }
  }

  def execute () = {
    logger.info ("Searching documents for term relationships.")
    val mesh = MeSHFactory.getMeSH (meshXML)
    analyzeDocuments (appHome, articleRootPath, meshXML)
  }
}

