package org.chemotext

import scala.util.matching.Regex
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.rogach.scallop.Scallop
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.compat.Platform
import scala.util.control.Breaks._

import org.xml.sax.SAXParseException
import scala.xml.XML

class WordEmbed (sc : SparkContext, articleRoot : String) {

  val logger = LoggerFactory.getLogger("WordEmbed")

  def getCorpus () : RDD[List[String]] = {
    val articleRootDir = new File (articleRoot)
    val articleRegex = new Regex (".*.fxml")
    val articleList = FileList.getFileList (articleRootDir, articleRegex)
    sc.parallelize (articleList).
      flatMap { article =>
        getText (article)
      }.map { text =>
        text.split (" ").toList
      }
  }

  /**
    * Create a concatenated corpus of all of PubMed Central
    */
  def getText (article : String) : List[String] = {
    val buffer = ListBuffer[String] ()
    logger.info ("Creating PMC text corpus")
    try {
      val xml = XML.loadFile (article)
      val paragraphs = (xml \\ "p")
        paragraphs.foreach { paragraph =>
          buffer += paragraph.text
        }
    } catch {
      case e: SAXParseException =>
        logger.error (s"Failed to parse $article")
    }
    buffer.toList
  }


  /**
    * Calculate the vector space of the corpus as a word2vec model
    */
  def vectorizeCorpus (path : String) : Word2VecModel = {
    val word2vec = new Word2Vec ()
    val model = word2vec.fit (getCorpus ())
    model.save (sc, path)
    model
  }

}

object WordEmbedApp {

  val logger = LoggerFactory.getLogger ("WordEmbedApp")

  def main(args: Array[String]) {

    val opts = Scallop (args)
      .version("v1.0.0 (c) 2016 Chemotext2 WordEmbed") // --version option is provided for you
      .banner("""Usage: ct2we [OPTION]...
                |Creates a word embedding matrix from a corpus
                |Options:
                |""".stripMargin) // --help is provided, will also exit after printing version,
                                  // banner, options usage, and footer
      .footer("\n(c) UNC-CH / RENCI")

      .opt[String]("name",     descr = "Name of the application.")
      .opt[String]("articles", descr = "Root directory of articles to analyze")

    val appName = opts[String]("name")
    val articleRootPath = opts[String]("articles")

    logger.info (s"appName        : $appName")
    logger.info (s"articleRootPath: $articleRootPath")

    val conf = new SparkConf().setAppName (appName)
    val sc = new SparkContext (conf)

    val embedding = new WordEmbed (sc, articleRootPath)

  }
}

