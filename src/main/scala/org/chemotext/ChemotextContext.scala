package org.chemotext

import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.HashMap
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.compat.Platform
import scala.xml.XML
import scala.xml.PrettyPrinter
/*
import scala.xml.Elem
import scala.xml.factory.XMLLoader
import javax.xml.parsers.SAXParser
object IgnoreDTDXML extends XMLLoader[Elem] {
  override def parser: SAXParser = {
    val f = javax.xml.parsers.SAXParserFactory.newInstance()
    f.setNamespaceAware(false)
    f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
    f.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
    f.newSAXParser()
  }
}
 */

import org.deeplearning4j.models.word2vec.{ Word2Vec }
import org.deeplearning4j.text.sentenceiterator.{ SentenceIterator, CollectionSentenceIterator }
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory
import org.deeplearning4j.text.tokenization.tokenizer.Tokenizer

/***
 * Configuration for Chemotext.
 */
case class ChemotextConfig (
  environment : String,
  collections : Map[String,Collection]);

/***
 * Items which can be configured via the accompnaying text
 * based configuration file.
 */
case class Collection (
  name        : String,
  description : String,
  categories  : Map[String,Map[String,String]]);

/***
 * An API for chemotext. Abstracts chemotext automation.
 */
class ChemotextContext (
  sparkContext : SparkContext,
  dataPath     : String = "../data/pubmed",
  configFile   : String = "etc/chemotext/chemotext.conf")
{
  val logger = LoggerFactory.getLogger("ChemotextContext")
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

  def execute () = {
    val batchSize = 1000
    val iterations = 50
    val layerSize = 500
    val tokenizer = new DefaultTokenizerFactory()
    val sentenceIterator = new CollectionSentenceIterator (PubmedArticleVisitor.parse (dataPath))
    val vec = new Word2Vec.Builder()
      .batchSize(batchSize)   //# words per minibatch.
      .sampling(1e-5)         // negative sampling. drops words out
      .minWordFrequency(15)
      .useAdaGrad(false)
      .layerSize(layerSize)   // word feature vector size
      .iterations(iterations) // # iterations to train
      .learningRate(0.025)
      .minLearningRate(1e-2)  // learning rate decays wrt # words. floor learning
      .negativeSample(0)     // sample size 10 words
      .iterate(sentenceIterator)
      .tokenizerFactory(tokenizer)
      .build()

    vec.fit

    logger.info ("Evaluate model....")
    var sim = vec.similarity("Cyclin", "protein")
    logger.info(s"Similarity between Cyclin and protein: $sim")

    sim = vec.similarity("Cyclin", "wombat")
    logger.info(s"Similarity between Cyclin and wombat: $sim")

    Array("protein", "disease", "drug", "compound", "efficacy", "cancer", "cyclin").foreach { word =>
      vec.wordsNearest (word, 40).foreach { analog => 
        if (! analog.contains ("(") && ! analog.contains (")")) {
          logger.info (s"words nearest $word: $analog")
        }
      }
    }

  }

  def execute0 () = {
    sparkContext.parallelize (Pubmed.getArticles (dataPath)).map { articlePath =>
      Pubmed.getSentences (articlePath)
    }.map { sentences =>
      println (sentences)
    }
  }

}

import java.io.{ IOException, File }
import java.nio.file.{ FileVisitor, Path, Files, FileSystems, FileVisitResult, SimpleFileVisitor }
import java.nio.file.attribute.BasicFileAttributes

trait PathVisitor extends FileVisitor[Path] {
  def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult
  def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult
  def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult
  def visitFileFailed(file: Path, exc: IOException): FileVisitResult
}

object PathVisitor {
  class Simple extends SimpleFileVisitor[Path] with PathVisitor { }
  val Continue     = FileVisitResult.CONTINUE
  val SkipSiblings = FileVisitResult.SKIP_SIBLINGS
  val SkipSubtree  = FileVisitResult.SKIP_SUBTREE
  val Terminate    = FileVisitResult.TERMINATE
  def apply(f: (Path, BasicFileAttributes) => FileVisitResult): PathVisitor = new Simple {
    override def visitFile(file: Path, attrs: BasicFileAttributes):  FileVisitResult = f(file, attrs)
  }
}

object PubmedArticleVisitor extends PathVisitor {
  val sentences : ListBuffer[String] = ListBuffer[String] ()
  var count = 0
  val max = 1000

  def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = PathVisitor.Continue
  def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = PathVisitor.Continue
  def visitFileFailed(file: Path, exc: IOException): FileVisitResult = PathVisitor.Continue

  def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
    val fileName = path.toString
    if (fileName.endsWith (".fxml") && fileName.contains ("3_Biotech") && ( count < max ) ) {
      count += 1
      println (s" ----> visiting $path")

      (XML.loadFile (path.toFile) \\ "article" \\ "body" \\ "sec" \\ "p" ).map { para =>
        println (s"text -> ${para.text}")
        val sentenceSet = para.text.split ("\\. ").foreach { sentence =>
          println (s"    --> $sentence")
          sentences += sentence
        }
      }
    }
    if (count < max) PathVisitor.Continue else PathVisitor.Terminate
  }

  def parse (dataPath : String) : List[String] = {
    val path = FileSystems.getDefault().getPath (dataPath)
    print (s"path: $path")
    val visitor = PathVisitor (visitFile)
    Files.walkFileTree (path, visitor)
    sentences.toList
  }

}



object Pubmed extends PathVisitor {
  val articles : ListBuffer[String] = ListBuffer[String] ()
  var count = 0
  val max = 1000

  def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = PathVisitor.Continue
  def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = PathVisitor.Continue
  def visitFileFailed(file: Path, exc: IOException): FileVisitResult = PathVisitor.Continue

  def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
    val fileName = path.toString
    if (fileName.endsWith (".fxml") && ( count < max ) ) {
      count += 1
      articles += fileName
    }
    if (count < max) PathVisitor.Continue else PathVisitor.Terminate
  }

  def getArticles (dataPath : String) : List[String] = {
    val path = FileSystems.getDefault().getPath (dataPath)
    print (s"path: $path")
    val visitor = PathVisitor (visitFile)
    Files.walkFileTree (path, visitor)
    articles.toList
  }

  def getSentences ( fileName : String ) : List[String] = {
    val sentences : ListBuffer[String] = new ListBuffer[String] ()
    val file = new File (fileName)
    (XML.loadFile (file) \\ "article" \\ "body" \\ "sec" \\ "p" ).map { para =>
      println (s"text -> ${para.text}")
      val sentenceSet = para.text.split ("\\. ").foreach { sentence =>
        println (s"    --> $sentence")
        sentences += sentence
      }
    }
    sentences.toList
  }

}

