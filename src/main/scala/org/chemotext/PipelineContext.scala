package org.chemotext

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
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
import org.json4s._
import org.deeplearning4j.models.word2vec.{ Word2Vec }
import org.deeplearning4j.text.sentenceiterator.{ SentenceIterator, CollectionSentenceIterator }
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory
import org.deeplearning4j.text.tokenization.tokenizer.Tokenizer

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

/*
object Spark {
  val ctx = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
}
 */

/***
 * Processor for searching articles for terms.
 * Implemented as an object to interoperate with
 * Spark serialization and worker semantics.
 */
object Processor {

  val logger = LoggerFactory.getLogger("Processor")

  // https://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.ml.classification.MultilayerPerceptronClassifier
  def executeMultiLayerPerceptronClassifier (train : DataFrame, test : DataFrame) = {
    val layers = Array[Int](4, 5, 4, 3)
    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)

    // train the model
    val model = trainer.fit(train)

    // compute precision on the test set
    val result = model.transform(test)
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("precision")
    println("Precision:" + evaluator.evaluate(predictionAndLabels))
  }

  def findTriples (article : String, meshXML : String) 
      : List[( String, String, String)] =
  {
    findTriples (findPairs (article, meshXML))
  }

  def findTriples (pairs : List[List[(String, String, Float)]])
      : List[( String, String, String)] =
  {
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
  def findPairs (article : String, meshXML : String) 
      : List[List[(String, String, Float)]] = 
  {
    findPairs (quantifyArticle (article, meshXML))
  }

  def findPairs (words : List[List[(String, Int, Int, Int)]]) 
      : List[List[(String, String, Float)]] = 
  {
    val threshold = 100
    List (
      findCooccurring (words (0), words (1), threshold),
      findCooccurring (words (1), words (2), threshold),
      findCooccurring (words (0), words (2), threshold)
    )
  }

  /**
    * Determine pairs based on distance and include an confidence score 
    */
  def findCooccurring (
    L         : List[(String, Int, Int, Int)],
    R         : List[(String, Int, Int, Int)],
    threshold : Int)
      : List[(String, String, Float)] =
  {
    L.flatMap { left =>
      R.map { right =>
        val difference = math.abs (left._2 - right._2) // comparing doc pos
        if ( difference < threshold && difference > 0) {
          logger.debug (s"     difference $difference")
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
  def quantifyArticle (article : String, meshXML : String) 
      : List[List[(String, Int, Int, Int)]] =
  {
    var A : List[(String, Int, Int, Int)] = List ()
    var B : List[(String, Int, Int, Int)] = List ()
    var C : List[(String, Int, Int, Int)] = List ()
    val mesh = MeSHFactory.getMeSH (meshXML)
    logger.info (s"@-article: ${article}")
    var docPos = 0
    var paraPos = 0
    var sentPos = 0
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
    List (A, B, C)
  }

  /**
    * Record locations of words within the document.
    */
  def getDocWords (words : Array[String], text : Array[String], docPos : Int, paraPos : Int, sentPos : Int) :
      List[(String, Int, Int, Int)] =
  {
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

  def executeChemotextPipeline (rdd : RDD[String], meshXML : String, ctd : RDD[String]) = {
    val words = rdd.map { a =>
      ( a, s"$meshXML" )
    }.sample (false, 0.001).map { article =>
      Processor.quantifyArticle (article._1, article._2)
    }.map { item =>
      Processor.findPairs (item)
    }.flatMap { item =>
      Processor.findTriples (item)
    }.map { triple =>
      ( triple, 1 )
    }.reduceByKey (_ + _).map { tuple =>
      ( tuple._1._1, tuple._1._2, tuple._1._3, tuple._2 )
    }.sortBy (elem => -elem._4)

    words.foreach { a =>
      logger.info (s"${a}")
    }

    compareCTD (words.collect (), ctd)
  }

  /**
    * ChemicalName,ChemicalID,CasRN,DiseaseName,DiseaseID,DirectEvidence,InferenceGeneSymbol,InferenceScore,OmimIDs,PubMedIDs
    * http://ctdbase.org/
    * parse CTD
    * find matching triples
    * create perceptron training set
    */
  def compareCTD (words : Array[(String, String, String, Int)], ctd : RDD[String]) = {
    ctd.filter { line =>
      ! line.startsWith ("#")
    }.map { line =>
      line.split(",").map { elem => elem.trim }
    }.map { tuple =>
      ( tuple(0).toLowerCase ().replaceAll ("\"", ""),
        tuple(3).toLowerCase ().replaceAll ("\"", "") )
    }.flatMap { tuple =>
      words.filter {  w => 
        tuple._1.equals (w._1) && tuple._2.equals (w._3)
      }
    }.distinct().collect ().foreach { t =>
      println (s" $t")
    }

  }
}

/***
 * An API for chemotext. Abstracts chemotext automation.
 */
class PipelineContext (
  sparkContext    : SparkContext,
  appHome         : String = "../data/pubmed",
  meshXML         : String = s"../data/pubmed/mesh/desc2016.xml",
  articleRootPath : String = s"../data/pubmed/articles")
{
  val sqlContext = new SQLContext(sparkContext)
  import sqlContext.implicits._

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
    /*
    fileList = JSONUtils.readJSON2 (fileListPath)
    if (fileList == null) {
      fileList = recursiveListFiles (articleRootDir, articleRegex).map (_.getCanonicalPath)
      JSONUtils.writeJSON (fileList, fileListPath)
    }
    fileList
     */
  }

  def analyzeDocuments () = {
    val extendedMesh = "---"
    val articleRootDir = new File (articleRootPath)
    val articleRegex = new Regex (".*.fxml")
    val articleList = getFileList (articleRootDir, articleRegex)

    val ctdPath = "../data/pubmed/ctd/CTD_chemicals_diseases.csv"
    val ctd = sparkContext.textFile(ctdPath)
    Processor.executeChemotextPipeline (sparkContext.parallelize (articleList), meshXML, ctd)


    //val ctd = sqlContext.read.load("../data/pubmed/ctd/CTD_chemicals_diseases.csv").rdd
    //val ctdRDD : RDD[Row] = ctd.rdd

/*
    triples.map { triple =>
      ctd.map { ctdRec =>
        println (s" $ctdRec")
      }
    }
 */

  }

  def execute () = {
    logger.info ("Searching documents for term relationships.")
    analyzeDocuments () //appHome, articleRootPath, meshXML)
  }
}

