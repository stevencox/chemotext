package org.chemotext

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.compat.Platform
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import scala.util.matching.Regex
import scala.xml.XML
import org.json4s._
import org.deeplearning4j.models.word2vec.{ Word2Vec }
import org.deeplearning4j.text.sentenceiterator.{ SentenceIterator, CollectionSentenceIterator }
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory
import org.deeplearning4j.text.tokenization.tokenizer.Tokenizer
import org.apache.spark.sql.DataFrame

//import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.util.MLUtils


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

  // checking a b c to verify they're really a b c

  // train this for whole set of docs
  //    logistic regression
  //       shortest distance
  //       average distance
  //       number of hits

  def trainLogisticRegressionModel (trainingSet : RDD[LabeledPoint]) : LogisticRegressionModel = {
    new LogisticRegressionWithLBFGS().run(trainingSet)

    /*
    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
     */
  }

  def findTriples (article : String, meshXML : String) 
      : List[( String, String, String)] =
  {
    findTriples (findPairs (article, meshXML))
  }

  def findTriples (pairs : List[List[(String, String, Double, Double, Double)]])
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

  def findProbTriples (
    AB : Array[(String, String, Double)],
    BC : Array[(String, String, Double)])
      : Array[( String, String, String, Double)] =
  {
    logger.debug (s" Finding triples in AB/BC pair lists ${AB.length}/${BC.length} long")
    AB.flatMap { ab =>
      BC.map { bc =>
        if (ab._2.equals (bc._1)) {
          ( ab._1, ab._2, bc._2, ab._3 * bc._3 )
        } else {
          ( null, null, null, 0.0 )
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
      : List[List[(String, String, Double, Double, Double)]] = 
  {
    findPairs (quantifyArticle (article, meshXML))
  }

  def findPairs (words : List[List[(String, Int, Int, Int)]]) 
      : List[List[(String, String, Double, Double, Double)]] = 
  {
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
  def findCooccurring (
    L         : List[(String, Int, Int, Int)],
    R         : List[(String, Int, Int, Int)],
    threshold : Int)
      : List[(String, String, Double, Double, Double)] =
  {
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
    val LogRM = trainLogisticRegressionModel (
      abBinaries.union (bcBinaries).map { item =>
        new LabeledPoint (
          label    = 1,
          features = new DenseVector (Array( item._3, item._4, item._5 ))
        )
      })

    logger.info ("== Use LogRM to associate probabilities with each binary.")
    val abWithProb = abBinaries.map { binary =>
      ( binary._1, binary._2,
        LogRM.predict (new DenseVector (Array ( binary._3, binary._4, binary._5 ))) )
    }.distinct().cache ()
    abWithProb.collect().foreach { item =>
      logger.info (s"AB with prob: $item")
    }

    val bcWithProb = bcBinaries.map { binary =>
      ( binary._1, binary._2,
        LogRM.predict (new DenseVector (Array ( binary._3, binary._4, binary._5 ))) )
    }.distinct().cache ()
    bcWithProb.collect ().foreach { item =>
      logger.info (s"BC with prob: $item")
    }

    logger.info ("== Hypotheses:")
    findProbTriples (abWithProb.collect (), bcWithProb.collect ()).foreach { triple =>
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
    /*
    fileList = JSONUtils.readJSON2 (fileListPath)
    if (fileList == null) {
      fileList = recursiveListFiles (articleRootDir, articleRegex).map (_.getCanonicalPath)
      JSONUtils.writeJSON (fileList, fileListPath)
    }
    fileList
     */
  }

  def execute () = {
    val articleRootDir = new File (articleRootPath)
    val articleRegex = new Regex (".*.fxml")
    val articleList = getFileList (articleRootDir, articleRegex)

    Processor.executeChemotextPipeline (
      articlePaths = sparkContext.parallelize (articleList),
      meshXML      = meshXML,
      ctd          = sparkContext.textFile(ctdPath),
      sampleSize   = sampleSize.toDouble)
  }
}

