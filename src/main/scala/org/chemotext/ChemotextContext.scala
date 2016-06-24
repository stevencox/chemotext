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
 * An API for chemotext. Abstracts chemotext automation.
 */
class ChemotextContext (
  sparkContext    : SparkContext,
  chemotextConfig  : ChemotextConfig,
  lexerConf       : TmChemLexerConf,
  ctdConfig       : CTDConfig)
{
  val logger = LoggerFactory.getLogger ("ChemotextContext")

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
      logger.info (s"Loaded existing file list from: $fileListPath")
      implicit val formats = DefaultFormats
      json.extract[Array[String]]
    } else {
      logger.info (s"Generating file list...")
      fileList = recursiveListFiles (articleRootDir, articleRegex).map (_.getCanonicalPath)
      JSONUtils.writeJSON (fileList, fileListPath)
      fileList
    }
  }

  def generateSlices () : List[ArrayBuffer[String]] = {
    val articleRootDir = new File (chemotextConfig.articlePath)
    val articleRegex = new Regex (".*.fxml")
    val articleList = getFileList (articleRootDir, articleRegex)
    val sliceBuffer = ListBuffer[ArrayBuffer[String]] ()
    if (chemotextConfig.slices == 1) {
      logger.info (s"Slice (one slice) ${articleList.size} files.")
      sliceBuffer += articleList.to[ArrayBuffer]
    } else {
      val sliceSize = articleList.size / chemotextConfig.slices
      for (sliceId <- 0 to chemotextConfig.slices - 1) {
        val start = sliceSize * sliceId
        val articleListSlice = articleList.slice (start, start + sliceSize)
        sliceBuffer += articleListSlice.to[ArrayBuffer]
        logger.info (s"Slice ${sliceId} processing ${articleListSlice.size} files.")
      }
    }
    sliceBuffer.toList
  }

  def execute () = {
    val ctdSampleSize = 1.0
    val AB = ChemotextProcessor.getFacts (sparkContext, ctdConfig.ctdABPath, ctdSampleSize, a = 0, b = 3, code = 1, pmids = 10)
    val BC = ChemotextProcessor.getFacts (sparkContext, ctdConfig.ctdBCPath, ctdSampleSize, a = 0, b = 2, code = 2, pmids = 8)
    val AC = ChemotextProcessor.getFacts (sparkContext, ctdConfig.ctdACPath, ctdSampleSize, a = 0, b = 3, code = 3, pmids = 9)

    val sliceBuffer = generateSlices ()
    for ( articleListSlice <- sliceBuffer ) {
      logger.info (s"--> Processing slice of ${articleListSlice.size} files")
      ChemotextProcessor.execute (
        articlePaths    = sparkContext.parallelize (articleListSlice),
        chemotextConfig = chemotextConfig,
        lexerConf       = lexerConf,
        AB              = AB,
        BC              = BC,
        AC              = AC
      )
    }
  }
}
