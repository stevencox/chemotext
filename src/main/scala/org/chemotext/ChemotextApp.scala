package org.chemotext

import java.nio.file.{Paths, Files}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.rogach.scallop.Scallop
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.slf4j.Logger
import org.slf4j.LoggerFactory

case class ChemotextConfig (
  articlePath       : String,
  outputPath        : String,
  dataHome          : String,
  meshXML           : String,
  sampleSize        : Double,
  distanceThreshold : Int,
  chemlex           : String,
  diseaselex        : String,
  slices            : Int)

case class CTDConfig (
  ctdACPath : String,
  ctdABPath : String,
  ctdBCPath : String)

object ChemotextApp {

  val logger = LoggerFactory.getLogger ("Chemotext2App")

  def formChemotextConfigPath (chemotextConfig : ChemotextConfig) = {
    Paths.get (chemotextConfig.outputPath, "chemotextOpts.json").toFile().getCanonicalPath ()
  }

  def main(args: Array[String], sc : SparkContext = null) {

    val opts = Scallop (args)
      .version("v1.0.0 (c) 2016 Chemotext2") // --version option is provided for you
      .banner("""Usage: Chemotext2 [OPTION]...
                |Chemotext2 searches medical literature for testable toxicology hypotheses.
                |Options:
                |""".stripMargin) // --help is provided, will also exit after printing version,
                                  // banner, options usage, and footer
      .footer("\n(c) UNC-CH / RENCI")

      .opt[String]("name",     descr = "Name of the application.")
      .opt[String]("dataHome", descr = "System root data directory")
      .opt[String]("articles", descr = "Root directory of articles to analyze")
      .opt[String]("mesh",     descr = "Path to MeSH XML definition file")
      .opt[String]("chemlex",  descr = "Chemical lexer to use (mesh|tmchem)")
      .opt[Double]("sample",   descr = "Sample size to apply to total article collection")
      .opt[String]("ctdAC",    descr = "Path to CTD AC data file")
      .opt[String]("ctdAB",    descr = "Path to CTD AB data file")
      .opt[String]("ctdBC",    descr = "Path to CTD BC data file")
      .opt[String]("output",   descr = "Output directory for process output")
      .opt[Int]   ("slices",   descr = "Total number of slices of article data")
      .opt[String]("lexerConfig",       descr = "Lexical analyzer configuration path (tmChem)")
      .opt[String]("lexerCache",        descr = "Lexical analyzer cache file path (tmChem)")
      .opt[String]("lexerDict",         descr = "Lexical analyzer dictionary path (tmChem)")
      .opt[Int]   ("distanceThreshold", descr = "Threshold distance between terms to constitute a binary")

    // http://alvinalexander.com/scala/how-to-load-xml-file-in-scala-load-open-read
    val appName = opts[String]("name")

    val ctdConfig = CTDConfig (
      ctdACPath = opts[String]("ctdAC"),
      ctdABPath = opts[String]("ctdAB"),
      ctdBCPath = opts[String]("ctdBC"))

    val tmChemConf = TmChemLexerConf (
      configPath     = opts[String]("lexerConfig"),
      cacheFileName  = opts[String]("lexerCache"),
      dictionaryPath = opts[String]("lexerDict"))

    val chemotextConfig = ChemotextConfig (
      articlePath       = opts[String]("articles"),
      outputPath        = opts[String]("output").replaceFirst("^(hdfs://|file://)",""),
      dataHome          = opts[String]("dataHome").replaceFirst("^(hdfs://|file://)",""),
      meshXML           = opts[String]("mesh"),
      sampleSize        = opts[Double]("sample"),
      distanceThreshold = opts[Int]   ("distanceThreshold"),
      chemlex           = opts[String]("chemlex"),
      diseaselex        = "mesh",
      slices            = opts[Int]   ("slices"))

    JSONUtils.writeJSON (
      chemotextConfig,
      formChemotextConfigPath (chemotextConfig))

    logger.info (s"appName         : $appName")
    logger.info (s"chemotextConfig : ${JSONUtils.writeString(chemotextConfig)}")
    logger.info (s"tmChemConf      : ${JSONUtils.writeString(tmChemConf)}")
    logger.info (s"ctdConfig       : ${JSONUtils.writeString(ctdConfig)}")

    // Connect to Spark
    var sparkContext = sc
    if (sparkContext == null) {
      val conf = new SparkConf().setAppName (appName)
      sparkContext = new SparkContext (conf)
    }

    // Create and execute the pipeline
    val chemotext = new ChemotextContext (
      sparkContext    = sparkContext,
      chemotextConfig = chemotextConfig,
      lexerConf       = tmChemConf,
      ctdConfig       = ctdConfig)

    chemotext.execute ()    
  }
}

