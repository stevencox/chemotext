package org.chemotext

import java.io.FileReader
import java.io.Reader
import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import scala.collection.JavaConverters._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/***
 * Base trait for Chemotext programs.
 */
trait ProcessBase {

  val logger = LoggerFactory.getLogger("ProcessBase")

  /***
   * Common implementation of getting and reading a configuration.
   */
   def getConfiguration (uri:String): SparkConf = {

    logger.info("Getting configuration....");
    assert (uri != null, "Non-null configuration URI required.")

    val sparkConf = new SparkConf ()
    var reader : Reader = null

    try {
      reader = new FileReader (uri)

      val properties = new Properties

      properties.load (reader)

      properties.asScala.map (element => {

        val key = element._1
        val value = element._2

        if (key.equals ("appName")) {

          logger.info ("setting appName: " + value)
          sparkConf.setAppName (value)

        } else if (key.equals ("master")) {

          logger.info ("setting master: " + value)
          sparkConf.setMaster (value)

        } else {

          logger.info ("setting " + key + " => " + value)
          sparkConf.set (key, value)

        }
      })

    } finally {

      if (reader != null) {
        reader.close ()
      }

    }

    val mesosMaster = System.getenv ("MESOS_MASTER")

    if (mesosMaster != null) {
      logger.info ("configuring mesos: " + mesosMaster)
      sparkConf.setMaster (mesosMaster)
    }

    val sparkExecutorURI = System.getenv ("SPARK_EXECUTOR_URI")

    if (sparkExecutorURI != null) {
      logger.info ("configuring spark executor uri: " + sparkExecutorURI)
      sparkConf.set ("spark.executor.uri", sparkExecutorURI)
    }

    return sparkConf
  }

  def getSparkConf (uri:String) : SparkConf = {
    assert (uri != null, "Non-null URI required to load analytic configuration")
    val sparkConf : SparkConf = getConfiguration (uri)

    return sparkConf
  }

  def getSparkContext (uri:String) : SparkContext = {
    val sparkConf : SparkConf = getSparkConf (uri)
    val sparkContext = new SparkContext (sparkConf)

    return sparkContext
  }

}

/***
 * A base class for analytics that abstracts the work of
 * reading configuration files in a standard way.
 * Uses reflection to dynamically instantiate the class to execute
 * and configures it given the loaded configuration.
 */
class ProcessAnalytic extends ProcessBase {

  override val logger = LoggerFactory.getLogger("ProcessAnalytic")

  def run ( service : String ) = {

    var result : Object = null

    try {
      var sparkConf = getConfiguration (service)

      var className = sparkConf.get ("className")
      var theClass = Class.forName (className)
      var constructor = theClass.getConstructor(classOf[SparkConf])

      sparkConf.setJars (SparkContext.jarOfClass (theClass).toSeq)
      sparkConf.setSparkHome (System.getenv("SPARK_HOME"))

      var instance = constructor.newInstance(sparkConf)
      var run = theClass.getMethod ("run")

      result = run.invoke (instance)

    } catch {
      case e @ (_ : RuntimeException | _ : java.io.IOException) => println(e.getMessage ())
      logger.error (e.getMessage ())
    }

    result
  }
}

/***
  * A generic main program for loading and running analytics.
  */
object ProcessMain {

  def main ( service : Array[String] ) {
    val process : ProcessAnalytic = new ProcessAnalytic ()

    process.run (service(0))
  }

}

