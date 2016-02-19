package org.chemotext

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import SparkContext._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/***
  * An analytic for automated chemotext and publication of event data.
  */
class Pipeline (theSparkConf : SparkConf) extends Serializable {

  val sparkConf : SparkConf = theSparkConf
  val log = LoggerFactory.getLogger("Chemotext Pipeline")
  val pipelineContext = init ()
 
  def getPipelineContext () = pipelineContext

  /***
   * Initialize
   */
  def init () : PipelineContext = {

    val sc = new SparkContext (sparkConf)
    val appHome = sparkConf.get ("appHome")
    val meshXML = sparkConf.get ("meshXML")
    val articleRootPath = sparkConf.get ("articleRootPath")

    /**********************************************************
     ** We create a chemotext context connected to the current
     ** spark context.
     **********************************************************/
    new PipelineContext (sc, appHome, meshXML, articleRootPath)
  }

  /***
   * Run the analytic.
   */
  def execute (exit : Boolean = true) = {

    pipelineContext.execute ()

    if (exit)
      System.exit (0)
  }
  def run () = {
    execute (true)
  }

}

