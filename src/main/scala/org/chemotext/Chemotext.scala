package org.chemotext

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import SparkContext._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/***
  * An analytic for automated chemotext and publication of event data.
  */
class ChemotextAnalytic (theSparkConf : SparkConf) extends Serializable {

  val sparkConf : SparkConf = theSparkConf
  val log = LoggerFactory.getLogger("ChemotextAnalytic")
  val chemotextContext = init ()

  def getChemotextContext () = chemotextContext

  /***
   * Initialize
   */
  def init () : ChemotextContext = {

    val sc = new SparkContext (sparkConf)
    val dataPath = sparkConf.get ("dataPath")
    val configFile = sparkConf.get ("configuration")

    /**********************************************************
     ** We create a chemotext context connected to the current
     ** spark context.
     **********************************************************/
    new ChemotextContext (sc, dataPath, configFile)
  }

  /***
   * Run the analytic.
   */
  def execute (exit : Boolean = true) = {

    chemotextContext.execute ()

    if (exit)
      System.exit (0)
  }
  def run () = {
    execute (true)
  }

}

