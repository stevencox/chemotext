package org.chemotext

import scala.util.matching.Regex
import org.json4s._
import java.io.File
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.compat.Platform

/***
 * Lists files
 */
object FileList {

  val logger = LoggerFactory.getLogger ("FileList")

  def recursiveListFiles(f : File, r : Regex): Array[File] = {
    val these = f.listFiles
    val good = these.filter(f => r.findFirstIn(f.getName).isDefined)
    good ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_, r))
  }

  def getFileList (rootDir : File, articleRegex : Regex) : Array[String] = {
    var fileList : Array[String] = null
    val fileListPath = "filelist.json"

    val json = JSONUtils.readJSON (fileListPath)
    if (json != null) {
      implicit val formats = DefaultFormats
      json.extract[Array[String]]
    } else {
      fileList = recursiveListFiles (rootDir, articleRegex).map (_.getCanonicalPath)
      JSONUtils.writeJSON (fileList, fileListPath)
      fileList
    }
  }
}
