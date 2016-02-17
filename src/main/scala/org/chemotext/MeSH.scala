package org.chemotext

import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.compat.Platform
import scala.xml.PrettyPrinter
import java.io.Serializable
import java.io.File
import java.io.FileReader
import java.io.Reader
import scala.util.matching.Regex
import scala.io.Source
import scala.xml.Elem
import scala.xml.factory.XMLLoader
import scala.xml.XML
import scala.xml.pull._
import javax.xml.stream.events._

object MeSHFactory {
  var meshCache : MeSH = null
  val logger = LoggerFactory.getLogger("MeSHFactory")
  def getMeSH (data : String) : MeSH = {
    if (meshCache == null) {
      logger.info ("MeSH Factory cache miss.")
      val mesh = new MeSH (data)
      mesh.init ()
      meshCache = mesh
    } else {
      logger.info ("MeSH Factory cache hit.")
    }
    meshCache
  }
}

case class MeSHVocab (categories : Map[String,Array[String]]);

class MeSH (meshData : String) {

  val disease_mesh_prefix = "C"
  val chemical_mesh_prefix = "D"
  val protein_mesh_prefix = "D12"

  val chemicals = ArrayBuffer.empty[String]
  val proteins = ArrayBuffer.empty[String]
  val diseases = ArrayBuffer.empty[String]

  val logger = LoggerFactory.getLogger("MeSH")

  def init () = {
    var vocab : MeSHVocab = null
    val startTime = Platform.currentTime
    if (meshData.endsWith (".xml")) {
      val xml = new XMLEventReader(Source.fromFile(meshData))
      var inDescriptor = false
      var inDescriptorName = false
      var inElementName = false
      var elementName : String = null
      var inTreeNum = false
      var inPharmaAction = false

      for (event <- xml) {
        event match {

          case EvElemStart(_, "PharmacologicalActionList", _, _) => {
            inPharmaAction = true
          }
          case EvElemEnd(_, "PharmacologicalActionList") => {
            inPharmaAction = false
          }

          case EvElemStart(_, "DescriptorRecord", _, _) => {
            inDescriptor = true
          }
          case EvElemEnd(_, "DescriptorRecord") => {
            inDescriptor = false
            elementName = null
          }

          case EvElemStart (_, "DescriptorName", _, _) => {
            if (! inPharmaAction) {
              inDescriptorName = true
            }
          }
          case EvElemEnd (_, "DescriptorName") => {
            if (! inPharmaAction) {
              inDescriptorName = false
            }
          }

          case EvElemStart (_, "String", _, _) => {
            if (inDescriptorName) {
              inElementName = true
            }
          }
          case EvElemEnd (_, "String") => {
            if (inDescriptorName) {
              inElementName = false
            }
          }
          case EvElemStart (_, "TreeNumber", _, _) => {
            inTreeNum = true
          }
          case EvElemEnd (_, "TreeNumber") => {
            inTreeNum = false
          }
          case EvText(t) => {
            if (inElementName) {
              elementName = t.trim ().toLowerCase ()
            } else if (inTreeNum) {
              if (elementName != null) {
                val treeNumber = t
                if (treeNumber.startsWith (protein_mesh_prefix)) {
                  addElement ("proteins", proteins, elementName)
                } else if (treeNumber.startsWith (disease_mesh_prefix)) {
                  addElement ("diseases", diseases, elementName)
                } else if (treeNumber.startsWith (chemical_mesh_prefix)) {
                  addElement ("chemicals", chemicals, elementName)
                }
              }
            }
          }
          case _ => {
            // println (s" event : ${event.getClass}")
          }
        }
      }
    } else if (meshData.endsWith (".json")) {
      implicit val formats = DefaultFormats
      val source = scala.io.Source.fromFile(meshData)
      val lines = try source.mkString finally source.close()
      val json = parse (lines)
      val vocab = json.extract [MeSHVocab]
      vocab.categories.foreach { keyVal =>
        if (keyVal._1.equals ("chemicals")) {
          keyVal._2.map { item =>
            chemicals += item
          }
        } else if (keyVal._1.equals ("proteins")) {
          keyVal._2.map { item =>
            proteins += item
          }
        } else if (keyVal._1.equals ("diseases")) {
          keyVal._2.map { item =>
            diseases += item
          }
        }
      }
    }
    val endTime = Platform.currentTime
    logger.info (s"""
Loaded ${chemicals.size} chemicals 
       ${proteins.size} proteins and 
       ${diseases.size} diseases in ${(endTime - startTime) / 1000} seconds.""")

  }

  def addElement (typeName : String, buffer : ArrayBuffer[String], elementName : String) = {
    if (! buffer.contains (elementName)) {
      buffer += elementName
      logger.debug (s"adding $elementName of type $typeName")
    }
  }

  def getChemicals () = { chemicals }
  def getProteins () = { proteins }
  def getDiseases () = { diseases }

}
