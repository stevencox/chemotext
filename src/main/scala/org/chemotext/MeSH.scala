package org.chemotext

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.File
import java.io.FileReader
import java.io.FileWriter
import java.io.IOException
import java.io.PrintWriter
import java.io.Reader
import java.io.Serializable
import java.nio.file.{Paths, Files}
import javax.xml.stream.events._
import org.json4s._
import org.json4s.jackson.JsonMethods._

//import org.json4s.native.Serialization
//import org.json4s.native.Serialization.{read, write}

import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.compat.Platform
import scala.io.Source
import scala.util.matching.Regex
import scala.xml.Elem
import scala.xml.PrettyPrinter
import scala.xml.XML
import scala.xml.factory.XMLLoader
import scala.xml.pull._

case class MeSHVocab (categories : List[ (String, Array[String]) ]);

object MeSHFactory {

  var meshCache : MeSH = null
  val logger = LoggerFactory.getLogger("MeSHFactory")

  def getMeSH (data : String) : MeSH = {
    if (meshCache == null) {
      logger.info ("MeSH Factory cache miss.")

      // Read JSON
      val jsonPath = "mesh.json"
      meshCache = readJSON (jsonPath)
      if (meshCache == null) {

        // Parse XML
        val mesh = new MeSH (data)
        mesh.init ()
        meshCache = mesh

        // Write JSON
        writeJSON (mesh, jsonPath)
      }
    }
    meshCache
  }

  def readJSON (jsonPath : String) = {
    var mesh : MeSH = null
    var meshVocab : MeSHVocab = null
    val startTime = Platform.currentTime
    if (Files.exists(Paths.get(jsonPath))) {
      var in : BufferedReader = null
      try {
        val data = new StringBuilder ()
        in = new BufferedReader (new FileReader (jsonPath), 4096)
        var line = in.readLine ()
        while (line != null) {
          data.append (line)
          line = in.readLine ()
        }
        implicit val formats = DefaultFormats
        var json = parse (data.toString())
        meshVocab = json.extract [MeSHVocab]
        mesh = new MeSH ()

        logger.info (s"0 => ${meshVocab.categories(0)._1}")
        logger.info (s"1 => ${meshVocab.categories(1)._1}")
        logger.info (s"2 => ${meshVocab.categories(2)._1}")
        mesh.chemicals = meshVocab.categories(0)._2.to[ArrayBuffer]
        mesh.proteins = meshVocab.categories(1)._2.to[ArrayBuffer]
        mesh.diseases = meshVocab.categories(2)._2.to[ArrayBuffer]
      } catch {
        case e: IOException =>
          logger.error (s"Error reading json $e")
          in.close ()
      } finally {
        in.close ()
      }
    }

    val endTime = Platform.currentTime
    logger.info (s"""
Loaded ${mesh.chemicals.size} chemicals 
       ${mesh.proteins.size} proteins and 
       ${mesh.diseases.size} diseases in ${(endTime - startTime) / 1000} seconds.""")

    mesh
  }

  def writeJSON (mesh : MeSH, jsonPath : String) = {
    // Write JSON
    val startTime = Platform.currentTime
    implicit val formats = Serialization.formats(NoTypeHints)
    val meshVocab = new MeSHVocab (List (
      ( "chemicals", mesh.chemicals.toArray ),
      ( "proteins",  mesh.proteins.toArray ),
      ( "diseases",  mesh.diseases.toArray )
    ))
    val serialized = write (meshVocab)
    var out : PrintWriter = null
    try {
      out = new PrintWriter(new BufferedWriter (new FileWriter (jsonPath)))
      out.println (serialized)
      out.flush ()
    } catch {
      case e : IOException =>
        logger.error (s"Error writing json mesh vocab: $e")
        out.close ()
    } finally {
      out.close ()
    }

    val endTime = Platform.currentTime
    logger.info (s"""
Wrote ${mesh.chemicals.size} chemicals 
      ${mesh.proteins.size} proteins and 
      ${mesh.diseases.size} diseases in ${(endTime - startTime) / 1000} seconds.""")

  }

}

class MeSH (meshData : String = "") {

  val disease_mesh_prefix = "C"
  val chemical_mesh_prefix = "D"
  val protein_mesh_prefix = "D12"

  var chemicals = ArrayBuffer.empty[String]
  var proteins = ArrayBuffer.empty[String]
  var diseases = ArrayBuffer.empty[String]

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
