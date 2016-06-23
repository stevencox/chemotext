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
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}
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

case class Vocabulary (
  A          : List[String],
  B          : List[String],
  C          : List[String],
  extended   : Boolean = false);

object VocabFactory {

  var cache : Vocabulary = null
  val defaultJsonPath = "vocabulary.json"
  val logger = LoggerFactory.getLogger ("VocabFactory")

  def getVocabulary (dataHome : String, data : String) : Vocabulary = {

    val jsonPath = Paths.get (dataHome, defaultJsonPath).toFile().getCanonicalPath ()
    if (cache == null) {

      // Read JSON
      cache = readJSON (jsonPath) //defaultJsonPath)
      if (cache == null) {

        logger.info ("Vocabulary cache miss. Generating vocabulary from sources.")

        // Parse XML
        val mesh = new MeSH (data)
        mesh.init ()
        cache = new Vocabulary (mesh.A.toList, mesh.B.toList, mesh.C.toList)

        // Write JSON
        writeJSON (cache, jsonPath) //defaultJsonPath)
      }
    }
    cache
  }

  def readJSON (jsonPath : String) = {
    var mesh : MeSH = null
    var vocabulary : Vocabulary = null
    var json = JSONUtils.readJSON (jsonPath)
    if (json != null) {
      implicit val formats = DefaultFormats
      vocabulary = json.extract [Vocabulary]
      mesh = new MeSH ()
      //logger.debug (s"vocab.A => ${vocabulary.A}")
      //logger.debug (s"vocab.B => ${vocabulary.B}")
      //logger.debug (s"vocab.C => ${vocabulary.C}")
    }
    vocabulary
  }

  def writeJSON (dataHome : String, vocab : Vocabulary) : Unit = {
    val jsonPath = Paths.get (dataHome, defaultJsonPath).toFile().getCanonicalPath ()
    writeJSON (vocab, jsonPath) //defaultJsonPath)
  }

  def writeJSON (vocab : Vocabulary, jsonPath : String) : Unit = {
    val startTime = Platform.currentTime
    implicit val formats = Serialization.formats(NoTypeHints)
    JSONUtils.writeJSON (vocab, jsonPath)
  }

}

class MeSH (meshData : String = "") {

  val disease_mesh_prefix = "C"
  val chemical_mesh_prefix = "D"
  val protein_mesh_prefix = "D12"

  var A = ArrayBuffer.empty[String]
  var B = ArrayBuffer.empty[String]
  var C = ArrayBuffer.empty[String]
  val map = HashMap.empty[String, String]

  val logger = LoggerFactory.getLogger("MeSH Vocab")

  def init () = {
    val startTime = Platform.currentTime
    if (meshData.endsWith (".xml")) {
      val xml = new XMLEventReader(Source.fromFile(meshData))
      var inDescriptor = false
      var inDescriptorName = false
      var inElementName = false
      var elementName : String = null
      var inTreeNum = false
      var inPharmaAction = false
      var inDescriptorUI = false
      var descriptorUI : String = null

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
            descriptorUI = null
          }
          case EvElemEnd(_, "DescriptorRecord") => {
            inDescriptor = false
            elementName = null
          }

          case EvElemStart(_, "DescriptorUI", _, _) => {
            inDescriptorUI = true
          }
          case EvElemEnd(_, "DescriptorUI") => {
            inDescriptorUI = false
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
              if (descriptorUI != null) {
                map += ( descriptorUI -> elementName)
              }
            } else if (inDescriptorUI) {
              descriptorUI = t.trim()
            } else if (inTreeNum && elementName != null) {
              val treeNumber = t
              if (treeNumber.startsWith (protein_mesh_prefix)) {
                addElement ("proteins", B, elementName, treeNumber)
              } else if (treeNumber.startsWith (chemical_mesh_prefix)) {
                addElement ("chemicals", A, elementName, treeNumber)
              } else if (treeNumber.startsWith (disease_mesh_prefix)) {
                addElement ("diseases", C, elementName, treeNumber)
              }
            }
          }
          case _ => {
            // println (s" event : ${event.getClass}")
          }
        }
      }
      // Minimize disease names that occur in the B list by subtracting 
      // the B intersection with C
      B = B.diff (B.intersect (C))
    }
    val endTime = Platform.currentTime
    logger.info (s"""
Loaded ${A.size} chemicals 
       ${B.size} proteins and 
       ${C.size} diseases in ${(endTime - startTime) / 1000} seconds.""")
  }

  def addElement (
    typeName : String,
    buffer : ArrayBuffer[String],
    elementName : String,
    treeNumber : String) =
  {
    if (elementName.equals ("apoptosis")) {
      logger.info (s"-----------------> APOPTOSIS: $treeNumber")
    }
    if (! buffer.contains (elementName)) {
      buffer += elementName
      logger.debug (s"adding $typeName: $elementName, TreeNumber: $treeNumber")
    }
  }

  def generateValidatedABList (ctdAB : String, supplementalDataFile : String) = {
    var in : BufferedReader = null
    try {
      val data = new StringBuilder ()
      in = new BufferedReader (new FileReader (ctdAB), 4096)
      var line = in.readLine ()
      while (line != null) {
        data.append (line)
        line = in.readLine ()
      }
    } catch {
      case e: IOException =>
        logger.error (s"Error reading json $e")
        in.close ()
    } finally {
      in.close ()
    }
  }

  def parseSupplementalDataFile (supplementalDataFile : String, outputFile : String) = {
    val startTime = Platform.currentTime
    val xml = new XMLEventReader(Source.fromFile(supplementalDataFile))
    var out : PrintWriter = null
    try {
      out = new PrintWriter(new BufferedWriter (new FileWriter (outputFile)))

      var inDescriptorName = false
      var inString = false
      var inRecordUI = false
      var inSupRecordName = false

      var recordUI : String = null
      var supplementalRecordName : String = null
      var descriptorName : String = null

      for (event <- xml) {
        event match {

          case EvElemStart(_, "SupplementalRecordUI", _, _) => {
            inRecordUI = true
          }
          case EvElemEnd(_, "SupplementalRecordUI") => {
            inRecordUI = false
          }

          case EvElemStart(_, "SupplementalRecordName", _, _) => {
            inSupRecordName = true
          }
          case EvElemEnd(_, "SupplementalRecordName") => {
            inSupRecordName = false
          }

          case EvElemStart(_, "DescriptorName", _, _) => {
            inDescriptorName = true
          }
          case EvElemEnd(_, "DescriptorName") => {
            inDescriptorName = false
          }

          case EvElemStart(_, "String", _, _) => {
            inString = true
          }
          case EvElemEnd(_, "String") => {
            inString = false
          }

          case EvText(text) => {
            if (inRecordUI) {
              recordUI = text.trim ()
            } else if (inSupRecordName) {
              supplementalRecordName = text.trim ().toLowerCase ()
            } else if (inDescriptorName) {
              descriptorName = text.trim ().toLowerCase ()
            }
            out.println (s"descriptorName: $descriptorName ")
          }
        }
      }
    } catch {
      case e : IOException =>
        logger.error (s"Error writing validated A->B list: $e")
        out.close ()
    } finally {
      out.close ()
    }

  }

}
