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

import org.json4s._
import org.json4s.JsonAST._
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

import scala.collection.mutable.ListBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.compat.Platform

object JSONUtils {

  val logger = LoggerFactory.getLogger("JSONUtils")

  def readJSON (jsonPath : String) = {
    var json : JValue = null
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
        json = parse (data.toString())
      } catch {
        case e: IOException =>
          logger.error (s"Error reading json $e")
          in.close ()
      } finally {
        in.close ()
      }
      val endTime = Platform.currentTime
      logger.info (s"""Loaded json in ${(endTime - startTime) / 1000} seconds.""")
    }
    json
  }

  def writeJSON [A <: AnyRef: Manifest](obj : A, jsonPath : String) = {
    val startTime = Platform.currentTime
    implicit val formats = Serialization.formats(NoTypeHints)
    val serialized = write (obj)
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
    logger.info (s"Wrote json in ${(endTime - startTime) / 1000} seconds.")
  }
}
