package org.chemotext


import scala.reflect._
//import scala.reflect.runtime.universe._
import reflect.runtime.universe._
import reflect.runtime.universe.SymbolTag
import reflect.ClassTag

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
//import org.json4s.jackson.JsonMethods._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{ read, write, writePretty}
import scala.collection.mutable.ListBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.compat.Platform


object JSONUtils {

  val logger = LoggerFactory.getLogger("JSONUtils")

/*
  def test[A](implicit ev: TypeTag[A]) = {
    // typeTag to classTag
    implicit val cl = ClassTag[A]( ev.mirror.runtimeClass( ev.tpe ) )

    // with an implicit classTag in scope, you can get a manifest
    manifest[A]
  }

  def readJSON2 [A : TypeTag](jsonPath : String) : A = {
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

    implicit val formats = DefaultFormats
    json.extract [test[A]]
  }
 */

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
    var out : PrintWriter = null
    try {
      out = new PrintWriter(new BufferedWriter (new FileWriter (jsonPath)))
      out.println (writePretty(obj)) //write(obj)))
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

/*
  def toManifest[T:TypeTag]: Manifest[T] = {
    val t = typeTag[T]
    val mirror = t.mirror
    def toManifestRec(t: Type): Manifest[_] = {
      val clazz = ClassTag[T](mirror.runtimeClass(t)).runtimeClass
      if (t.args.length == 1) {
        val arg = toManifestRec(t.args.head)
        ManifestFactory.classType(clazz, arg)
      } else if (t.args.length > 1) {
        val args = t.args.map(x => toManifestRec(x))
        ManifestFactory.classType(clazz, args.head, args.tail: _*)
      } else {
        ManifestFactory.classType(clazz)
      }
    }
    toManifestRec(t.tpe).asInstanceOf[Manifest[T]]
  }
 */
}
