package org.chemotext


import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.util.control.Breaks._

class MeshLexer (meshXML : String) extends Lexer {

  val logger = LoggerFactory.getLogger ("Lexer")
  val vocab = VocabFactory.getVocabulary (meshXML)

  var words : List[String] = null

  def findTokens (
    sentence : String,
    features : ListBuffer[WordFeature]) =
  {
    val tokens = sentence.split (" ").map (_.trim ().replaceAll ("[\\./]", ""))
    words.foreach { word =>
      breakable {
        tokens.foreach { token =>
          if (token.equals (word)) {

            val textPos = position.text + sentence.indexOf (token)
            features.add (new WordFeature (
              word    = word,
              docPos  = textPos, //position.document + textPos,
              paraPos = position.paragraph,
              sentPos = position.sentence ))

            logger.debug (
              s"** adding word:$word dpos:${position.document} tpos:$textPos token:$token " +
                s" ppos:${position.paragraph} spos:${position.sentence}")

            break
          }
        }
      }
    }
    position.sentence += 1
    position.text += sentence.length ()
  }
}

class AMeshLexer (meshXML : String) extends MeshLexer (meshXML) { words = vocab.A }

class BMeshLexer (meshXML : String) extends MeshLexer (meshXML) { words = vocab.B }

class CMeshLexer (meshXML : String) extends MeshLexer (meshXML) { words = vocab.C }
