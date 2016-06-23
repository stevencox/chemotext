package org.chemotext


import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.util.control.Breaks._

class MeshLexer (dataHome : String, meshXML : String) extends Lexer {

  val logger = LoggerFactory.getLogger (getClass.getName) //"MeSHLexer")
  val vocab = VocabFactory.getVocabulary (dataHome, meshXML)

  var words : List[String] = null

  def findTokens (
    sentence : String,
    features : ListBuffer[WordFeature]) =
  {
    val tokens = sentence.split (" ").map (_.trim ().replaceAll ("[\\./]", ""))

    var index = 0
    words.foreach { word =>
      breakable {

        var pos = -1
	while ({ pos = sentence.indexOf (word, pos + 1); pos } > -1) {

          // Need to better handle:
          //  1. Exact matches. Make sure p53 does not match p53xyz
          //  2. Multi word matches wrt #1.

          val textPos = position.text + pos
          features.add (new WordFeature (
            word    = word,
            docPos  = textPos, //position.document + textPos,
            paraPos = position.paragraph,
            sentPos = position.sentence ))

          logger.debug (
            s"** adding word:$word dpos:${position.document} tpos:$textPos " +
              s" ppos:${position.paragraph} spos:${position.sentence}")
	}

        /*


        val token = s" $word "
        var pos = -1
	while ({ pos = sentence.indexOf (token, pos + 1); pos } > -1) {
          val textPos = position.text + pos + 1
          features.add (new WordFeature (
            word    = word,
            docPos  = textPos, //position.document + textPos,
            paraPos = position.paragraph,
            sentPos = position.sentence ))

          logger.debug (
            s"** adding word:$word dpos:${position.document} tpos:$textPos token:$token " +
              s" ppos:${position.paragraph} spos:${position.sentence}")
	}


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
         */
      }
    }
    position.sentence += 1
    position.text += sentence.length ()
  }
}

class AMeshLexer (dataHome : String, meshXML : String) extends MeshLexer (dataHome, meshXML) { words = vocab.A }

class BMeshLexer (dataHome : String, meshXML : String) extends MeshLexer (dataHome, meshXML) { words = vocab.B }

class CMeshLexer (dataHome : String, meshXML : String) extends MeshLexer (dataHome, meshXML) { words = vocab.C }
