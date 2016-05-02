package org.chemotext

import scala.collection.mutable.ListBuffer

trait Lexer {

  val position = Position ()

  def update (documentPosition : Int, sentencePosition : Int, paragraphPosition : Int) = {
//    position.document += documentPosition
//    position.sentence += sentencePosition
    position.paragraph += paragraphPosition
  }

  def findTokens (
    sentence : String,
    features : ListBuffer[WordFeature]) : Unit

}
