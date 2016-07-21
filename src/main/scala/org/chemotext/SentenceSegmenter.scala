package org.chemotext

import java.io.InputStream
import opennlp.tools.sentdetect.SentenceModel
import opennlp.tools.sentdetect.SentenceDetectorME
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  *  Add OpenNLP as a strategy for sentence segmentation.
  */
class SentenceSegmenter {
//  val stream : InputStream = getClass.getResourceAsStream ("/models/en-sent.bin")
  val stream : InputStream = getClass.getResourceAsStream ("/opennlp/en-sent.bin")
  val model = new SentenceModel (stream);
  val sentenceDetector = new SentenceDetectorME(model);

  val logger = LoggerFactory.getLogger("SentenceSegmenter")

  def segment (text : String) : Array[String] = {
    //logger.info (s"------------> $text")
    var result : Array[String] = null
    try {
      result = sentenceDetector.sentDetect (text)
    } catch {
      case e: IndexOutOfBoundsException =>
        logger.info (s"Array index out of bounds processing text: $text")
    }
    result
  }
}
