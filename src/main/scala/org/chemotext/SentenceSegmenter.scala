package org.chemotext

import java.io.InputStream
import opennlp.tools.sentdetect.SentenceModel
import opennlp.tools.sentdetect.SentenceDetectorME

/**
  *  Add OpenNLP as a strategy for sentence segmentation.
  */
class SentenceSegmenter {
  val stream : InputStream = getClass.getResourceAsStream ("/models/en-sent.bin")
  val model = new SentenceModel (stream);
  val sentenceDetector = new SentenceDetectorME(model);

  def segment (text : String) = {
    sentenceDetector.sentDetect (text)
  }
}
