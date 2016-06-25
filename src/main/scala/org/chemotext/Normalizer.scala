package org.chemotext

import banner.eval.BANNER
import banner.eval.BANNER.MatchCriteria
import banner.eval.BANNER.Performance
import banner.eval.dataset.Dataset
import banner.postprocessing.PostProcessor
import banner.tagging.CRFTagger
import banner.tokenization.Tokenizer
import banner.types.EntityType
import banner.types.Mention
import banner.types.Mention.MentionType
import banner.types.Sentence
import banner.types.SentenceWithOffset
import banner.util.SentenceBreaker
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.util.ArrayList;
import java.util.Collections
import ncbi.OffsetTaggedPipe
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

case class NormalizerConf (
  configPath     : String,
  cacheFileName  : String,
  dictionaryPath : String)

class Normalizer (conf : NormalizerConf) {

  val logger = LoggerFactory.getLogger("BannerChem:Normalizer")

  logger.debug (s"Loading normalizer config from ${conf.configPath}.")
  val config = new XMLConfiguration (conf.configPath)
  val localConfig = config.configurationAt(classOf[BANNER].getPackage().getName())
  val modelFileName = localConfig.getString("modelFilename")

  logger.debug (s"Creating tagger with modelFileName=$modelFileName")
  val modelFileStream = new File(modelFileName)
  val lemmatiser = BANNER.getLemmatiser (config)
  val posTagger = BANNER.getPosTagger (config)
  val cacheFile = Collections.singletonList(conf.cacheFileName)
  val tagger = CRFTagger.load (modelFileStream, lemmatiser, posTagger, cacheFile)
  val dataSet = BANNER.getDataset (config)

  logger.debug (s"Creating tokenizer")
  val tokenizer = BANNER.getTokenizer(config)
  val dictionary = loadDictionary (conf.dictionaryPath)

  def changeType (sentences : List[Sentence]) = {
    sentences.foreach { sentence =>
      val mentions = sentence.getMentions ()
      mentions.foreach { mention =>
	mention.setEntityType (EntityType.getType("CHEMICAL"))
      }
    }
  }

  def getText (sentences : List[Sentence]) : List[String] = {
    val buffer = new ListBuffer[String] ()
    sentences.foreach { sentence =>
      buffer += getText (sentence)
    }
    buffer.toList
  }
  def getText (sentence : Sentence) : String = {
    val buffer = new ListBuffer[String] ()
    sentence.getMentions ().foreach { mention =>
      buffer += mention.getText ()
    }
    buffer.toList.mkString (" ")
  }
  def normalize (text : String) : String = {
    val bannerSentence = new Sentence ("", "", text)
    val normalizedSentences : List[Sentence] = normalize (List(bannerSentence))
    val normalizedText : List[String] = getText (normalizedSentences)
    normalizedText.mkString (" ")
   }

  def normalize (sentences : List[Sentence]) : List[Sentence] = {
    var result : List[Sentence] = null
    try {
      val processedSentences = ListBuffer[Sentence] ()
      changeType (sentences)
      sentences.foreach { sentence =>
        val sentenceCopy : Sentence = sentence.copy (false, false)
        tokenizer.tokenize (sentenceCopy)
        tagger.tag (sentenceCopy)
        sentenceCopy match {
          case sentenceWithOffset : SentenceWithOffset =>
            val mentions = sentenceWithOffset.getMentions ()
            mentions.foreach { mention =>
              val mentionText = mention.getText ()
	      val processedText = mentionText.replaceAll ("[^A-Za-z0-9]", "")
	      var conceptId = dictionary.get (processedText)
              mention.setConceptId (conceptId.getOrElse ("-1"))
            }
          case _ =>
        }
        processedSentences.add (sentenceCopy)
      }
      result = processedSentences.toList
    } catch {
      case e: ConfigurationException =>
        e.printStackTrace ()
        logger.error (s"Unable to normalize string")
      case e: java.io.StreamCorruptedException =>
        e.printStackTrace ()
    }
    result
  }

  def loadDictionary (fileName : String) : Map[String, String] = {
    val dict = new HashMap[String,String] ()
    var reader : BufferedReader = null
    try {
      reader = new BufferedReader (new FileReader (fileName))
      var line = reader.readLine()
      while (line != null) {
	val fields = line.split("\t")
	val text = fields(0)
	val conceptId = fields(1)
	dict.put(text, conceptId)
	line = reader.readLine()
      }
    } finally {
      if (reader != null) {
	reader.close()
      }
    }
    dict.toMap
  }
}

object NormalizerApp {
  val logger = LoggerFactory.getLogger ("NormalizerApp")
  def main (args : Array[String]) = {
    val normalizerConfigPath    = args (0)
    val normalizerCacheFile     = args (1)
    val normalizerDictPath      = args (2)
    logger.info (s"normalizerConfigPath    : $normalizerConfigPath")
    logger.info (s"normalizerCacheFile     : $normalizerCacheFile")
    logger.info (s"normalizerDictPath      : $normalizerDictPath")
    val normalizerConf = NormalizerConf (
        configPath     = normalizerConfigPath,
        cacheFileName  = normalizerCacheFile,
        dictionaryPath = normalizerDictPath
    )

    val normalizer = new Normalizer (normalizerConf)
    val sentence = "this is a sentence with the words acetaminophen and 2-[(hydroxyimino)methyl]-1-methylpyridin-1-ium chloride"
    val normalized = normalizer.normalize (sentence)
    logger.debug (s"Original: $sentence")
    logger.debug (s"Normalized: $normalized")

  }
}
