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

case class TmChemLexerConf (
  configPath     : String,
  cacheFileName  : String,
  dictionaryPath : String)

class TmChemLexer (conf : TmChemLexerConf) extends Lexer {

  val logger = LoggerFactory.getLogger("TmChemLexer[Banner]")

  logger.debug (s"Loading lexer config from ${conf.configPath}.")
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
      logger.debug (s"Mention [${mention.getText()}] at pos: [${mention.getStart()}]")
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

  def findTokens (
    sentence : String,
    features : ListBuffer[WordFeature]) : Unit =
  {
    val capitalSentence = sentence.capitalize.trim ()
    try {
      val bannerSentences = List (new Sentence ("", "", capitalSentence))
      val sentences = normalize (bannerSentences)
      sentences.foreach { normalizedSentence =>
        val mentions = normalizedSentence.getMentions ()
        mentions.foreach { mention =>
          val word = mention.getText ().toLowerCase ()
          // If the word has been altered (normalized) by banner, then looking for it by index
          // literally will be not quite right. But in the greater scheme, it may be the best we
          // can do since mentinon.getStart() doesn't do quite what we want.
          val textPos = position.text + sentence.indexOf (word) //mention.getStart ()
	  
 	  if (word.split(" ").length < 3) {
             features.add (new WordFeature ( 
                word    = word,
                docPos  = textPos, //position.document + textPos,
                paraPos = position.paragraph,
                sentPos = position.sentence))

             val p = position
             logger.debug (s"** adding word:$word dpos:${p.document} tpos:$textPos ppos:${p.paragraph} spos:${p.sentence}")
	  }
        }
        position.sentence += 1
        position.text += sentence.length ()
      }
    } catch {
      case e: Exception =>
        logger.error (s"Error normalizing [$capitalSentence]")
    }
  }

}

object TmChemLexerApp {
  val logger = LoggerFactory.getLogger ("TmChemLexerApp")
  def main (args : Array[String]) = {
    val lexerConfigPath    = args (0)
    val lexerCacheFile     = args (1)
    val lexerDictPath      = args (2)
    logger.info (s"lexerConfigPath    : $lexerConfigPath")
    logger.info (s"lexerCacheFile     : $lexerCacheFile")
    logger.info (s"lexerDictPath      : $lexerDictPath")
    val lexerConf = TmChemLexerConf (
        configPath     = lexerConfigPath,
        cacheFileName  = lexerCacheFile,
        dictionaryPath = lexerDictPath
    )
    val sentence = args (3)
    //"this is a sentence with the words acetaminophen and 2-[(hydroxyimino)methyl]-1-methylpyridin-1-ium chloride"

    val lexer = new TmChemLexer (lexerConf)
    val normalized = lexer.normalize (sentence)
    logger.debug (s"Original: $sentence")
    logger.debug (s"Normalized: $normalized")

    lexer.findTokens (
      sentence      = sentence,
      features      = new ListBuffer[WordFeature] ())

  }

}
