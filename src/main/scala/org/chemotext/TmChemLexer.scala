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
import scala.util.matching.Regex

case class TmChemLexerConf (
  configPath     : String,
  cacheFileName  : String,
  dictionaryPath : String)

object TmChemLexer {
  val logger = LoggerFactory.getLogger("TmChemLexer(Obj)[Banner]")
  var threadLocal = new ThreadLocal[TmChemLexer] ()
  def getInstance (conf : TmChemLexerConf) = {
    var instance = threadLocal.get ()
    if (instance == null) {
      instance = new TmChemLexer (conf)
      instance.init ()
      threadLocal.set (instance)
    }
    instance
  }

/*
  var instance : TmChemLexer = null
  def getInstance (conf : TmChemLexerConf) = {
    if (instance == null) {
      instance = new TmChemLexer (conf)
    }
    instance
 */

  def invalidate () = {
    logger.info ("Invalidating TmChemLexer object *********************************************************************")
    threadLocal.set (null) //instance = null
  }
}

class TmChemLexer (conf : TmChemLexerConf) extends Lexer {

  val logger = LoggerFactory.getLogger("TmChemLexer[Banner]")

  System.setOut (new BannerFilterPrintStream (System.out))

  logger.debug (s"Loading lexer config from ${conf.configPath}.")
  val config = new XMLConfiguration (conf.configPath)
  val localConfig = config.configurationAt(classOf[BANNER].getPackage().getName())
  val modelFileName = localConfig.getString("modelFilename")

  /*
  logger.debug (s"Creating tagger with modelFileName=$modelFileName")
  val modelFileStream = new File(modelFileName)
  val lemmatiser = BANNER.getLemmatiser (config)
  val posTagger = BANNER.getPosTagger (config)
  val cacheFile = Collections.singletonList(conf.cacheFileName)
  var tagger = CRFTagger.load (modelFileStream, lemmatiser, posTagger, cacheFile)
  val dataSet = BANNER.getDataset (config)

  logger.debug (s"Creating tokenizer")
  val tokenizer = BANNER.getTokenizer(config)
  val dictionary = loadDictionary (conf.dictionaryPath)
   */
  logger.debug (s"Creating tagger with modelFileName=$modelFileName")
  var modelFileStream = new File(modelFileName)
  var lemmatiser = BANNER.getLemmatiser (config)
  var posTagger = BANNER.getPosTagger (config)
  var cacheFile = Collections.singletonList(conf.cacheFileName)
  var tagger = CRFTagger.load (modelFileStream, lemmatiser, posTagger, cacheFile)
  var dataSet = BANNER.getDataset (config)

  logger.debug (s"Creating tokenizer")
  var tokenizer = BANNER.getTokenizer(config)
  var dictionary = loadDictionary (conf.dictionaryPath)

  def init () = {
    logger.info (">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> RESETTING LEXER <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    //tagger = CRFTagger.load (modelFileStream, lemmatiser, posTagger, cacheFile)

    logger.debug (s"Creating tagger with modelFileName=$modelFileName")
    modelFileStream = new File(modelFileName)
    lemmatiser = BANNER.getLemmatiser (config)
    posTagger = BANNER.getPosTagger (config)
    cacheFile = Collections.singletonList(conf.cacheFileName)
    tagger = CRFTagger.load (modelFileStream, lemmatiser, posTagger, cacheFile)
    dataSet = BANNER.getDataset (config)

    logger.debug (s"Creating tokenizer")
    tokenizer = BANNER.getTokenizer(config)
    dictionary = loadDictionary (conf.dictionaryPath)
  }



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
      case e: Exception =>
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

  /*
   16/06/25 12:54:38 INFO TmChemLexer[Banner]: ----------> word: monoclinic,, index: 0, sent: [monoclinic,]
   16/06/25 12:54:38 INFO TmChemLexer[Banner]: ----------> word: = 118.250, index: 0, sent: [= 118.250]
   16/06/25 12:54:38 INFO TmChemLexer[Banner]: ----------> word: 7179 reflections, index: 0, sent: [7179 reflections]
   16/06/25 12:54:38 INFO TmChemLexer[Banner]: ----------> word: (10) 3, index: 0, sent: [(10) 3]
   16/06/25 12:54:38 INFO TmChemLexer[Banner]: ----------> word: 301 parameters, index: 0, sent: [301 parameters]
   16/06/25 12:54:38 INFO TmChemLexer[Banner]: ----------> word: 33 restraints, index: 0, sent: [33 restraints]
   also...
   (4)
   n = 4
   = 3
   */
  // (3) word
  // (4)
  //val listNumberPattern = new Regex ("\\(\\d+\\)( \\w+)?")
  //val numberWordPattern = new Regex ("\\d+ \\w+")
  //val decimalNumber = new Regex ("\d+(\.\d)?")
  // = 3
  // x = 4
  // 
  val rejectPattern = new Regex ("^\\(\\d+\\)( \\w+)?$|^\\d+(\\.\\d)$?|^= .*$|^\\w+ = \\d$|^.*doi:.*$")
  /*
16/06/25 14:04:28 INFO TmChemLexer[Banner]: REJECTED: bis(3-pyridyl) porphyrin 1
16/06/25 14:04:28 INFO TmChemLexer[Banner]: REJECTED: pyridyl cholate 3
16/06/25 14:04:28 INFO TmChemLexer[Banner]: REJECTED: biotinylated pyridyl cholate 4
16/06/25 14:04:29 INFO TmChemLexer[Banner]: REJECTED: 53.84)tri-nucleotide101
16/06/25 14:04:29 INFO TmChemLexer[Banner]: REJECTED: tetra-nucleotide18 (4.42)20 (6.2)penta-nucleotide4
16/06/25 14:04:30 INFO TmChemLexer[Banner]: REJECTED: methylated dinucleotides (5-cpg-3)
16/06/25 14:04:31 INFO TmChemLexer[Banner]: REJECTED: 6-hydroxyl kaempferol 3-0 arabinoglucoside
   */
  var rejectedCount = 0
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
          // can do since mentinon.getStart() doesn't do quite what we want either.

          val index = sentence.indexOf (word)
          val textPos = position.text + index //mention.getStart ()
	  
          // This ought to slow things down :(
/*
          val valid = (
//            word.split (" ").length < 5 &&
              rejectPattern.findAllIn(word).length == 0 &&
//              ! (word.contains (" =") || word.contains ("= ")) &&
//              ! ( word.startsWith ("(") && word.endsWith (")") && word.length < 5 ) &&
              ! ( word.indexOf ("doi: ") > -1 )
          )
 	  if (valid) { //word.split(" ").length < 3) {
 */
          if (rejectPattern.findAllIn (word).length == 0) {
            //logger.info (s"----------> word: $word, index: $index, sent: [$sentence]")

            features.add (new WordFeature (
              word    = word,
              docPos  = textPos, //position.document + textPos,
              paraPos = position.paragraph,
              sentPos = position.sentence))

            val p = position
            logger.debug (s"** adding word:$word dpos:${p.document} tpos:$textPos ppos:${p.paragraph} spos:${p.sentence}")
            rejectedCount = 0
	  } else {
            logger.info (s"REJECTED: $word")
            rejectedCount += 1
            if (rejectedCount > 5) {
              init ()
              rejectedCount = 0
            }
          }
        }
        position.sentence += 1
        position.text += sentence.length ()
      }
    } catch {
      case e: Exception =>
        e.printStackTrace (java.lang.System.out)
        init ()
        rejectedCount = 0
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


/*
16/06/27 14:24:29 INFO TmChemLexer[Banner]: ----------> word: fullerenes, index: 58, sent: [nfas with lower electron affinities
than the archetypical fullerenes have successfully been used in conjunction
with wide bandgap donors to enhance the open-circuit voltage.]
16/06/27 14:24:29 INFO TmChemLexer[Banner]: REJECTED: (604397)
16/06/27 14:24:29 INFO TmChemLexer[Banner]: ----------> word: poly[4,8-bis(5-(2-ethylhexyl)thiophen-2-yl)benzo[1,2-b;4,5-b']dithiophene-2,6-diyl-alt-(4-(2-ethylhexanoyl)-thieno[3,4-b]thiophene-)-2-6-diyl), index: 0, sent: [poly[4,8-bis(5-(2-ethylhexyl)thiophen-2-yl)benzo[1,2-b;4,5-b']dithiophene-2,6-diyl-alt-(4-(2-ethylhexanoyl)-thieno[3,4-b]thiophene-)-2-6-diyl)]]
16/06/27 14:24:29 INFO TmChemLexer[Banner]: ----------> word: poly[4,8-bis(5-(2-ethylhexyl)thiophen-2-yl)benzo[1,2-b;4,5-b]dithiophene-2,6- 157 diyl-alt-(4-(2-ethylhexyl)-3-fluorothieno[3,4-b]thiophene-)-2-
158 carboxylate-26-diyl), index: 0, sent: [poly[4,8-bis(5-(2-ethylhexyl)thiophen-2-yl)benzo[1,2-b;4,5-b]dithiophene-2,6- 157 diyl-alt-(4-(2-ethylhexyl)-3-fluorothieno[3,4-b]thiophene-)-2-
158 carboxylate-26-diyl)]]
16/06/27 14:24:29 INFO TmChemLexer[Banner]: ----------> word: poly[dithieno[2,3-d:2,3-d]benzo[1,2-b:4,5-b]dithiophene-co-1,3-bis(thiophen-2-yl)-benzo-[1,2-c:4,5-c]dithiophene-4,8-dione, index: 0, sent: [poly[dithieno[2,3-d:2,3-d]benzo[1,2-b:4,5-b]dithiophene-co-1,3-bis(thiophen-2-yl)-benzo-[1,2-c:4,5-c]dithiophene-4,8-dione]]
16/06/27 14:24:29 INFO TmChemLexer[Banner]: REJECTED: poly[4,8-bis(5-(2-
171 ethylhexyl)thiophen-2-yl)benzo[1,2-b;4,5-b]dithiophene-2,6- 172 diyl-alt-(4-(2-ethylhexyl)-3-fluorothieno[3,4-b]thiophene)-2-
173 carboxylate-2,6-diyl]
16/06/27 14:24:29 INFO TmChemLexer[Banner]: ----------> word: poly[(5,6-difluoro-2,1,3-benzothiadiazol-4,7-diyl)-alt-(3,3'''-di(2-decyltetradecyl)-2,2';5',2'';5'',2'''-quaterthiophen-5,5'''-diyl)], index: 0, sent: [poly[(5,6-difluoro-2,1,3-benzothiadiazol-4,7-diyl)-alt-(3,3'''-di(2-decyltetradecyl)-2,2';5',2'';5'',2'''-quaterthiophen-5,5'''-diyl)].]
16/06/27 14:24:29 INFO TmChemLexer[Banner]: ----------> word: poly[n-(2-hexyldodecyl)-2,2-bithiophene-3,3-dicarboximide-alt-5,5-(2,5-bis(3-decylthiophen-2-yl)-thiophene), index: 0, sent: [poly[n-(2-hexyldodecyl)-2,2-bithiophene-3,3-dicarboximide-alt-5,5-(2,5-bis(3-decylthiophen-2-yl)-thiophene)].]
16/06/27 14:24:29 INFO TmChemLexer[Banner]: ----------> word: poly[(4,4'-bis(3-(2-ethylhexyl)dithieno[3,2-b:2,3-d]silole)-2,6-diyl-alt-(2,5-bis(3-(2-ethylhexyl)thiophen-2yl)thiazolo[5,4-d]thiazole, index: 0, sent: [poly[(4,4'-bis(3-(2-ethylhexyl)dithieno[3,2-b:2,3-d]silole)-2,6-diyl-alt-(2,5-bis(3-(2-ethylhexyl)thiophen-2yl)thiazolo[5,4-d]thiazole)].]
16/06/27 14:24:29 INFO TmChemLexer[Banner]: ----------> word: poly[2-methoxy-5-(2-ethylhexyloxy)-1,4-phenylenevinylene], index: 0, sent: [poly[2-methoxy-5-(2-ethylhexyloxy)-1,4-phenylenevinylene].]
16/06/27 14:24:29 INFO TmChemLexer[Banner]: ----------> word: poly[(4,8-bis-(2-ethylhexyloxy)-benzo(1,2-b:4,5-b)dithiophene)-2,6-diyl-alt-(4-(2-ethylhexyl)-3-fluorothieno[3,4-b]thiophene-)-2-carboxylate-2-6-diyl)], index: 0, sent: [poly[(4,8-bis-(2-ethylhexyloxy)-benzo(1,2-b:4,5-b)dithiophene)-2,6-diyl-alt-(4-(2-ethylhexyl)-3-fluorothieno[3,4-b]thiophene-)-2-carboxylate-2-6-diyl)].]
16/06/27 14:24:29 INFO Chemotext2: @-article: /Users/scox/dev/starshome/var/chemotext/pubmed/articles/Accid_Anal_Prev/Accid_Anal_Prev_2011_May_43(3)_1062-1067.fxml
[Stage 6:>                                                          (0 + 2) / 6]16/06/27 14:24:36 INFO TmChemLexer[Banner]: ----------> word: remazol, index: 43, sent: [in the present study, biotransformation of remazol orange 3r (ro3r) was studied using well-known bacterial isolate pseudomonas aeruginosa strain bch.]
16/06/27 14:24:36 INFO TmChemLexer[Banner]: ----------> word: veratryl alcohol, index: 21, sent: [laccase, tyrosinase, veratryl alcohol oxidase and dcip reductase were observed in the cells obtained after decolorization of ro3r, which supports their role in decolorization.]
16/06/27 14:24:36 INFO TmChemLexer[Banner]: REJECTED: n-(7 amino 8 hydroxy-napthalen-2yl) actamide

 */
