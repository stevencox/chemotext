import argparse
import glob
import json
import os
import re
import string
import traceback
from chemotext_util import DataLake
from chemotext_util import DataLakeConf
from chemotext_util import Intact
from chemotext_util import KinaseConf
from chemotext_util import KinaseBinary
from chemotext_util import SerializationUtil as SerUtil
from chemotext_util import SparkConf
from chemotext_util import SparkUtil
from chemotext_util import LoggingUtil
from chemotext_util import Vocabulary
from chemotext_util import WordPosition
from pyspark.mllib.feature import Word2Vec
from pyspark.mllib.feature import Word2VecModel
from pyspark.sql import SQLContext

logger = LoggingUtil.init_logging (__file__)

class Ctext(object):
    ''' Chemotext core logic '''
    @staticmethod
    def metalexer (article, A, B):
        As = Ctext.lexer (article, A)
        Bs = Ctext.lexer (article, B)
        return Ctext.make_binaries (article, As, Bs)
    @staticmethod
    def lexer (article, terms):
        logger = LoggingUtil.init_logging (__file__)
        logger.debug ("Parsing @ {0}.json".format (article.fileName))
        result = []
        doc_pos = 0
        para_pos = 0
        sent_pos = 0
        for para in article.paragraphs:
            for sentence in para.sentences:
                sentence = sentence.replace (".", " ")
                for term in terms.value:
                    if not term or len(term) < 3:
                        continue
                    pos = sentence.find (term) if " " in term else sentence.find (" %s " % term)
                    if pos > -1:
                        result.append (WordPosition (word    = term,
                                                     docPos  = doc_pos + pos,
                                                     paraPos = para_pos,
                                                     sentPos = sent_pos))
                sent_pos = sent_pos + 1
                doc_pos = doc_pos + len (sentence)
            para_pos = para_pos + 1
        for r in result:
            logger.info ("word: {0} file: {1}".format (r, article.fileName))
        return result
    @staticmethod
    def make_binaries (article, L, R, threshold=8000):
        logger = LoggingUtil.init_logging (__file__)
        result = []
        for l in L:
            for r in R:
                distance = abs(l.docPos - r.docPos)
                logger.debug ("Distance - {0}".format (distance))
                if distance < threshold:
                    if article.date.find ("--") > -1:
                        article.date = "0-0-9000"
                    binary = KinaseBinary (id = 0,
                                           L = l.word,
                                           R = r.word,
                                           docDist  = abs(l.docPos - r.docPos),
                                           paraDist = abs(l.paraPos - r.paraPos),
                                           sentDist = abs(l.sentPos - r.sentPos),
                                           code = 0,
                                           fact = False,
                                           refs = [],
                                           pmid = article.id,
                                           date = SerUtil.parse_date (article.date),
                                           file_name = article.fileName)
                    logger.info ("Binary: {0}".format (binary))
                    result.append (binary)
        return result

class WordEmbed(object):
    def __init__(self, sc, conf, articles):
        self.sc = sc
        self.conf = conf
        if os.path.exists (conf.w2v_model):
            logger.info ("Load existing word2vec model: {0}".format (self.conf.w2v_model))
            self.model = Word2VecModel.load (self.sc, self.conf.w2v_model)
        else:
            logger.info ("Compute word2vec word embedding model...")
            text = articles.                                    \
                   flatMap (lambda a : a.paragraphs ).          \
                   flatMap (lambda p : p.sentences ).           \
                   map (lambda s : s.replace(".", " ").split (" ") )
            self.model = Word2Vec ().            \
                         setNumPartitions (100). \
                         fit (text)
            self.model.save (self.sc, self.conf.w2v_model)
    def find_syn (self, word, radius=10):
        results = []
        try:
            if not " " in word:
                results = self.model.findSynonyms (word, radius)
        except:
            pass
        return results

class LitCrawl(object):
    ''' Crawl the literature in search of interactions '''
    @staticmethod
    def find_interactions (sc, vocabulary, articles):
        logger.info ("Find kinase-p53 interactions in article text.")
        broadcast_A = sc.broadcast (vocabulary.A.collect ())
        broadcast_B = sc.broadcast (vocabulary.B.collect ())
        return articles.flatMap (lambda a : Ctext.metalexer (a, broadcast_A, broadcast_B) ).cache ()
    @staticmethod
    def find_facts (vocabulary, binaries):
        logger.info ("Join matches from full text with the intact database to find 'facts'.")
        binaries_map = binaries.map (lambda r : ( r.L, r) ) # ( A -> KinaseBinary.L )
        intact_map = vocabulary.ref.flatMap (lambda inter : Intact.map_As(inter, 'uniprotkb:P04637') ) # ( A -> P53Inter )
        return binaries_map.join (intact_map) # ( A -> ( KinaseBinary, P53Inter ) )    
    @staticmethod
    def find_before (pmid_date, facts):
        logger.info ("Join facts with the pmid->date map to find interactions noticed before published discovery.")
        ref_pmid_to_binary = facts.map (lambda r : ( r[1][1].pmid, r[1][0] ) ) # ( intact.REF[pmid] -> KinaseBinary )
        # TEST. Add reference pmids with late dates.
        pmid_date = pmid_date.union (ref_pmid_to_binary.map (lambda r : ( r[0], SerUtil.parse_date ("1-1-2300") )))
        before = ref_pmid_to_binary.                                                \
                 join (pmid_date).                                                  \
                 map (lambda r : r[1][0].copy (ref_date = r[1][1]) ).               \
                 filter (lambda k : k.date and k.ref_date and k.date < k.ref_date). \
                 distinct ()
        return before

def execute (conf, home):
    sc = SparkUtil.get_spark_context (conf.spark_conf)

    data_lake = DataLake (sc, conf.data_lake_conf)
    kin2prot = data_lake.get_kin2prot ()
    articles = data_lake.load_articles ()
    vocabulary = data_lake.load_vocabulary (kin2prot)
    pmid_date = data_lake.load_pmid_date () # ( pmid -> date )

    binaries = LitCrawl.find_interactions (sc, vocabulary, articles)
    facts = LitCrawl.find_facts (vocabulary, binaries)
    before = LitCrawl.find_before (pmid_date, facts)

    for m in before.collect ():
        logger.info ("Before-Ref-Date:> {0}".format (m))

    embed = WordEmbed (sc, conf, articles)
    for w in vocabulary.A.collect ():
        for syn in embed.find_syn (w, radius=800):
            if "kinase" in syn or "p53" in syn:
                print "   --[ {0} ]:syn>> {1}".format (w, syn)
            
def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master",    help="Mesos master host")
    parser.add_argument("--name",      help="Spark framework name")
    parser.add_argument("--input",     help="Data root directory")
    parser.add_argument("--home",      help="App home")
    parser.add_argument("--venv",      help="Path to Python virtual environment to use")
    parser.add_argument("--intact",    help="Path to intact data")
    parser.add_argument("--medline",   help="Path to Medline data")
    parser.add_argument("--mesh",      help="File containing JSON array of MeSH synonyms")
    parser.add_argument("--proqinase", help="Kinase synonyms from Pro Qinase")
    parser.add_argument("--kin2prot",  help="Kinase name to uniprot id mapping")    
    parser.add_argument("--w2v",       help="Word embedding model file")
    args = parser.parse_args()
    conf = KinaseConf (spark_conf         = SparkConf (
                           host           = args.master,
                           venv           = args.venv,
                           framework_name = args.name),
                       data_lake_conf     = DataLakeConf (
                           input_dir      = args.input,
                           intact         = args.intact,
                           medline        = args.medline, 
                           proqinase_syn  = args.proqinase,
                           mesh_syn       = args.mesh,
                           kin2prot       = args.kin2prot),
                       w2v_model          = args.w2v)
    execute (conf, args.home)

if __name__ == "__main__":
    main ()
