import argparse
import json
import glob
import os
import re
import string
import traceback
from pyspark.mllib.feature import Word2Vec
from pyspark.mllib.feature import Word2VecModel
from chemotext_util import KinaseConf
from chemotext_util import P53Inter
from chemotext_util import Cache
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil
from chemotext_util import LoggingUtil
from pyspark.sql import SQLContext

logger = LoggingUtil.init_logging (__file__)

def get_article_dirs (articles, app_home):
    logger.info ("Listing directory: {0}".format (articles))
    cache = Cache (app_home)
    dirs = cache.get ('pubmed_dirs.json')
    c = 0
    if dirs is None:
        dirs = []
        for root, dirnames, files in os.walk (articles):
            for d in dirnames:
                dirs.append (os.path.join (root, d))
        cache.put ('pubmed_dirs.json', dirs)
    return dirs

def parse_synonyms (synonyms, separator, result):
    logger = LoggingUtil.init_logging (__file__)
    synonym_pat = re.compile (r".*:([\w ]+)\(.*\)$", re.IGNORECASE)
    synonyms = synonyms.split (separator)
    for opt in synonyms:
        match = synonym_pat.search (opt)
        if match:
            text = match.group(1).lower ()
            result.append (text)
    return result

def get_As (inter, target):
    result = []
    synonyms = inter.alt_B if inter.A == target else inter.alt_A
    return parse_synonyms (synonyms, "|", result)
def get_Bs (inter, target):
    result = []
    synonyms = inter.alt_B if inter.B == target else inter.alt_A
    return parse_synonyms (synonyms, "|", result)

def lexer (article, A, B):
    result = []
    a_match = []
    b_match = []
    loc = 0
    for para in article.paragraphs:
        for sentence in para['sentences']:
            for a in A.value:
                if len(a) > 3:
                    if " " in a:
                        pos = sentence.find (a)
                        if pos > -1:
                            a_match.append ([ a, loc + pos ])
                    else:
                        pos = sentence.find (" %s " % a)
                        if pos > -1:
                            a_match.append ([ a, loc + pos ])
            '''
            words = sentence.split (" ")
            for word in words:
                for a in A.value:
                    if len(a) < 3 and not a in [ 'for' ]: # temporarily avoid spurious stop word and single letter matches
                        continue
                    sentence = sentence.replace (".", " ")
                    if " " in a:
                        pos = sentence.find (a)
                        if pos > -1:
                            a_match.append ([ a, pos ]) 
                    else:
                        pos = sentence.find (" %s " % a)
                        if pos > -1:
                            a_match.append ([ a, pos ])

                    #pos = sentence.find (a)
                    #if pos > -1:
                    #    a_match.append ([ a, pos ])
            '''
            for b in B.value:
                pos = sentence.find (b)
                if pos > -1:
                    b_match.append ([ b, loc + pos ])
            loc = loc + len (sentence)
    for a in a_match:
        for b in b_match:
            result.append ([ a[0], a[1], b[0], b[1], article.id, article.date, article.fileName ])
            #result.append ("{0}-{1}-{2}-{3}-{4}".format (a, b, article.id, article.date, article.fileName))
    return result

def execute (conf, home):
    logger.info ("Get list of article directories")
    dirs = get_article_dirs (conf.input_dir, home)
    sc = SparkUtil.get_spark_context (conf)
    cache = Cache (home)
    article_list = cache.get ('articles')
    if article_list is None or len(article_list) == 0:
        article_list = glob.glob (os.path.join (conf.input_dir, "*.fxml.json"))
        cache.put ('articles', article_list)
        
    logger.info ("Load articles...")
    articles = sc.parallelize (article_list). \
               map (lambda a : SUtil.read_article (a)). \
               cache ()

    logger.info ("Load uniprot db...")
    sqlContext = SQLContext(sc)
    meta_vocab = sqlContext.read.                    \
                 format('com.databricks.spark.csv'). \
                 options(comment='#',                \
                         delimiter='\t').            \
                 load(conf.uniprot).rdd.             \
                 map (lambda r : P53Inter ( A = r.C0, B = r.C1, alt_A = r.C4, alt_B = r.C5 ) ).cache ()
    
    logger.info ("Derive As and Bs from uniprot db...")
    A = meta_vocab.flatMap (lambda inter : get_As (inter, 'uniprotkb:P04637')).distinct().cache ()
    B = meta_vocab.flatMap (lambda inter : get_Bs (inter, 'uniprotkb:P04637')).distinct().cache ()

    logger.info ("Add MeSH derived kinase terms to list of As...")
    with open ("kinases.json", "r") as stream:
        kinases = json.loads (stream.read ())
        logger.info (kinases)
        A = A.union (sc.parallelize (kinases))

    logger.info ("Do lexical analyis of article text given As and Bs...")
    broadcastA = sc.broadcast (A.collect ())
    broadcastB = sc.broadcast (B.collect ())

    matches = articles. \
        flatMap (lambda a : lexer (a, broadcastA, broadcastB)). \
        collect ()

#        distinct (). \

    logger.info ("Output...")
    for m in matches:
        logger.info ("Match: {0}".format (m))

def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master",  help="Mesos master host")
    parser.add_argument("--name",    help="Spark framework name")
    parser.add_argument("--input",   help="Data root directory")
    parser.add_argument("--home",    help="App home")
    parser.add_argument("--venv",    help="Path to Python virtual environment to use")
    parser.add_argument("--uniprot", help="Path to Uniprot data")
    args = parser.parse_args()
    conf = KinaseConf (args.master, args.venv, args.name, args.input, args.uniprot)
    execute (conf, args.home)

main ()

