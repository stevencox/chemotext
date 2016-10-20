from __future__ import division
import argparse
import glob
import json
import os
import gensim
import logging
import traceback
from chemotext_util import Article
from chemotext_util import CT2
from chemotext_util import SparkConf
from chemotext_util import EvaluateConf
from chemotext_util import LoggingUtil
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil

logger = LoggingUtil.init_logging (__file__)

def parse_article (p): 
    result = []
    a = SUtil.get_article (p)
    if a:
        a.words = [ s.split (' ') for paragraph in a.paragraphs for s in paragraph.sentences ]
        a.words = [ w.replace ('.', '') for w_set in a.words for w in w_set ]
    return a

class SentenceGenerator(object):
    def __init__(self, conf):
        sc = SparkUtil.get_spark_context (conf.spark_conf)
        article_paths = SUtil.get_article_paths (conf.input_dir) [:200000]
        self.articles = sc.parallelize (article_paths, conf.spark_conf.parts). \
                        map (lambda p : parse_article (p))

    def __iter__(self):
        # Take a chunk of articles
        chunk = self.articles.take (10000)

        # Remove taken articles 
        ids = [ a.id for a in chunk ]
        self.articles = self.articles.filter (lambda a : a.id not in ids)

        # Get the sentences
        all_sentences = [ a.words for a in chunk ]
        print ("Processing {0} sentences.".format (len (all_sentences)))
        with open ("x", "w") as stream:
            for s in all_sentences:
                stream.write (" sentence=> {0}\n".format (s))
        for each in all_sentences:
            yield each
def build (conf):
    sentences = SentenceGenerator (conf) # a memory-friendly iterator
    model = gensim.models.Word2Vec (sentences, workers = 32)

#--------------------------------------------------------------------------------------------------------------

def generate_year_model (w2v_dir, year, sentences):
    model = None
#    print ("sentences length in w2v> {0}".format (len(sentences)))
    if len(sentences) > 0:
        model = gensim.models.Word2Vec (sentences, workers=8)
        file_name = os.path.join (w2v_dir, "pmc-{0}.w2v".format (year))
        print ("  *** Writing model file => {0}".format (file_name))
        model.save (file_name)
    return model

def sentences_by_year (a):
    result = ( 0, [] )
    if a is not None and a.date is not None and a.date.count ('-') == 2:
        print ("--------------> {0}".format (a.date))
        year = a.date.split('-')[2]
        if len(year) >= 4:
            result = (
                year[:4],
                [ s.split(' ') for p in a.paragraphs for s in p.sentences ]
            )
    return result

def build_models (conf):
    root = os.path.dirname (conf.input_dir)
    w2v_dir = os.path.join (root, "w2v", "gensim")
    limit=5000000

    count = CT2.from_conf (conf, limit=limit). \
            articles.map (lambda a : sentences_by_year (a)). \
            reduceByKey (lambda x,y: x + y). \
            map (lambda x : generate_year_model (w2v_dir, x[0], x[1])).count ()

    print ("Count: {0}".format (count))

def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",   help="Mesos master host")
    parser.add_argument("--name",   help="Spark framework name")
    parser.add_argument("--input",  help="Output directory for a Chemotext2 run.")
    parser.add_argument("--output", help="Output directory for evaluation.")
    parser.add_argument("--slices", help="Number of slices of files to iterate over.")
    parser.add_argument("--parts",  help="Number of partitions for the computation.")
    parser.add_argument("--venv",   help="Path to Python virtual environment to use")
    args = parser.parse_args()
    conf = EvaluateConf (
        spark_conf = SparkConf (host           = args.host,
                                venv           = args.venv,
                                framework_name = args.name,
                                parts          = int(args.parts)),
        input_dir      = args.input.replace ("file://", ""),
        output_dir     = args.output.replace ("file://", ""),
        slices         = int(args.slices))
    build_models (conf)
 
main ()



#    with open ("out.txt", "w") as stream:
#        stream.write ("sentence_by_year keys: {0}".format (sent_by_year.map(lambda sy : sy[0]).collect ()))

