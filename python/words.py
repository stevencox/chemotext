from __future__ import division
import argparse
import glob
import json
import os
import gensim
import logging
import traceback
from chemotext_util import Article
from chemotext_util import Binary
from chemotext_util import BinaryEncoder
from chemotext_util import BinaryDecoder
from chemotext_util import Fact
from chemotext_util import Quant
from chemotext_util import CTDConf
from chemotext_util import SparkConf
from chemotext_util import EvaluateConf
from chemotext_util import LoggingUtil
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil

logger = LoggingUtil.init_logging (__file__)
 
class SentenceGenerator(object):
    def __init__(self, conf):
        sc = SparkUtil.get_spark_context (conf.spark_conf)
        article_paths = SUtil.get_article_paths (conf.input_dir) [:20000]
        self.articles = sc.parallelize (article_paths, conf.spark_conf.parts). \
                        map (lambda p : SUtil.get_article (p))

    def __iter__(self):
        # Take a chunk of articles
        chunk = self.articles.take (1000)

        # Remove taken articles 
        ids = [ a.id for a in chunk ]
        self.articles = self.articles.filter (lambda a : a.id not in ids)

        # Get the sentences
        all_sentences = [ s.split (' ') for a in chunk for p in a.paragraphs for s in p.sentences ]
        print ("Processing {0} sentences.".format (len (all_sentences)))
        with open ("x", "w") as stream:
            for s in all_sentences:
                stream.write (" sentence=> {0}\n".format (s))
        for each in all_sentences:
            yield each

def build (conf):
    sentences = SentenceGenerator (conf) # a memory-friendly iterator
    model = gensim.models.Word2Vec (sentences)

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
    build (conf)
 
main ()
