from __future__ import division
import argparse
import datetime
import glob
import json
import os
import logging
import math
import re
import shutil
import sys
import socket
import time
import traceback
from chemotext_util import Article
from chemotext_util import ArticleEncoder
from chemotext_util import Binary
from chemotext_util import BinaryEncoder
from chemotext_util import BinaryDecoder
from chemotext_util import SparkConf
from chemotext_util import EquivConf
from chemotext_util import LoggingUtil
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil

from equiv_set import EqBinary
from equiv_set import EquivalentSet

logger = LoggingUtil.init_logging (__file__)

def get_article (article_path):
    logger = LoggingUtil.init_logging (__file__)
    logger.info ("Article: @-- {0}".format (article_path))
    return SUtil.read_article (article_path)
def get_articles (sc, conf):
    articles = glob.glob (os.path.join (conf.input_dir, "*fxml.json"))
    #articles = glob.glob (os.path.join (conf.input_dir, "Mol_Cancer_2008_May_12_7_37*fxml.json"))
    logger.info ("-- articles:{0}".format (len (articles)))
    return sc.parallelize (articles, conf.spark_conf.parts).map (lambda p : get_article (p))

def write_article (article, path):
    subdir = os.path.join (path, article.fileName[0], article.fileName[1])
    if not os.path.exists (subdir):
        os.makedirs (subdir)
    output_path = os.path.join (subdir, "{0}.json".format (article.fileName))
    with open (output_path, "w") as stream:
        stream.write (json.dumps (article, cls=ArticleEncoder, indent=2))
    return article

def process_article (article, output_dir):
    output_dir = output_dir.replace ("file://", "")
    logger = LoggingUtil.init_logging (__file__)
    result = []
    subdir = os.path.join (output_dir, article.fileName[0], article.fileName[1])
    if not os.path.exists (subdir):
        os.makedirs (subdir)
    output_path = os.path.join (subdir, "{0}.json".format (article.fileName))
    if os.path.exists (output_path):
        logger.info ("Skipping {0}".format (output_path))
    else:
        logger.info ("Writing article: {0}".format (output_path))
#        result.append (write_article (get_article_equiv_set (article), output_dir).id)
        result.append (write_article (EquivalentSet.get_article_equiv_set (article), output_dir).id)
    return result

def trace_set (trace_level, label, rdd):
    if (logger.getEffectiveLevel() > trace_level):
        for g in rdd.collect ():
            print ("  {0}> {1}->{2}".format (label, g[0], g[1]))

def execute (conf):
    sc = SparkUtil.get_spark_context (conf.spark_conf)
    if not os.path.exists (conf.output_dir):
        os.mkdir (conf.output_dir)
    logger.info ("Generating equivalent sets from {0} to {1}".format (conf.input_dir, conf.output_dir))
    get_articles (sc, conf).flatMap (lambda a : process_article (a, conf.output_dir)).collect ()

def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",   help="Mesos master host")
    parser.add_argument("--name",   help="Spark framework name")
    parser.add_argument("--venv",   help="Path to Python virtual environment to use")
    parser.add_argument("--parts",  help="Number of partitions for the computation.")
    parser.add_argument("--input",  help="Output directory for a Chemotext2 run.")
    parser.add_argument("--output", help="Output directory for equivalent sets.")
    args = parser.parse_args()
    execute (EquivConf (
        spark_conf = SparkConf (
            host           = args.host,
            venv           = args.venv,
            framework_name = args.name,
            parts          = int(args.parts)),
        input_dir      = args.input,
        output_dir     = args.output))
             
if __name__ == "__main__":
    main()

