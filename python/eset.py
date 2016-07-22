from __future__ import division
import argparse
import datetime
import glob
import json
import os
import logging
import math
#import numpy
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

logger = LoggingUtil.init_logging (__file__)

def get_article (article_path):
    logger = LoggingUtil.init_logging (__file__)
    logger.info ("Article: @-- {0}".format (article_path))
    return SUtil.read_article (article_path)
def get_articles (sc, conf):
    #articles = glob.glob (os.path.join (conf.input_dir, "*fxml.json"))
    articles = glob.glob (os.path.join (conf.input_dir, "Mol_Cancer_2008_May_12_7_37*fxml.json"))
    return sc.parallelize (articles, conf.spark_conf.parts).map (lambda p : get_article (p))

class EqBinary(object):
    def __init__(self, binary, L, R):
        self.binary = binary
        self.L = L
        self.R = R
    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        return "l:{0} r:{1} dist:{2} lpos:{3} rpos:{4}".format (
            self.binary.L, self.binary.R, self.binary.docDist, self.L.docPos, self.R.docPos)

log_trace = True
def log_sorted (s):
    if log_trace:
        print ("   Sorted")
        for k in s:
            print ("     1. sorted: {0}".format (k))

def log_discard (e):
    if log_trace:
        print ("     3. discard: l:{0} r:{1} dist:{2} lpos:{3} rpos:{4}".format (
            e.binary.L, e.binary.R, e.binary.docDist, e.L.docPos, e.R.docPos))

def log_reduced_set (REk_key, original_length, key):
    if log_trace:
        print ("Reduced length from {0} to {1} for key {2}".format (original_length, len (REk_key), key))
        for e in REk_key:
            print ("   4. REk_key: l:{0} r:{1} dist:{2}".format (e.L, e.R, e.docDist))

def make_equiv_set (L, R):
    pairs = {}
    for left in L:
        for right in R:
            docDist = abs (left.docPos - right.docPos)
            if docDist < 200:
                key = "{0}@{1}".format (left.word, right.word)
                binary = Binary (
                    id = 0, # id
                    L = left.word,
                    R = right.word,
                    docDist = docDist,
                    sentDist = abs ( left.sentPos - right.sentPos ),
                    paraDist = abs ( left.paraPos - right.paraPos ),
                    code = 1,
                    fact = False,
                    refs = [])
                #print ("making binary: {0}".format (binary))
                if key in pairs:
                    pairs[key].append ( EqBinary (binary, left, right) )
                else:
                    pairs[key] = [ EqBinary (binary, left, right) ]
    # GroupBy (x,y)
    REk = []
    for key in pairs:
        REk_key = []
        print ("key: {0}".format (key))
        Ek = pairs[key]
        original_length = len (Ek)
        # Sort
        Ek.sort (key = lambda p: p.binary.docDist)
        while len(Ek) > 0:
            log_sorted (Ek)
            canonical = Ek[0]
            print ("     2. canonical: {0}".format (canonical))
            # Min distance pair
            REk_key.append (canonical.binary)
            for e in Ek:
                if e.L.docPos == canonical.L.docPos or e.R.docPos == canonical.R.docPos:
                    Ek.remove (e)
                    log_discard (e)
        log_reduced_set (REk_key, original_length, key)
        REk = REk + REk_key
    return REk

def get_article_equiv_set (article):
    article.AB = make_equiv_set (article.A, article.B)
    article.BC = make_equiv_set (article.B, article.C)
    return article
        
def write_article (article, path):
    subdir = os.path.join (path, article.fileName[0])
    if not os.path.exists (subdir):
        os.mkdir (subdir)
    output_path = os.path.join (subdir, "{0}.json".format (article.fileName))
    with open (output_path, "w") as stream:
        stream.write (json.dumps (article, cls=ArticleEncoder, indent=2))
        stream.flush ()
    return article

def trace_set (trace_level, label, rdd):
    if (logger.getEffectiveLevel() > trace_level):
        for g in rdd.collect ():
            print ("  {0}> {1}->{2}".format (label, g[0], g[1]))

def execute (conf):
    logger.info ("Evaluating Chemotext2 output: {0}".format (conf.input_dir))
    output_dir = os.path.join (conf.output_dir, "equiv_sets")
    articles = glob.glob (os.path.join (conf.input_dir, "Mol_Cancer_2008_May_12_7_37*fxml.json"))
    a = get_article_equiv_set (get_article (articles[0]))
    if os.path.exists (output_dir):
        shutil.rmtree (output_dir)
    os.mkdir (output_dir)
    write_article (a, output_dir)

def x():
    sc = SparkUtil.get_spark_context (conf.spark_conf)
    equiv_sets = get_articles (sc, conf).map (lambda a : get_article_equiv_set (a))
    output_dir = os.path.join (conf.output_dir, "equiv_sets")
    logger.info ("Generating annotated output for " + output_dir)
    if os.path.exists (output_dir):
        shutil.rmtree (output_dir)
    os.mkdir (output_dir)
    equiv_sets.map (lambda a : write_article (a, output_dir)).cache ()
    for e in equiv_sets.collect():
        print ("{0}".format (json.dumps (e, cls=ArticleEncoder, indent=2)))
        write_article (e, output_dir)

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
             
main ()

