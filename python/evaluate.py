from __future__ import division
import argparse
import datetime
import fnmatch
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
from chemotext_util import BinaryEncoder
from chemotext_util import BinaryDecoder
from chemotext_util import Fact
from chemotext_util import Quant
from chemotext_util import EvaluateConf
from chemotext_util import LoggingUtil
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil
from equiv_set import EquivalentSet
from pyspark.sql import SQLContext

logger = LoggingUtil.init_logging (__file__)

def make_key (L, R, pmid):
    return "{0}->{1} i={2}".format (L, R, pmid)

class Facts(object):
    @staticmethod
    def load_facts (sqlContext, ctdRef, L_index, R_index, pmid_index):
        return sqlContext.read.                                     \
            format('com.databricks.spark.csv').                     \
            options(comment='#').                                   \
            load(ctdRef).rdd.                                       \
            map (lambda a : (a["C{0}".format (L_index)].lower (),
                             a["C{0}".format (R_index)].lower (),
                             a["C{0}".format (pmid_index)] ))
    @staticmethod
    def expand_ref_binaries (binary):
        result = []
        pmids = binary[2] if len(binary) > 1 else None
        if pmids:
            pmid_list = pmids.split ("|") if "|" in pmids else [ pmids ]
            for p in pmid_list:
                f = Fact (L=binary[0], R=binary[1], pmid=p)
                t = ( make_key(f.L, f.R, f.pmid), f ) 
                result.append (t)
                #    else:
                #        f = Fact ("L", "R", 0)
                #        t = ( make_key (f.L, f.R, f.pmid), f ) 
                #        result.append (t)
        return result
    @staticmethod
    def get_facts (sc, conf):
        sqlContext = SQLContext(sc)
        ab = Facts.load_facts (sqlContext, conf.ctdAB, 0, 3, 10)
        bc = Facts.load_facts (sqlContext, conf.ctdBC, 0, 2, 8)
        ac = Facts.load_facts (sqlContext, conf.ctdAC, 0, 3, 9)
        reference_binaries = [ ab, bc, ac ]
        return sc.union (reference_binaries).flatMap (Facts.expand_ref_binaries).cache ()

class Guesses(object):
    @staticmethod
    def get_article_guesses (article):
        guesses = article.AB + article.BC + article.AC + article.BB
        skiplist = [ 'for', 'was', 'she', 'long' ]
        result = []
        for g in guesses:
            if not g.L in skiplist and not g.R in skiplist:
                g.pmid = article.id
                result.append ( ( make_key (g.L, g.R, g.pmid), Guesses.distance (g) ) )
        return result
    @staticmethod
    def distance (b):
        b.dist = 1000000 * b.paraDist + 100000 * b.sentDist + b.docDist
        return b
    @staticmethod
    def get_guesses (sc, conf, articles, slices=1, slice_n=1):
        logger = LoggingUtil.init_logging (__file__)
        slice_size = int (len (articles) / slices)
        offset = slice_size * slice_n
        rest = len(articles) - offset
        if rest > slice_size and rest <= 2 * slice_size:
            slice_size = rest
        the_slice = articles [ offset : offset + slice_size ]
        logger.info ("   -- Guesses (input:{0}, articles:{1}, slice_size:{2}, offset:{3})".
                     format (conf.input_dir, len(articles), slice_size, offset)) 
        articles = sc.parallelize (the_slice, conf.spark_conf.parts). \
                map    (lambda p : SUtil.get_article (p)).         \
                filter (lambda p : p is not None).                 \
                map    (lambda a : EquivalentSet.get_article_equiv_set (a)).cache ()
        pmids = articles.map (lambda a : a.id).collect ()
        guesses = articles.flatMap (lambda article : Guesses.get_article_guesses (article)).cache ()
        return ( guesses, pmids )
    @staticmethod
    def mark_binary (binary, fact=True):
        binary.fact = fact
        binary.refs = []
        return binary
    @staticmethod
    def trace_set (trace_level, label, rdd):
        logger = LoggingUtil.init_logging (__file__)
        if (logger.getEffectiveLevel() > trace_level):
            for g in rdd.collect ():
                print ("  {0}> {1}->{2}".format (label, g[0], g[1]))
    @staticmethod
    def annotate (guesses, facts, article_pmids):
        trace_level = logging.ERROR
        Guesses.trace_set (trace_level, "Fact", facts)
        Guesses.trace_set (trace_level, "Guess", guesses)
        
        '''
        We get a list of all facts for every slice of guesses. It's not correct to subtract 

        true postivies from *all* facts. We need to use only facts asserted for the affected
        documents. Filter facts to only those with pmids in the target list.
        pmids <- articles.pmids
        target_facts <- facts.filter (pmids)
        '''
        relevant_facts = facts.filter (lambda f : f[1].pmid in article_pmids).cache ()
        
        # Things we found in articles that are facts
        true_positive = guesses.                                                  \
                        join (relevant_facts).                                    \
                        map (lambda b : ( b[0], Guesses.mark_binary (b[1][0], fact=True) ))
        Guesses.trace_set (trace_level, "TruePos", true_positive)
        
        # Things we found in articles that are not facts
        false_positive = guesses.subtractByKey (true_positive).                  \
                         map (lambda b : ( b[0], Guesses.mark_binary (b[1], fact=False) ))
        Guesses.trace_set (trace_level, "FalsePos", false_positive)
        
        # Things that are facts in these articles that we did not find
        false_negative = relevant_facts.subtractByKey (true_positive)
        Guesses.trace_set (trace_level, "FalseNeg", false_negative)
        
        union = true_positive. \
                union (false_positive). \
                union (false_negative)
        return union.map (lambda v: v[1])

def is_training (b):
    result = False
    try:
        result = int(b.pmid) % 2 == 0
    except ValueError:
        print ("(--) pmid: {0}".format (b.pmid))
    return result

def evaluate_articles (conf):
    logger.info ("Evaluating Chemotext2 output: {0}".format (conf.input_dir))
    sc = SparkUtil.get_spark_context (conf.spark_conf)
    logger.info ("Loading facts")
    facts = Facts.get_facts (sc, conf)
    logger.info ("Listing input files")
    articles = SUtil.get_article_paths (conf.input_dir)
    for slice_n in range (0, conf.slices):
        output_dir = os.path.join (conf.output_dir, "annotated", str(slice_n))
        if os.path.exists (output_dir):
            logger.info ("Skipping existing directory {0}".format (output_dir))
        else:
            logger.info ("Loading guesses")
            guesses, article_pmids = Guesses.get_guesses (sc, conf, articles, conf.slices, slice_n)
            annotated = Guesses.annotate (guesses, facts, article_pmids)

            logger.info ("Generating annotated output for " + output_dir)
            os.makedirs (output_dir)

            train = annotated.filter (lambda b : is_training (b)).map(lambda b : json.dumps (b, cls=BinaryEncoder))
            train_out_dir = os.path.join (output_dir, 'train')
            train.saveAsTextFile ("file://" + train_out_dir)
            print ("   --> train: {0}".format (train_out_dir))
            
            test  = annotated.filter (lambda b : not is_training (b)).map(lambda b : json.dumps (b, cls=BinaryEncoder))
            test_out_dir = os.path.join (output_dir, 'test')
            test.saveAsTextFile ("file://" + test_out_dir)
            print ("   --> test: {0}".format (test_out_dir))

def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",   help="Mesos master host")
    parser.add_argument("--name",   help="Spark framework name")
    parser.add_argument("--input",  help="Output directory for a Chemotext2 run.")
    parser.add_argument("--output", help="Output directory for evaluation.")
    parser.add_argument("--slices", help="Number of slices of files to iterate over.")
    parser.add_argument("--parts",  help="Number of partitions for the computation.")
    parser.add_argument("--venv",   help="Path to Python virtual environment to use")
    parser.add_argument("--ctdAB",  help="Path to CTD AB data")
    parser.add_argument("--ctdBC",  help="Path to CTD BC data")
    parser.add_argument("--ctdAC",  help="Path to CTD AC data")
    args = parser.parse_args()
    evaluate_articles (
        EvaluateConf (host           = args.host,
                      venv           = args.venv,
                      framework_name = args.name,
                      input_dir      = args.input.replace ("file://", ""),
                      output_dir     = args.output.replace ("file://", ""),
                      slices         = int(args.slices),
                      parts          = int(args.parts),
                      ctdAB          = args.ctdAB,
                      ctdBC          = args.ctdBC,
                      ctdAC          = args.ctdAC))

if __name__ == "__main__":
    main()


