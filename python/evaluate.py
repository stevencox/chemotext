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
from pyspark.sql import SQLContext

logger = LoggingUtil.init_logging (__file__)

def make_key (L, R, pmid):
    return "{0}->{1} i={2}".format (L, R, pmid)

def load_facts (sqlContext, ctdRef, L_index, R_index, pmid_index):
    return sqlContext.read.                                     \
        format('com.databricks.spark.csv').                     \
        options(comment='#').                                   \
        load(ctdRef).rdd.                                       \
        map (lambda a : (a["C{0}".format (L_index)].lower (),
                         a["C{0}".format (R_index)].lower (),
                         a["C{0}".format (pmid_index)] ))

def expand_ref_binaries (binary):
    result = []
    pmids = binary[2] if len(binary) > 1 else None
    if pmids:
        pmid_list = pmids.split ("|") if "|" in pmids else [ pmids ]
        for p in pmid_list:
            f = Fact (L=binary[0], R=binary[1], pmid=p)
            t = ( make_key(f.L, f.R, f.pmid), f ) 
            result.append (t)
    else:
        f = Fact ("L", "R", 0)
        t = ( make_key (f.L, f.R, f.pmid), f ) 
        result.append (t)
    return result
def get_facts (sc, conf):
    sqlContext = SQLContext(sc)
    ab = load_facts (sqlContext, conf.ctdAB, 0, 3, 10)
    bc = load_facts (sqlContext, conf.ctdBC, 0, 2, 8)
    ac = load_facts (sqlContext, conf.ctdAC, 0, 3, 9)
    reference_binaries = [ ab, bc, ac ]
    return sc.union (reference_binaries).flatMap (expand_ref_binaries).cache ()

def get_article (article_path):
    logger = LoggingUtil.init_logging (__file__)
    logger.info ("Article: @-- {0}".format (article_path))
    article = SUtil.read_article (article_path)
    return article
def get_article_guesses (article):
    guesses = article.AB + article.BC + article.AC
    skiplist = [ 'for', 'was', 'she', 'long' ]
    result = []
    for g in guesses:
        if not g.L in skiplist and not g.R in skiplist:
            g.pmid = article.id
            result.append (g)
    return result
def distance (b):
    b.dist = 1000000 * b.paraDist + 100000 * b.sentDist + b.docDist
    return b

def get_guesses (sc, conf, articles, slices=1, slice_n=1):
    slice_size = int (len (articles) / slices)
    offset = slice_size * slice_n
    logger.info ("   -- Evaluate execution (input=:{0},articles:{1},slice_size:{2}, offset:{3})".
                 format (conf.input_dir, len(articles), slice_size, offset)) 
    articles = articles [ offset : offset + slice_size ]
    articles = sc.parallelize (articles, conf.parts)
    articles = articles.map (lambda p : get_article (p))
    pmids = articles.map (lambda a : a.id).collect ()
    guesses = articles.\
              flatMap (lambda article : get_article_guesses (article)). \
              map (lambda b : ( make_key (b.L, b.R, b.pmid), distance (b) ) )
    return (guesses, pmids)

def mark_binary (binary, fact=True):
    binary.fact = fact
    binary.refs = []
    return binary
def trace_set (trace_level, label, rdd):
    if (logger.getEffectiveLevel() > trace_level):
        for g in rdd.collect ():
            print ("  {0}> {1}->{2}".format (label, g[0], g[1]))
def annotate (guesses, facts, article_pmids):
    trace_level = logging.ERROR
    trace_set (trace_level, "Fact", facts)
    trace_set (trace_level, "Guess", guesses)

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
                    map (lambda b : ( b[0], mark_binary (b[1][0], fact=True) ))
    trace_set (trace_level, "TruePos", true_positive)

    # Things we found in articles that are not facts
    false_positive = guesses.subtractByKey (true_positive).                  \
                     map (lambda b : ( b[0], mark_binary (b[1], fact=False) ))
    trace_set (trace_level, "FalsePos", false_positive)

    # Things that are facts in these articles that we did not find
    false_negative = relevant_facts.subtractByKey (true_positive)
    trace_set (trace_level, "FalseNeg", false_negative)

    union = true_positive. \
            union (false_positive). \
            union (false_negative)
    return union.map (lambda v: v[1])

def get_articles (conf):
    count = 0
    articles = []
    files = "evalfiles.json"
    if os.path.exists (files):
        with open (files, "r") as stream:
            articles = json.loads (stream.read ())
    else:
        for root, dirnames, filenames in os.walk (conf.input_dir):
            for filename in fnmatch.filter(filenames, '*.fxml.json'):
                articles.append(os.path.join(root, filename))
                count = count + 1
                if (count % 10000 == 0):
                    logger.info ("   ...{0} files".format (len (articles)))
        with open (files, "w") as stream:
            stream.write (json.dumps (articles, indent=2))
    return articles

def is_training (b):
    result = False
    try:
        result = int(b.pmid) % 2 == 0
    except ValueError:
        print ("(--) pmid: {0}".format (b.pmid))
    return result

def evaluate_articles (conf):
    logger.info ("Evaluating Chemotext2 output: {0}".format (conf.input_dir))
    sc = SparkUtil.get_spark_context (conf)
    logger.info ("Loading facts")
    facts = get_facts (sc, conf)
    for slice_n in range (0, conf.slices):
        output_dir = os.path.join (conf.output_dir, "annotated", str(slice_n))
        if os.path.exists (output_dir):
            logger.info ("Skipping existing directory {0}".format (output_dir))
        else:
            logger.info ("Listing input files")
            articles = get_articles (conf)

            logger.info ("Loading guesses")
            guesses, article_pmids = get_guesses (sc, conf, articles, conf.slices, slice_n)
            annotated = annotate (guesses, facts, article_pmids)
            logger.info ("Generating annotated output for " + output_dir)
            os.makedirs (output_dir)
            '''
            annotated.\
                map(lambda b : json.dumps (b, cls=BinaryEncoder)). \
                saveAsTextFile ("file://" + output_dir)
            '''
            train = annotated.filter (lambda b : is_training (b)).map(lambda b : json.dumps (b, cls=BinaryEncoder))
            train_out_dir = os.path.join (output_dir, 'train')
            train.saveAsTextFile ("file://" + train_out_dir)
            
            test  = annotated.filter (lambda b : not is_training (b)).map(lambda b : json.dumps (b, cls=BinaryEncoder))
            test_out_dir = os.path.join (output_dir, 'test')
            test.saveAsTextFile ("file://" + test_out_dir)

def evaluate_articles0 (conf):
    logger.info ("Evaluating Chemotext2 output: {0}".format (conf.input_dir))
    sc = SparkUtil.get_spark_context (conf)
    logger.info ("Loading facts")
    facts = get_facts (sc, conf)
    output_dir = os.path.join (conf.output_dir, "annotated", str(slice_n))
    logger.info ("Loading guesses")
    guesses, article_pmids = get_guesses (sc, conf, conf.slices, slice_n)
    annotated = annotate (guesses, facts, article_pmids)
    logger.info ("Generating annotated output for " + output_dir)
    annotated.\
        map(lambda b : json.dumps (b, cls=BinaryEncoder)). \
        saveAsTextFile ("file://" + output_dir)

def train_log_reg (sc, annotated):
    def label (b):
        features = [ b.docDist, b.paraDist, b.sentDist ]
        return LabeledPoint (1.0 if b.fact else 0.0, features)
    labeled = annotated.map (label)
    model = LogisticRegressionWithLBFGS.train (labeled)
        
    labelsAndPreds = labeled.map (lambda p: (p.label, model.predict(p.features)))
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(labeled.count())
    print("Training Error = {0}".format (trainErr))

    model.save(sc, "machine")
    sameModel = LogisticRegressionModel.load(sc, "machine")

def json_io ():
    with open ("machine.txt", "w") as stream:
        stream.write (json.dumps (annotated.collect (), cls=BinaryEncoder, indent=2))

def main ():
    '''
    Parse command line arguments for the evaluation pipeline.
    '''
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
    conf = EvaluateConf (host           = args.host,
                         venv           = args.venv,
                         framework_name = args.name,
                         input_dir      = args.input,
                         output_dir     = args.output,
                         slices         = int(args.slices),
                         parts          = int(args.parts),
                         ctdAB          = args.ctdAB,
                         ctdBC          = args.ctdBC,
                         ctdAC          = args.ctdAC)
    #evaluate (conf)
    evaluate_articles (conf)

main ()

