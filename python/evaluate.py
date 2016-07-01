from __future__ import division
import argparse
import datetime
import glob
import json
import os
import logging
import math
import numpy
import re
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

def count_binaries (article_path, input_dir):
    ''' This is a function mapped to individual article summaries on distributed Spark workers.
    For each article, it loads it, and all its binaries.
    For each binary, it counts binaries discovered before its verifiable discovery reference date.
    It also counts false positives - non verifiable binary assertions.
    '''
    MIN_DATE = SUtil.parse_date ('1-1-1000')
    MAX_DATE = SUtil.parse_date ('1-1-9000')
    logger = LoggingUtil.init_logging (__file__)
    logger.info ("Article: @-- {0}".format (article_path))
    article = SUtil.read_article (article_path)
    before = 0
    not_before = 0
    false_positive = 0
    pmids = SUtil.get_pmid_map (os.path.join (input_dir, "pmid_date.json"))
    binaries = article.AB + article.BC + article.AC
    doc_date = SUtil.parse_date (article.date)
    for binary in binaries:
        if binary.fact:
            refs = binary.refs
            if refs:
                logger.debug ("fact: {0}".format (binary))
                ref_dates = [ SUtil.parse_date (pmids[ref]) if ref in pmids else None for ref in refs ]
                ref_dates = [ d for d in ref_dates if d ]
                min_ref_date = min (ref_dates) if len(ref_dates) > 0 else MIN_DATE
                max_ref_date = max (ref_dates) if len(ref_dates) > 0 else MAX_DATE
                if doc_date < min_ref_date:
                    logger.info ("  -- is_before")
                    before = before + 1
                else:
                    logger.info ("  -- is_not_before")
                    not_before = not_before + 1
        else:
            false_positive = false_positive + 1
    return Quant (before, not_before, false_positive)


def load_reference_binaries (sqlContext, ctdRef, L_index, R_index):
    return sqlContext.read.                 \
        format('com.databricks.spark.csv'). \
        options(comment='#').               \
        load(ctdRef).rdd.                   \
        map (lambda a : (a["C{0}".format (L_index)].lower (),
                         a["C{0}".format (R_index)].lower ()) )

def count_false_negatives_by_type (sqlContext, ctdRef, articles, L_index, R_index, tuple_type):
    '''
    Read a CSV formatted CTD file into a Spark RDD.
    Filter the RDD, ref, to create a list of reference binaries.
    Read designated articles into a second RDD and filter to binaries with references.
    Subtract generated facts from the CTD facts to create the false negative count.

    :param sqlContext: Spark SQL context.
    :param ctdRef: Comparative Toxicogenomics Database file.
    :param articles: List of articles to analyze.
    :param L_index: Index of the left term in this CTD file.
    :param R_index: Index of the right term in this CTD file.
    :param tupleType: AB/BC/AC
    '''
    ref = load_reference_binaries (sqlContext, ctdRef, L_index, R_index)
    '''
    ref = sqlContext.read.                    \
          format('com.databricks.spark.csv'). \
          options(comment='#').               \
          load(ctdRef).rdd.                   \
          map (lambda a : (a["C{0}".format (L_index)].lower (),
                           a["C{0}".format (R_index)].lower ()) )
    '''
    generated = articles.                                     \
                flatMap (lambda a : a.__dict__[tuple_type] ). \
                filter  (lambda a : a.fact ).                 \
                map     (lambda a : (a.L, a.R))
    return ref.subtract (generated).count ()

def count_false_negatives (sc, conf, article_paths):
    '''
    Counts and sums false negatives for each category of binaries.

    :param sc: Spark Context
    :param conf: Configuration
    :param articles: List of articles to 
    '''
    sqlContext = SQLContext(sc)
    articles = article_paths.map (lambda a : SUtil.read_article (a) )
    ab = count_false_negatives_by_type (sqlContext, conf.ctdAB, articles, 0, 3, "AB")
    bc = count_false_negatives_by_type (sqlContext, conf.ctdBC, articles, 0, 2, "BC")
    ac = count_false_negatives_by_type (sqlContext, conf.ctdAC, articles, 0, 3, "AC")
    return ab + bc + ac
    
def evaluate (conf):
    '''
    Evaluate the output of a Chemotext2 run.

    :param conf: The configuration to work with.

    load the pmid -> date map 
    foreach preprocessed article
       for all asserted binaries found in CTD
          b: sum corroborated binaries asserted in articles predating their references
          nb: sum binaries asserted not before reference dates
          tp += b + nb
    fp += sum asserted binaries not in CTD
    fn = sum CTD assertions found in no preprocessed article
    precision = tp / ( tp + fp)
    recall = tp / ( tp + fn )
    '''
    logger.info ("Evaluating Chemotext2 output: {0}".format (conf.input_dir))
    sc = SparkUtil.get_spark_context (conf)
    articles = glob.glob (os.path.join (conf.input_dir, "*fxml.json"))
    articles = sc.parallelize (articles [0:200])
    quanta = articles.map (lambda article : count_binaries (article, conf.input_dir))

    before = quanta.map (lambda q : q.before).sum()
    not_before = quanta.map (lambda q : q.not_before).sum ()
    false_positives = quanta.map (lambda q : q.false_positives).sum ()
    true_positives = before + not_before
    false_negatives = count_false_negatives (sc, conf, articles)

    logger.info ("before: {0} not_before: {1} false_positives: {2} true_positives: {3} false_negatives {4}".format (
        before, not_before, false_positives, true_positives, false_negatives))
    
    if true_positives > 0:
        precision = true_positives / ( true_positives + false_positives )
        recall = true_positives / ( true_positives + false_negatives )
        logger.info ("precision: {0}, recall: {1}".format (precision, recall))
    else:
        logger.info ("precision/recall can't be calculated. true_positives: {0}".format (true_positives))


#-----------------------------------------------------------------------------------------------
#-- V2.0 -
#-----------------------------------------------------------------------------------------------

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
        pmid_list = pmids.split ("|")
        if len(pmid_list) > 1:
            for p in pmid_list:
                f = Fact (L=binary[0], R=binary[1], pmid=p)
                t = ( make_key(f.L, f.R, f.pmid), f ) 
                result.append (t)
        else:
            f = Fact (L=binary[0], R=binary[1], pmid=pmid_list[0])
            t = ( make_key (f.L, f.R, f.pmid), f ) 
            result.append (t)
    else:
        f = Fact ("L", "R", 0)
        t = ( make_key (f.L, f.R, f.pmid), f ) 
        result.append (t)
    return result
def get_facts (sc, conf):
    sqlContext = SQLContext(sc)
    reference_binaries = [
        load_facts (sqlContext, conf.ctdAB, 0, 3, 10),
        load_facts (sqlContext, conf.ctdBC, 0, 2, 8),
        load_facts (sqlContext, conf.ctdAC, 0, 3, 9)
    ]
    return sc.union (reference_binaries). \
        flatMap (expand_ref_binaries)
    
def get_article_guesses (article_path):
    logger = LoggingUtil.init_logging (__file__)
    logger.info ("Article: @-- {0}".format (article_path))
    article = SUtil.read_article (article_path)
    return article.AB + article.BC + article.AC
def get_guesses (sc, conf):
    articles = glob.glob (os.path.join (conf.input_dir, "*fxml.json"))
    articles = sc.parallelize (articles)
    return articles.flatMap (lambda article : get_article_guesses (article)).cache ()

class CT2Params (object):
    def __init__(self, title, distanceThreshold):
        self.distanceThreshold = distanceThreshold
def get_parameter_sets ():
    return [
        CT2Params ("First-Run", 100)#,
#        CT2Params ("Second-Run", 500),
#        CT2Params ("Third-Run", 800)
    ]
'''
def strength (b):
    result = b
    if isinstance (b, Binary):
        doc, para, sent = ( b.docDist, b.paraDist, b.sentDist )
        result =                                         \
                 doc  * math.exp ( -doc  * (doc  - 1)) + \
                 para * math.exp ( -para * (para - 1)) + \
                 sent * math.exp ( -sent * (sent - 1))
    return result
def calculate_assertion_strength (a, b):
    return strength (a) + strength (b)
def calculate_equivalent_sets (observed, T):
    observed. \
        map (lambda b : ( make_key (b.L, b.R, b.pmid), b ) ).   \
        reduceByKey (lambda x, y : strength(x) + strength (y)). \
        map (lambda x : ( x[0], strength (x[1]) ) )
'''

def mark_binary (binary, fact=True):
    binary.fact = fact
    binary.refs = []
    return binary
def annotate (guesses, facts):    
    logger.info ("True positives...")

    trace = logging.DEBUG

    if (logger.getEffectiveLevel() == trace):
        for f in facts.filter (lambda f : f[0].find ('10,10-bis(4-pyridinylmethyl)-9(10h)') > -1 ).collect ():
            print "fact-----------> {0}".format (f)

    keyed_guesses = guesses. \
                    map (lambda b : ( make_key (b.L, b.R, b.pmid), b ) )
    if (logger.getEffectiveLevel() == trace):
        for g in keyed_guesses.collect ():
            print ("Guess> {0}->{1}".format (g[0], g[1]))

    true_positive = keyed_guesses. \
                    join (facts).                                        \
                    map (lambda b : ( b[0], mark_binary (b[1][0], fact=True) ))
    if (logger.getEffectiveLevel() == trace):
        for tp in true_positive.collect ():
            print "TPos-> k: {0} b: {1}".format (tp[0], tp[1])

    false_positive = keyed_guesses.subtractByKey (true_positive). \
                     map (lambda b : ( b[0], mark_binary (b[1], fact=False) ))
    if (logger.getEffectiveLevel() == trace):
        for fp in false_positive.collect ():
            print "FPos-> k: {0} b: {1}".format (fp[0], fp[1])

    logger.info ("False negatives...")
    false_negative = facts.subtractByKey (true_positive)
    if (logger.getEffectiveLevel() == trace):
        for fn in false_negative.collect ():
            print "FN>: {0}->{1}".format (fn[0], fn[1])

    return true_positive.union (false_positive). \
        map (lambda v: v[1])

def evaluate_articles (conf):
    logger.info ("Evaluating Chemotext2 output: {0}".format (conf.input_dir))
    sc = SparkUtil.get_spark_context (conf)

    facts = get_facts (sc, conf)
    guesses = get_guesses (sc, conf)
    annotated = annotate (guesses, facts)
    with open ("machine.txt", "w") as stream:
        stream.write (json.dumps (annotated.collect (), cls=BinaryEncoder, indent=2))

def read_training_set ():
    with open ("machine.txt", "r") as stream:
        decoder = BinaryDecoder ()
        for b in decoder.decode (stream.read ()):
            print b

def main ():
    '''
    Parse command line arguments for the evaluation pipeline.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",  help="Mesos master host")
    parser.add_argument("--name",  help="Spark framework name")
    parser.add_argument("--input", help="Output directory for a Chemotext2 run.")
    parser.add_argument("--venv",  help="Path to Python virtual environment to use")
    parser.add_argument("--ctdAB", help="Path to CTD AB data")
    parser.add_argument("--ctdBC", help="Path to CTD BC data")
    parser.add_argument("--ctdAC", help="Path to CTD AC data")
    args = parser.parse_args()
    conf = EvaluateConf (host           = args.host,
                         venv           = args.venv,
                         framework_name = args.name,
                         input_dir      = args.input,
                         ctdAB          = args.ctdAB,
                         ctdBC          = args.ctdBC,
                         ctdAC          = args.ctdAC)
    #evaluate (conf)
    evaluate_articles (conf)

main ()

