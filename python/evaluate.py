from __future__ import division
import argparse
import datetime
import glob
import json
import os
import logging
import sys
import socket
import time
import traceback
from chemotext_util import Article
from chemotext_util import EvaluateConf
from chemotext_util import LoggingUtil
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil
from chemotext_util import Quant
from pyspark.sql import SQLContext

logger = LoggingUtil.init_logging (__file__)

def count_binaries (article_path, input_dir):
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
    for binary in binaries:
        doc_date = SUtil.parse_date (article.date)
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

def count_false_negatives_by_type (sqlContext, ctdRef, articles, L_index, R_index, tupleType):
    ref = sqlContext.read. \
          format('com.databricks.spark.csv'). \
          options(comment='#'). \
          load(ctdRef).rdd \
          map (lambda a : (a["C{0}".format (L_index)].lower (),
                           a["C{0}".format (R_index)].lower ()) )
    generated = articles. \
                flatMap (lambda a : SUtil.read_article (a).__dict__[tupleType] ). \
                filter (lambda a : a.fact ). \
                map (lambda a : (a.L, a.R))
    return ref.subtract (generated).count ()

def count_false_negatives (sc, conf, articles):
    sqlContext = SQLContext(sc)
    ab = count_false_negatives_by_type (sqlContext, conf.ctdAB, articles, 0, 3, "AB")
    bc = count_false_negatives_by_type (sqlContext, conf.ctdBC, articles, 0, 2, "BC")
    ac = count_false_negatives_by_type (sqlContext, conf.ctdAC, articles, 0, 3, "AC")
    return ab + bc + ac
    
'''
load the pmid -> date map 
foreach preprocessed article
   for all asserted binaries found in CTD
      b: detect binaries asserted in an article predating the binarys reference articles
      nb: detect binaries asserted not before the date of reference articles
   tp += b + nb
   fp += count all asserted binaries not in CTD
fn = count CTD assertions found in no preprocessed article
precision = tp / ( tp + fp)
recall = tp / ( tp + fn )
'''
def evaluate (conf):
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

def main ():
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
    evaluate (conf)

main ()

