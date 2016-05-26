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
from chemotext_util import Quant
from chemotext_util import Article
from chemotext_util import SerializationUtil as SUtil

def init_logging ():
    FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    return logging.getLogger(__file__)

logger = init_logging ()

def get_spark_context (conf):
    os.environ['PYSPARK_PYTHON'] = "{0}/bin/python".format (conf.venv)
    from pyspark import SparkConf, SparkContext
    from StringIO import StringIO
    ip = socket.gethostbyname(socket.gethostname())
    sparkConf = (SparkConf()
                 .setMaster(conf.host)
                 .setAppName(conf.framework_name))
    return SparkContext(conf = sparkConf)

def process_article (article_path, input_dir):
    MIN_DATE = SUtil.parse_date ('1-1-1000')
    MAX_DATE = SUtil.parse_date ('1-1-9000')
    logger = init_logging ()
    logger.info ("Article: @-- {0}".format (article_path))
    traceback.print_stack ()
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

def count_false_negatives (sc, conf, articles):
    from pyspark.sql import SQLContext
    sqlContext = SQLContext(sc)
    ref_AB = sqlContext.read.format('com.databricks.spark.csv').options(comment='#').load(conf.ctdAB).rdd
    ref_AB = ref_AB.map (lambda a : (a.C0, a.C3) )
    gen_AB = articles.flatMap (lambda a : SUtil.read_article (a).AB )
    gen_AB = gen_AB.filter (lambda a : a.fact ).map (lambda a : (a.L, a.R))
    return ref_AB.subtract (gen_AB).count ()

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
    sc = get_spark_context (conf)

    articles = glob.glob (os.path.join (conf.input_dir, "*fxml.json"))
    articles = sc.parallelize (articles [0:200])
    quanta = articles.map (lambda article : process_article (article, conf.input_dir))

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
        logger.info ("precision: {0}".format (precision))
        logger.info ("recall: {0}".format (recall))
    else:
        logger.info ("precision/recall can't be calculated. true_positives: {0}".format (true_positives))

class Conf(object):
    def __init__(self, host, venv, framework_name, input_dir, ctdAB, ctdBC, ctdAC):
        self.host = host
        self.venv = venv
        self.framework_name = framework_name
        self.input_dir = input_dir
        self.ctdAB = ctdAB
        self.ctdBC = ctdBC
        self.ctdAC = ctdAC

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
    
    conf = Conf (host           = args.host,
                 venv           = args.venv,
                 framework_name = args.name,
                 input_dir      = args.input,
                 ctdAB          = args.ctdAB,
                 ctdBC          = args.ctdBC,
                 ctdAC          = args.ctdAC)

    evaluate (conf)

main ()

