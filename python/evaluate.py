import argparse
import datetime
import glob
import json
import os
import logging
import sys
import socket
from Quant import Quant

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
    
def read_json_file (file_name):
    result = None
    with open (file_name) as stream:
        result = json.loads (stream.read ())
    return result

def get_pmid_map (pmids):
    return read_json_file (pmids)

def get_article (article):
    return read_json_file (article)

def parse_date (date):
    return datetime.datetime.strptime (date, "%d-%m-%Y")

def process_article (article_path, input_dir):
    logger = init_logging ()
    logger.info ("Article: @-- {0}".format (article_path))
    article = get_article (article_path)
    before = 0
    not_before = 0
    false_positive = 0
    pmids = get_pmid_map (os.path.join (input_dir, "pmid_date.json"))
    binaries = article["AB"] + article["BC"] + article["AC"]
    for binary in binaries:
        logger.info ("Binary: {0}".format (binary))        
        doc_date = parse_date (article['date'])
        if binary ['fact'] == 'true':
            ref_dates = [ parse_date (pmids[ref]) if ref in pmids else None for ref in refs ]
            min_ref_date = min (ref_dates)
            max_ref_date = max (ref_dates)
            if doc_date < min_ref_date:
                before = before + 1
            else:
                not_before = not_before + 1
        else:
            false_positive = false_positive + 1
    return Quant(before, not_before, false_positive)

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

    precision_denominator = ( true_positives + false_positives ) 

    logger.info ("before: {0} not_before: {1} false_positives: {2} true_positives: {3} precision_denom: {4}".format (
        before, not_before, false_positives, true_positives, precision_denominator))
    
    if precision_denominator > 0:
        logger.info ("precision: {0}".format ( true_positives / ( true_positives + false_positives ) ))
    else:
        logger.info ("precision can't be calculated. true_positives: {0}".format (true_positives))

class Conf(object):
    def __init__(self, host, venv, framework_name, input_dir):
        self.host = host
        self.venv = venv
        self.framework_name = framework_name
        self.input_dir = input_dir

def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",  help="Mesos master host")
    parser.add_argument("--name",  help="Spark framework name")
    parser.add_argument("--input", help="Output directory for a Chemotext2 run.")
    parser.add_argument("--venv",  help="Path to Python virtual environment to use")
    args = parser.parse_args()
    
    conf = Conf (host = args.host,
                 venv = args.venv,
                 framework_name = args.name,
                 input_dir = args.input)
    evaluate (conf)

main ()

