import argparse
import datetime
import glob
import json
import os
import logging
import sys

def init_logging ():
    FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    return logging.getLogger(__file__)
logger = init_logging ()

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

class Quant(object):
    def __init__(self, before, not_before, false_positives):
        self.before = before
        self.not_before = not_before
        self.false_positives = false_positives

def process_article (article_path, input_dir):
    logger.info ("Article: @-- {0}".format (article_path))
    article = get_article (article_path)
    before = 0
    not_before = 0
    false_positive = 0
    pmids = get_pmid_map (os.path.join (input_dir, "pmid_date.json"))
    binaries = article["AB"] + article["BC"] + article["AC"]
    for binary in binaries:
        logger.info ("Binary: {0}".format (binary))        
        if 'fact' in binary:
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
    return Quant (before, not_before, false_positive)

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
def evaluate (input_dir):
    logger.info ("Evaluating Chemotext2 output: {0}".format (input_dir))
    articles = glob.glob (os.path.join (input_dir, "*fxml.json"))
    articles = articles [0:200]
    quanta = [ process_article (article_path, input_dir) for article_path in articles ]
    before = sum ([ quant.before for quant in quanta ])
    not_before = sum ([ quant.not_before for quant in quanta ])
    false_positives = sum ([ quant.false_positives for quant in quanta ])
    true_positives = before + not_before

    precision_denominator = ( true_positives + false_positives ) 
    if precision_denominator > 0:
        logger.info ("precision: {0}".format ( true_positives / ( true_positives + false_positives ) ))
    else:
        logger.info ("precision can't be calculated. true_positives: {0}".format (true_positives))

def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Output directory for a Chemotext2 run.")
    args = parser.parse_args()
    evaluate (args.input)

main ()

