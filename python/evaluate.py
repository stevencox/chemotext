from __future__ import division
import argparse
import calendar
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
from chemotext_util import CTDConf
from chemotext_util import SparkConf
from chemotext_util import EvaluateConf
from chemotext_util import LoggingUtil
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil
from equiv_set import EquivalentSet
from itertools import chain
from operator import add
from plot import Plot
from pyspark.sql import SQLContext

logger = LoggingUtil.init_logging (__file__)

debug_scale = 1

def make_key (L, R, pmid):
    return "{0}->{1} i={2}".format (L, R, pmid)

def simplekey (b):
    return "{0}@{1}".format (b.L, b.R)

class Facts(object):

    @staticmethod
    def load_facts (sqlContext, tableRef, L_index, R_index, pmid_index, delimiter=","):
        with open ("debug.log", "a") as stream:
            stream.write ("  load facts -> conf : {0}".format (tableRef))
        return sqlContext.read.                                     \
            format('com.databricks.spark.csv').                     \
            options(comment='#').                                   \
            options(delimiter=delimiter).                           \
            load(tableRef).rdd.                                       \
            map (lambda a : (a["C{0}".format (L_index)].lower (),
                             a["C{0}".format (R_index)].lower (),
                             a["C{0}".format (pmid_index)] ))
    @staticmethod
    def expand_ref_binaries (binary):
        result = []
        pmids = binary[2] if len(binary) > 1 else None
        if pmids:
            if "|" in pmids:
                pmid_list = pmids.split ("|")
            elif ";" in pmids:
                pmid_list = pmids.split (";")
            else:
                pmid_list = [ pmids ]
            for p in pmid_list:
                f = Fact (L=binary[0], R=binary[1], pmid=p)
                t = ( make_key(f.L, f.R, f.pmid), f )
                result.append (t)
        return result

    @staticmethod
    def get_facts (sc, conf):
        sqlContext = SQLContext(sc)
        ab = Facts.load_facts (sqlContext, conf.ctdAB, 0, 3, 10)
        bc = Facts.load_facts (sqlContext, conf.ctdBC, 0, 2, 8)
        ac = Facts.load_facts (sqlContext, conf.ctdAC, 0, 3, 9)
        bb = Facts.load_facts (sqlContext, conf.geneGene, 0, 2, 4, delimiter="\t")
        reference_binaries = [ ab, bc, ac, bb ]
        return sc.union (reference_binaries).flatMap (Facts.expand_ref_binaries).\
            sample(False, debug_scale).\
            cache ()

class Guesses(object):

    @staticmethod
    def get_article_guesses (article):
        guesses = article.AB + article.BC + article.AC + article.BB
        skiplist = [ 'for', 'was', 'she', 'long' ]
        result = []
        for g in guesses:
            if not g.L in skiplist and not g.R in skiplist:
                g.pmid = article.id
                try:
                    date = SUtil.parse_date (article.date)
                    if date:
                        g.date = calendar.timegm (date.timetuple())
                except:
                    print ("No date parsed in {0}".format (article.fileName))
                result.append ( ( make_key (g.L, g.R, g.pmid), Guesses.distance (g) ) )
        return result

    @staticmethod
    def distance (b):
        b.dist = 1000000 * b.paraDist + 100000 * b.sentDist + b.docDist
        return b

    @staticmethod
    def get_guesses (sc, input_dir, partitions, articles, slices=1, slice_n=1):
        from chemotext_util import LoggingUtil
        logger = LoggingUtil.init_logging (__file__)
        slice_size = int (len (articles) / slices)
        offset = slice_size * slice_n
        rest = len(articles) - offset
        if rest > slice_size and rest <= 2 * slice_size:
            slice_size = rest
        the_slice = articles [ offset : offset + slice_size ]
        logger.info ("   -- Guesses (input:{0}, articles:{1}, slice_size:{2}, offset:{3})".
                     format (input_dir, len(articles), slice_size, offset)) 
        articles = sc.parallelize (the_slice, partitions).  \
                   flatMap (lambda p : EquivalentSet.get_article (p)).\
                   sample (False, debug_scale).\
                   cache ()
        return (
            articles.flatMap (Guesses.get_article_guesses).cache (),
            articles.map (lambda a : a.id).collect ()
        )

    @staticmethod
    def mark_binary (binary, is_fact=True):
        binary.fact = is_fact
        return binary

    @staticmethod
    def trace_set (trace_level, label, rdd):
        logger = LoggingUtil.init_logging (__file__)
        if (logger.getEffectiveLevel() > trace_level):
            for g in rdd.collect ():
                print ("  {0}> {1}->{2}".format (label, g[0], g[1]))
    @staticmethod
    def annotate (guesses, facts, pmids):
        trace_level = logging.ERROR
        Guesses.trace_set (trace_level, "Fact", facts)
        Guesses.trace_set (trace_level, "Guess", guesses)
        '''
        We get a list of all facts for every slice of guesses. It's not correct to subtract
        true postivies from *all* facts to calculate false negatives. We should use the set
        of all facts asserted for the affected documents. Filter facts to only those with pmids
        in the target list.
        '''
        relevant_facts = facts.filter (lambda f : f[1].pmid in pmids.value).cache ()
        
        # Things we found in articles that are facts
        true_positive = guesses.                                                  \
                        join (relevant_facts).                                    \
                        map (lambda b : ( b[0], Guesses.mark_binary (b[1][0], is_fact=True) ))
        Guesses.trace_set (trace_level, "TruePos", true_positive)
        
        # Things we found in articles that are not facts
        false_positive = guesses.subtractByKey (true_positive).                  \
                         map (lambda b : ( b[0], Guesses.mark_binary (b[1], is_fact=False) ))
        Guesses.trace_set (trace_level, "FalsePos", false_positive)
        
        # Things that are facts in these articles that we did not find
        false_negative = relevant_facts.subtractByKey (true_positive)
        Guesses.trace_set (trace_level, "FalseNeg", false_negative)
        
        union = true_positive. \
                union (false_positive). \
                union (false_negative)
        return union.map (lambda v: v[1])

tp53_targets = map (lambda x : x.lower (), [
    "AKAP10",
    "AKAP11",
    "AKAP12",
    "AKAP2",
    "AKAP6",
    "CASK",
    "CCNH",
    "CDC42BPA",
    "CDC42BPG",
    "CDK1",
    "CHUK",
    "CKB",
    "CKS1B",
    "CKS2",
    "CLK1",
    "CLK2",
    "FES",
    "FGFR1",
    "FGFR2",
    "FGFR3",
    "ILKAP",
    "INSR",
    "IRAK1",
    "IRAK2",
    "IRAK3",
    "STK16",
    "STK24",
    "STK25",
    "STK26",
    "STK36",
    "STK38",
    "SYK",
    "TAB1",
    "TAB2",
    "TAB3",
    "TAOK1",
    "TBK1",
    "TEK",
    "TJP2",
    "WASF1",
    "WEE1",
    "WNK2",
    "ZAP70",
    "ATM",
    "ATR",
    "BMX",
    "CDK2",
    "CDKN1A",
    "CDKN2A",
    "CDKN2C",
    "CHEK1",
    "CHEK2",
    "CLK3",
    "CSNK1D",
    "CSNK1E",
    "CSNK2A1",
    "CSNK2A2",
    "CSNK2B",
    "GSK3B",
    "HIPK1",
    "HIPK2",
    "HIPK3",
    "ICK",
    "IKBKB",
    "LRRK2",
    "MAPK1",
    "MAPK11",
    "MAPK14",
    "MAPK8",
    "MAPKAPK5",
    "MPP5",
    "MTOR",
    "NUAK1",
    "PBK",
    "PLK1",
    "PRKAB2",
    "PRKCD",
    "PTK2",
    "RB1CC1",
    "SKP1",
    "SRPK1",
    "STK11",
    "STK4",
    "TK1",
    "TP53",
    "VRK1"
])

def is_special_interest (b):
    return b is not None and ( b.L in tp53_targets or b.R in tp53_targets )

def is_training (b):
    result = False
    try:
        result = int(b.pmid) % 2 == 0
    except ValueError:
        print ("(--) pmid: {0}".format (b.pmid))
    return result

def to_csv_row (b):
    return ','.join ([
        b.pmid,
        str(b.date),
        datetime.datetime.fromtimestamp (b.date).strftime ("%d-%m-%Y") if b.date else "",
        '"{0}"'.format (b.L),
        '"{0}"'.format (b.R),
        str(b.paraDist),
        str(b.sentDist),
        str(b.docDist),
        str(b.fact).lower (),
        str(b.date) # time_until_verified
    ])

class Evaluate(object):

    @staticmethod
    def evaluate (conf):
        logger = LoggingUtil.init_logging (__file__)
        logger.info ("Evaluating Chemotext2 output: {0}".format (conf.input_dir))
        sc = SparkUtil.get_spark_context (conf.spark_conf)
        facts = Facts.get_facts (sc, conf.ctd_conf)
        logger.info ("Loaded {0} facts".format (facts.count ()))
        articles = SUtil.get_article_paths (conf.input_dir)
        logger.info ("Listed {0} input files".format (len(articles)))
        for slice_n in range (0, conf.slices):
            output_dir = os.path.join (conf.output_dir, "eval", "annotated", str(slice_n))
            if os.path.exists (output_dir):
                logger.info ("Skipping existing directory {0}".format (output_dir))
            else:
                logger.info ("Loading guesses")
                start = time.time ()
                guesses, article_pmids = Guesses.get_guesses (sc,
                                                              conf.input_dir,
                                                              conf.spark_conf.parts,
                                                              articles,
                                                              conf.slices,
                                                          slice_n)
                elapsed = round (time.time () - start, 2)
                count = guesses.count ()
                logger.info ("Guesses[slice {0}]. {1} binaries in {2} seconds.".format (slice_n, count, elapsed))
                
                pmids = sc.broadcast (article_pmids)
                start = time.time ()
                annotated = Guesses.annotate (guesses, facts, pmids).cache ()
                elapsed = round (time.time () - start, 2)
                count = annotated.count ()
                
                logger.info ("Annotation[slice {0}]. {1} binaries in {2} seconds.".format (slice_n, count, elapsed))
                logger.info ("Generating annotated output for " + output_dir)
                os.makedirs (output_dir)


                train = annotated. \
                        filter (lambda b : b is not None and is_training (b))
                train.count ()

                train = train.map (lambda b : json.dumps (b, cls=BinaryEncoder))
                train.count ()
                train_out_dir = os.path.join (output_dir, 'train')
                train.saveAsTextFile ("file://" + train_out_dir)
                print ("   --> train: {0}".format (train_out_dir))
                
                test  = annotated. \
                        filter (lambda b : b is not None and not is_training (b)).\
                        map (lambda b : json.dumps (b, cls=BinaryEncoder))
                test_out_dir = os.path.join (output_dir, 'test')
                test.saveAsTextFile ("file://" + test_out_dir)
                print ("   --> test: {0}".format (test_out_dir))

                ''' Save CSV '''
                csv_output = "file://{0}".format (os.path.join (output_dir, "csv"))                
                annotated. \
                    map (to_csv_row). \
                    saveAsTextFile (csv_output)
                print ("   --> csv: {0}".format (csv_output))

        ''' Concatenate all csvs into one big one '''
        csv_dirs = os.path.join (conf.output_dir, "eval", "annotated")
        print ("scanning {0}".format (csv_dirs))
        csv_files = []
        for root, dirnames, filenames in os.walk (csv_dirs):
            for filename in fnmatch.filter(filenames, '*part-*'):
                if not "crc" in filename and "csv" in root:
                    file_name = os.path.join(root, filename)
                    csv_files.append (file_name)
        big_csv = os.path.join (conf.output_dir, "eval", "eval.csv")
        
        with open (big_csv, "w") as stream:
            stream.write ("pubmed_id,pubmed_date_unix_epoch_time,pubmed_date_human_readable,binary_a_term,binary_b_term,paragraph_distance,sentence_distance,word_distance,flag_if_valid,time_until_verified\n")
            for f in csv_files:
                with open (f, "r") as in_csv:
                    stream.write (in_csv.read ())

    @staticmethod
    def plot_before (before, conf):
        from chemotext_util import LoggingUtil
        logger = LoggingUtil.init_logging (__file__)

        if before.count () <= 0:
            return
        before = before.reduceByKey (lambda x, y : x + y) 
        
        true_plots_dir = os.path.join (conf.output_dir, "chart")
        true_mentions = before. \
                        mapValues (lambda x : Plot.plot_mentions (x, true_plots_dir, "T")). \
                        filter (lambda x : len(x[1]) > 0)
        logger.info ("true mentions: {0}".format (true_mentions.count ()))

        false_plots_dir = os.path.join (conf.output_dir, "chart", "false")
        false_mentions = before.subtractByKey (true_mentions)
        false_frequency = false_mentions. \
                          mapValues   (lambda x : Plot.false_mention_histogram (x, false_plots_dir))

        false_mentions = false_mentions. \
                         mapValues   (lambda x : Evaluate.plot_mentions (x, false_plots_dir, "F"))

        logger.info ("false mentions: {0}".format (false_mentions.count ()))

    @staticmethod
    def plot_distances (distances):
        if distances.count () <= 0:
            return

        d = distances. \
            map (lambda x : ( x[0], x[1], x[2], x[3] ) ). \
            sample (False, 0.15). \
            toDF().toPandas ()
        d = d.rename (columns = { "_1" : "truth",
                                  "_2" : "doc_dist",
                                  "_3" : "par_dist",
                                  "_4" : "sen_dist" })
        Plot.plot_distances (d)

    @staticmethod
    def plot (conf):
        sc = SparkUtil.get_spark_context (conf.spark_conf)
        conf.output_dir = conf.output_dir.replace ("file:", "")
        conf.output_dir = "file://{0}".format (conf.output_dir)
        sample = conf.sample
        min_year = 1990
        max_year = datetime.datetime.now().year
        years = np.arange (min_year, max_year)
        df = None
        before = sc.parallelize ([], numSlices=conf.spark_conf.parts)
        distances = sc.parallelize ([])

        for slice_n in range (0, conf.slices):

            ''' Read slices '''
            logger.info ("reading slice {0}".format (slice_n))
            files = ",".join ([
                os.path.join (conf.output_dir, "annotated", str(slice_n), "train"),
                os.path.join (conf.output_dir, "annotated", str(slice_n), "test")
            ])
            annotated = sc.textFile (files, minPartitions=conf.spark_conf.parts). \
                        map    (lambda t : BinaryDecoder().decode (t)). \
                        filter (lambda x : not isinstance (x, Fact))

            ''' csv output '''
            csv_output = annotated. \
                        map         (lambda b : ( simplekey(b), [ b ] ))
                        
            ''' Sum distances grouped by fact attribute (T/F) '''
            slice_distances = annotated.map (lambda x : ( x.fact, x.docDist, x.paraDist, x.sentDist) )
            distances = distances.union (slice_distances)
            logger.info ("   Distances count({0}): {1}".format (slice_n, distances.count ()))

            before = before.union (annotated. \
                                   map         (lambda b : ( simplekey(b), [ b ] )). \
                                   reduceByKey (lambda x,y : x + y))
            logger.info ("   Before count({0}): {1}".format (slice_n, distances.count ()))
            break

        Evaluate.plot_distances (distances)
        Evaluate.plot_before (before, conf)

    @staticmethod
    def flatten_dist(truth, dist):
        yield (truth, "doc",  dist[0])
        yield (truth, "sent", dist[1])
        yield (truth, "para", dist[2])
        
def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",   help="Mesos master host")
    parser.add_argument("--name",   help="Spark framework name")
    parser.add_argument("--input",  help="Output directory for a Chemotext2 run.")
    parser.add_argument("--output", help="Output directory for evaluation.")
    parser.add_argument("--slices", help="Number of slices of files to iterate over.")
    parser.add_argument("--parts",  help="Number of partitions for the computation.")
    parser.add_argument("--venv",   help="Path to Python virtual environment to use")
    parser.add_argument("--sample", help="Sample size for plot")
    parser.add_argument("--ctdAB",  help="Path to CTD AB data")
    parser.add_argument("--ctdBC",  help="Path to CTD BC data")
    parser.add_argument("--ctdAC",  help="Path to CTD AC data")
    parser.add_argument("--geneGene",  help="Gene interaction data")
    parser.add_argument("--plot",   help="Plot data", action='store_true')
    args = parser.parse_args()
    conf = EvaluateConf (
        spark_conf = SparkConf (host           = args.host,
                                venv           = args.venv,
                                framework_name = args.name,
                                parts          = int(args.parts)),
        input_dir      = args.input.replace ("file://", ""),
        output_dir     = args.output.replace ("file://", ""),
        slices         = int(args.slices),
        sample         = float(args.sample),
        ctd_conf = CTDConf (
            ctdAB          = args.ctdAB,
            ctdBC          = args.ctdBC,
            ctdAC          = args.ctdAC,
            geneGene       = args.geneGene))

    if args.plot:
        Evaluate.plot (conf)
    else:
        Evaluate.evaluate (conf)

if __name__ == "__main__":
    main()


