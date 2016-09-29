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
import numpy as np
import random
import re
import shutil
import sys
import socket
import time
import traceback
from chemotext_util import Article
from chemotext_util import Binary
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
from chemotext_util import WordEmbed
from equiv_set import EquivalentSet
from itertools import chain
from operator import add
from plot import Plot
from pyspark.sql import SQLContext
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.mllib.regression import LabeledPoint
                                                

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
        #bb = Facts.load_facts (sqlContext, conf.geneGene, 0, 2, 4, delimiter="\t")
        reference_binaries = [ ab, bc, ac ]
        return sc.union (reference_binaries).flatMap (Facts.expand_ref_binaries).\
            sample(False, debug_scale).\
            cache ()

    @staticmethod
    def get_pathway_facts (sc, conf):
        sqlContext = SQLContext (sc)
        return Facts.load_facts (sqlContext, conf.geneGene, 0, 2, 4, delimiter="\t"). \
            flatMap (Facts.expand_ref_binaries). \
            sample(False, debug_scale). \
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
    def mark_binary (binary, is_fact=True, pmid_date_map=None):
        binary.fact = is_fact
        if pmid_date_map:
            date_map = pmid_date_map.value
            if len(binary.refs) > 0:
                confirmed_dates = []
                for ref in binary.refs:
                    try:
                        if ref in date_map:
                            confirmed_dates.append (date_map [ref])
                    except:
                        traceback.print_exc ()
                earliest = min (confirmed_dates)
                if earliest is not None and binary.date is not None:
                    binary.time_til_validated = earliest - binary.date
        return binary

    @staticmethod
    def trace_set (trace_level, label, rdd):
        logger = LoggingUtil.init_logging (__file__)
        if (logger.getEffectiveLevel() > trace_level):
            for g in rdd.collect ():
                print ("  {0}> {1}->{2}".format (label, g[0], g[1]))
    @staticmethod
    def annotate (guesses, facts, pathway_facts, pmids, pmid_date_map):
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
        # Yields:  ( key -> ( Binary, Fact ) )
        true_positive = guesses.                                                  \
                        join (relevant_facts).                                    \
                        map (lambda b : ( b[0], Guesses.mark_binary (b[1][0], is_fact=True, pmid_date_map=pmid_date_map) ))
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

def load_binary_row (r):
    date = None
    para_dist = None
    sent_dist = None
    doc_dist = None
    try:
        para_dist = int (r.C5), # para
        sent_dist = int (r.C6), # sent
        doc_dist  = int (r.C7), # doc
        date_str = r.C1
        if not date_str == "None":
            date = int (date_str)
    except:
        traceback.print_exc ()
        print r
    return Binary (
        id = 0,
        L = r.C3, # a
        R = r.C4, # b
        paraDist = para_dist,
        sentDist = sent_dist,
        docDist  = doc_dist,
        code = 0,
        fact = r.C8 == "true",
        refs = [ ],
        pmid = r.C0, # pubmed_id,
        leftDocPos=None, rightDocPos=None, dist=None,
        date = date)

def freq_derivative (binaries):
    binaries = filter (lambda b : b is not None, binaries)
    dates = map (lambda b : b.date, binaries)
    for index, b in enumerate (binaries):
        if index < 3: # too few points to try looking for patterns.
            continue
        d_slice = filter (lambda d : d != None, dates [:index])
        d_freq_hist, bins = np.histogram (d_slice)
        b.freq_sec_deriv = sum (np.gradient (np.gradient (d_freq_hist)))
    return binaries

class Evaluate(object):

    @staticmethod
    def evaluate (conf):
        logger = LoggingUtil.init_logging (__file__)
        logger.info ("Evaluating Chemotext2 output: {0}".format (conf.input_dir))
        sc = SparkUtil.get_spark_context (conf.spark_conf)
        facts = Facts.get_facts (sc, conf.ctd_conf)
        pathway_facts = Facts.get_pathway_facts (sc, conf.ctd_conf)
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
                pmid_date_map = None
                pmid_map_path = os.path.join (conf.output_dir, "pmid", "pmid_date_2.json")
                with open (pmid_map_path, "r") as stream:
                    pmid_date_map = json.loads (stream.read ())
                elapsed = round (time.time () - start, 2)

                if pmid_date_map:
                    start = time.time ()
                    pmid_date_map_broadcast = sc.broadcast (pmid_date_map)
                    annotated = Guesses.annotate (guesses, facts, pathway_facts, pmids, pmid_date_map_broadcast).cache ()
                    count = annotated.count ()
                    elapsed = round (time.time () - start, 2)
                
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
            stream.write ("#pubmed_id,pubmed_date_unix_epoch_time,pubmed_date_human_readable,binary_a_term,binary_b_term,paragraph_distance,sentence_distance,word_distance,flag_if_valid,time_until_verified\n")
            for f in csv_files:
                with open (f, "r") as in_csv:
                    for line in in_csv:
                        stream.write(line)

    @staticmethod
    def augment (conf):
        ''' For now, this is a place to extend the data in the CSV with ...
        * Word2Vec derived cosine similarity
        * Second derivative of the frequency of mentions
        '''
        sc = SparkUtil.get_spark_context (conf.spark_conf)
        conf.output_dir = conf.output_dir.replace ("file:", "")
        conf.output_dir = "file://{0}".format (conf.output_dir)

        groups = Evaluate.load_all (sc, conf).                          \
                 map         (lambda b    : ( simplekey(b), [ b ] ) ).  \
                 reduceByKey (lambda x, y : x + y).                     \
                 mapValues   (lambda b    : freq_derivative (b)).       \
                 flatMap     (lambda x    : x[1])
        
        '''
        for k, v in groups.collect ():
            for b in v:
                print (" frequency second derivative {0} => {1}".format (k, b.freq_sec_deriv))
        '''
        groups = groups.coalesce (1)
        output_file = os.path.join (conf.output_dir, "eval", "augment.csv")
        no_proto_output_file = output_file.replace ("file://", "")
        if os.path.exists (no_proto_output_file):
            print ("removing existing output file")
            shutil.rmtree (no_proto_output_file)
        groups.map(to_csv_row).saveAsTextFile (output_file)

    @staticmethod
    def word2vec (conf):
        logger = LoggingUtil.init_logging (__file__)
        logger.info ("Creating Chemotext2 word embeddings from input: {0}".format (conf.input_dir))
        sc = SparkUtil.get_spark_context (conf.spark_conf)
        article_paths = SUtil.get_article_paths (conf.input_dir) #[:20000]
        articles = sc.parallelize (article_paths, conf.spark_conf.parts). \
                   map (lambda p : SUtil.get_article (p))
        logger.info ("Listed {0} input files".format (articles.count ()))
        
        conf.output_dir = conf.output_dir.replace ("file:", "")
        conf.output_dir = "file://{0}/w2v".format (conf.output_dir)
        return WordEmbed (sc, conf.output_dir, articles)

    @staticmethod
    def plot_before (before, output_dir):
        from chemotext_util import LoggingUtil
        logger = LoggingUtil.init_logging (__file__)

        if before.count () <= 0:
            return
        '''
        before = before.reduceByKey (lambda x, y : x + y). \
                 mapValues (lambda x : filter (lambda v : v is not None, x))
        '''
        true_plots_dir = os.path.join (output_dir, "chart")
        print ("-------------> before 0")
        true_mentions = before. \
                        mapValues (lambda x : Plot.plot_true_mentions (x, true_plots_dir)). \
                        filter (lambda x : len(x[1]) > 0)
        logger.info ("Plotted {0} sets of true mentions.".format (true_mentions.count ()))
        print ("-------------> before 1")

        false_plots_dir = os.path.join (output_dir, "chart", "false")
        false_mentions = before.subtractByKey (true_mentions)
        false_frequency = false_mentions. \
                          mapValues   (lambda x : Plot.false_mention_histogram (x, false_plots_dir))

        false_mentions = false_mentions. \
                         mapValues   (lambda x : Plot.plot_false_mentions (x, false_plots_dir))

        logger.info ("false mentions: {0}".format (false_mentions.count ()))
        true_mentions = None
        false_mentions = None
        false_frequency = None

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
        sqlContext = SQLContext(sc) # object unused but defines toDF()
        print ("Original: Output dir: {0}".format (conf.output_dir))
        conf.output_dir = conf.output_dir.replace ("file:", "")
        conf.output_dir = "file://{0}/eval".format (conf.output_dir)

        print ("Output dir: {0}".format (conf.output_dir))

        annotated = Evaluate.load_all (sc, conf) #.sample (False, 0.02)
        before = annotated. \
                 map         (lambda b : ( simplekey(b), [ b ] ) ). \
                 reduceByKey (lambda x,y : x + y). \
                 mapValues   (lambda x : filter (lambda v : v is not None, x))

        print ("Got {0} before values".format (before.count ()))

        plot_path = conf.output_dir.replace ("file://", "")
        print ("Generating plots to plot path: {0}".format (plot_path))
        Evaluate.plot_before (before, plot_path)
        before = None

        distances = annotated.map (lambda x : ( x.fact, x.docDist, x.paraDist, x.sentDist) )
        Evaluate.plot_distances (distances)

    @staticmethod
    def flatten_dist(truth, dist):
        yield (truth, "doc",  dist[0])
        yield (truth, "sent", dist[1])
        yield (truth, "para", dist[2])

    #pubmed_id,pubmed_date_unix_epoch_time,pubmed_date_human_readable,binary_a_term,binary_b_term,paragraph_distance,sentence_distance,word_distance,flag_if_valid,time_until_verified

    @staticmethod
    def load_binary (r):
        date = None
        date_str = r.C1
        try:
            if not date_str == "None":
                date = int (date_str)
        except:
            print ("invalid date: {0}".format (date_str))
        return Binary (
            id = 0,
            L = r.C3, # a
            R = r.C4, # b
            paraDist = int (r.C5), # para
            sentDist = int (r.C6), # sent
            docDist  = int (r.C7), # doc
            code = 0,
            fact = r.C8 == "true",
            refs = [ ],
            pmid = r.C0, # pubmed_id,
            leftDocPos=None, rightDocPos=None, dist=None,
            date = date)
            
    @staticmethod
    def load_all (sc, conf):
        input_file = os.path.join (conf.output_dir, "eval", "eval.csv")
        start = time.time ()
        sqlContext = SQLContext (sc)
        rdd = sqlContext.read.                                  \
            format('com.databricks.spark.csv').                     \
            options(comment='#').                                   \
            options(delimiter=",").                                 \
            load(input_file).rdd.                                   \
            map (lambda r : load_binary_row (r))
        count = rdd.count ()
        elapsed = time.time () - start
        print ("Loaded {0} rows in {1} seconds".format (count, elapsed))
        return rdd

    @staticmethod
    def train_model (conf):
        sc = SparkUtil.get_spark_context (conf.spark_conf)
        conf.output_dir = conf.output_dir.replace ("file:", "")
        conf.output_dir = "file://{0}".format (conf.output_dir)

        labeled = Evaluate.load_all (sc, conf). \
                  map (lambda b : LabeledPoint ( label = 1.0 if b.fact else 0.0,
                                                 features = [ b.paraDist, b.sentDist, b.docDist ] ) )

#        labeled = sc.parallelize ([ (x/10) * 9 for x in random.sample(range(1, 100000000), 30000) ]). \
#                  map (lambda b : LabeledPoint ( 1.0 if b % 2 == 0 else 0.0,
#                                                 [ b % 2, b * 9 ] ) )

        train, test = labeled.randomSplit (weights=[ 0.8, 0.2 ], seed=12345)

        count = train.count ()
        start = time.time ()
        model = LogisticRegressionWithLBFGS.train (train)
        elapsed = time.time () - start
        print ("Trained model on training set of size {0} in {1} seconds".format (count, elapsed))

        start = time.time ()
        model_path = os.path.join (conf.output_dir, "eval", "model")
        file_path = model_path.replace ("file://", "")
        if os.path.isdir (file_path):
            print ("Removing existing model {0}".format (file_path))
            shutil.rmtree (file_path)
        model.save(sc, model_path)
        sameModel = LogisticRegressionModel.load(sc, model_path)
        elapsed = time.time () - start
        print ("Saved and restored model to {0} in {1} seconds".format (model_path, elapsed))


        # Metrics
        labelsAndPreds = test.map (lambda p: (p.label, model.predict (p.features)))
        trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count () / float (train.count())
        print("Training Error => {0}".format (trainErr))

        predictionsAndLabels = labelsAndPreds.map (lambda x : ( float(x[1]), float(x[0]) ))
        metrics = MulticlassMetrics (predictionsAndLabels) 
        print (" --------------> {0}".format (predictionsAndLabels.take (1000)))

        #print (labelsAndPreds.collect ())
        print ("\nMETRICS:")
        try:
            print ("false positive (0.0): {0}".format (metrics.falsePositiveRate(0.0)))
            print ("false positive (1.0): {0}".format (metrics.falsePositiveRate(1.0)))
        except:
            traceback.print_exc ()
        try:
            print ("precision          : {0}".format (metrics.precision(1.0)))
        except:
            traceback.print_exc ()
        try:
            print ("recall             : {0}".format (metrics.recall(1.0)))
        except:
            traceback.print_exc ()
        try:
            print ("fMeasure           : {0}".format (metrics.fMeasure(0.0, 2.0)))
        except:
            traceback.print_exc ()

        print ("confusion matrix   : {0}".format (metrics.confusionMatrix().toArray ()))
        print ("precision          : {0}".format (metrics.precision()))
        print ("recall             : {0}".format (metrics.recall()))
        print ("weighted false pos : {0}".format (metrics.weightedFalsePositiveRate))
        print ("weighted precision : {0}".format (metrics.weightedPrecision))
        print ("weighted recall    : {0}".format (metrics.weightedRecall))
        print ("weight f measure   : {0}".format (metrics.weightedFMeasure()))
        print ("weight f measure 2 : {0}".format (metrics.weightedFMeasure(2.0)))
        print ("")

        # Regression metrics
        predictedAndObserved = test.map (lambda p: (model.predict (p.features) / 1.0 , p.label / 1.0 ) )

        regression_metrics = RegressionMetrics (predictedAndObserved)
        print ("explained variance......: {0}".format (regression_metrics.explainedVariance))
        print ("absolute error..........: {0}".format (regression_metrics.meanAbsoluteError))
        print ("mean squared error......: {0}".format (regression_metrics.meanSquaredError))
        print ("root mean squared error.: {0}".format (regression_metrics.rootMeanSquaredError))
        print ("r2......................: {0}".format (regression_metrics.r2))
        print ("")

        labelsAndPreds = test.map (lambda p: (p.label, sameModel.predict (p.features)))
        testErr = labelsAndPreds.filter (lambda (v, p): v != p).count () / float (test.count ())
        print ("Testing Error => {0}".format (testErr))
        
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
    parser.add_argument("--logreg", help="Model prediction function (logistical regression)", action='store_true')
    parser.add_argument("--w2v",    help="Word2Vec model (create)", action='store_true')
    parser.add_argument("--aug",    help="Add features", action='store_true')
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
    elif args.logreg:
        Evaluate.train_model (conf)
    elif args.w2v:
        Evaluate.word2vec (conf)
    elif args.aug:
        Evaluate.augment (conf)
    else:
        Evaluate.evaluate (conf)

if __name__ == "__main__":
    main()

