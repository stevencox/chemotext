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
from pyspark.sql import SQLContext

import numpy as np
import seaborn as sns

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
        return result
    @staticmethod
    def get_facts (sc, ctdAB, ctdBC, ctdAC):
        sqlContext = SQLContext(sc)
        ab = Facts.load_facts (sqlContext, ctdAB, 0, 3, 10)
        bc = Facts.load_facts (sqlContext, ctdBC, 0, 2, 8)
        ac = Facts.load_facts (sqlContext, ctdAC, 0, 3, 9)
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
                   flatMap (lambda p : EquivalentSet.get_article (p)).cache ()
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

class Evaluate(object):
    @staticmethod
    def is_training (b):
        result = False
        try:
            result = int(b.pmid) % 2 == 0
        except ValueError:
            print ("(--) pmid: {0}".format (b.pmid))
        return result
    @staticmethod
    def evaluate (conf):
        logger = LoggingUtil.init_logging (__file__)
        logger.info ("Evaluating Chemotext2 output: {0}".format (conf.input_dir))
        sc = SparkUtil.get_spark_context (conf.spark_conf)
        logger.info ("Loading facts")
        facts = Facts.get_facts (sc, conf.ctd_conf.ctdAB, conf.ctd_conf.ctdBC, conf.ctd_conf.ctdAC)
        logger.info ("Listing input files")
        articles = SUtil.get_article_paths (conf.input_dir)
        for slice_n in range (0, conf.slices):
            output_dir = os.path.join (conf.output_dir, "annotated", str(slice_n))
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

                #Evaluate.plot (sc, annotated, conf.sample)
                
                train = annotated.filter (lambda b : Evaluate.is_training (b)).\
                        map(lambda b : json.dumps (b, cls=BinaryEncoder))
                train_out_dir = os.path.join (output_dir, 'train')
                train.saveAsTextFile ("file://" + train_out_dir)
                print ("   --> train: {0}".format (train_out_dir))
                
                test  = annotated.filter (lambda b : not Evaluate.is_training (b)).\
                        map(lambda b : json.dumps (b, cls=BinaryEncoder))
                test_out_dir = os.path.join (output_dir, 'test')
                test.saveAsTextFile ("file://" + test_out_dir)
                print ("   --> test: {0}".format (test_out_dir))


    @staticmethod
    def process_before (binaries):
        result = sorted (binaries, key=lambda b: b.date)
        return result

    @staticmethod
    def delete_after (binaries):
        result = sorted (binaries, key=lambda b: b.date)
        fact_pos = -1
        for idx, b in enumerate (binaries):
            if b.fact:
                fact_pos = idx
                break
        return result [0 : fact_pos] if fact_pos > -1 else []

    @staticmethod
    def plot (conf):
        sc = SparkUtil.get_spark_context (conf.spark_conf)
        sqlContext = SQLContext(sc)
        conf.output_dir = conf.output_dir.replace ("file:", "")
        conf.output_dir = "file://{0}".format (conf.output_dir)
        sns.set(style="white")
        sample = conf.sample
        min_year = 1990
        max_year = datetime.datetime.now().year
        years = np.arange (min_year, max_year)
        df = None
        before = sc.parallelize ([])
        distances = sc.parallelize ([])
        # TODO - REMOVE (DEBUG)
        #conf.slices = 1
        for slice_n in range (0, conf.slices):
            print "reading slice {0}".format (slice_n)
            files = ",".join ([
                os.path.join (conf.output_dir, "annotated", str(slice_n), "train") ])
#                os.path.join (conf.output_dir, "annotated", str(slice_n), "test") ])
            annotated = sc.textFile (files). \
                map (lambda t : BinaryDecoder().decode (t))

            # Sum distances grouped by fact attribute (T/F)
            slice_distances = annotated. \
                              filter (lambda x : not isinstance (x, Fact)). \
                              map (lambda x : ( x.fact, ( x.docDist, x.paraDist, x.sentDist) )).\
                              reduceByKey (lambda x,y: ( x[0] + y[0], x[1] + y[1], x[2] + y[2] ))
            distances = distances.union (slice_distances).\
                        reduceByKey (lambda x,y: ( x[0] + y[0], x[1] + y[1], x[2] + y[2] ))
            print "Distances size:{0} elements:{1}".format (distances.count (), distances.collect ())

            # Find binaries detected before CTD reference article date
            early = annotated. \
                           map         (lambda b : ( simplekey(b), [b] )). \
                           reduceByKey (lambda x,y : x + y). \
                           mapValues   (lambda x : Evaluate.process_before (x)). \
                           takeOrdered (10, key=lambda x: -len(x[1]))
            p_by_year = {}
            for k,v in early:
                for b in v:
                    year = datetime.datetime.fromtimestamp (b.date).year if b.date else 0
                    if isinstance (year, int) and year > min_year and year <= max_year:
                        p_by_year [year] = p_by_year[year] + 1 if year in p_by_year else 0
            before = before.union (sc.parallelize([ (year, p_by_year[year]) for year in p_by_year ])).\
                     reduceByKey (lambda x,y: x + y)
            print "before size => {0}".format (before.count ())
            print "before collect() => {0}".format (before.collect ())

            # Count by year
            countByYear = annotated. \
                map (lambda b: ( datetime.datetime.fromtimestamp (b.date).year if b.date else 0, 1) ). \
                filter (lambda y : isinstance (y[0], int) and y[0] > min_year and y[0] <= max_year ). \
                reduceByKey (lambda x, y: x + y). \
                sample (withReplacement=False, fraction=sample, seed=12345)
            df = df.union (countByYear) if df else countByYear
            df = df.reduceByKey (lambda x, y: x + y)

        if distances.count () > 0:
            print "Distances: {0}".format (distances.collect ())
            distances = distances. \
                        flatMap (lambda x: ( ( x[0], "doc",  x[1][0] ),
                                             ( x[0], "para", x[1][1] * 10 ),
                                             ( x[0], "sent", x[1][2] * 10 ))).\
                        toDF().toPandas ().\
                        rename (columns = { "_1" : "truth",
                                            "_2" : "type",
                                            "_3" : "count" })
            g = sns.factorplot(x="truth", y="count", col="type",
                               data=distances, saturation=.5,
                               kind="bar", ci=None, aspect=.6).\
                set_axis_labels("", "Count").\
                set_xticklabels(["False", "True"]).\
                set_titles("{col_name} {col_var}").\
                despine(left=True).\
                savefig ("distances.png")

        if before is not None and before.count () > 0:
            before = before.toDF().toPandas ()
            before = before.rename (columns = { '_1' : 'year' } )
            before = before.rename (columns = { '_2' : 'before' } )
            g = sns.factorplot (data=before, x="year", y="before",
                                kind="bar", hue="year",
                                size=10, aspect=1.5, order=years)
            sns.set_style("ticks")
            g.set_xticklabels (step=2)
            g.savefig ("before.png")

        if df is not None and df.count () > 0:
            df = df.toDF().toPandas ()
            df = df.rename (columns = { '_1' : 'year' } )
            df = df.rename (columns = { '_2' : 'relationships' } )
            g = sns.factorplot (data=df, x="year", y="relationships",
                                kind="bar", hue="year",
                                size=10, aspect=1.5, order=years)
            g.set_xticklabels (step=2)
            g.savefig ("figure.png")

    @staticmethod
    def flatten_dist(truth, dist):
        yield (truth, "doc",  dist[0])
        yield (truth, "sent", dist[1])
        yield (truth, "para", dist[2])
        
def simplekey (b):
    return "{0}@{1}".format (b.L, b.R)

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
            ctdAC          = args.ctdAC))
    if args.plot:
        Evaluate.plot (conf)
    else:
        Evaluate.evaluate (conf)

if __name__ == "__main__":
    main()


