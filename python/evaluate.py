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


import matplotlib.pyplot as plt
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

days_per_year = 365
quarter = int(days_per_year / 4) * 24 * 60 * 60
two_years = 2 * days_per_year
two_years_delta = datetime.timedelta (days=two_years)

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
    def false_mention_histogram (binaries):
        import numpy
        import matplotlib.pyplot as plt
        import seaborn as sns
        binaries = sorted (binaries, key=lambda b : b.date)
        dates = map (lambda b : b.date, binaries)
        dates = [ d for d in dates if d is not None ]
        diffs = [abs(v - dates[(i+1)%len(dates)]) for i, v in enumerate(dates)]

        if len(diffs) > 500:
            key = "{0}@{1}".format (binaries[0].L, binaries[0].R)
            output_dir = "/projects/stars/var/chemotext/chart/false"
            outfile = "{0}/false_{1}_{2}.png".format (output_dir, key, len(diffs))
            try:
                if len(diffs) > 0:
                    diffs.insert (0, diffs.pop ())
                plt.clf ()
                g = sns.distplot(diffs,
                                 bins=10,
                                 rug=True,
                                 axlabel="Mention Frequency : {0}".format (key));
                g.axes.set_title('Mention Frequency Distribution', fontsize=14)
                g.set_xlabel("Time",size = 14)
                g.set_ylabel("Probability",size = 14)
                plt.savefig (outfile)
            except:
                traceback.print_exc ()

        return []

    @staticmethod
    def true_mention_histogram (binaries):
        import numpy
        import matplotlib.pyplot as plt
        import seaborn as sns

        result = []
        ctd_date = None
        is_fact = False

        if len(binaries) == 0:
            return result

        key = "{0}@{1}".format (binaries[0].L, binaries[0].R).\
              replace ("\n", "_").\
              replace (" ", "_")

        # get mentions before the CTD note date        
        mentions = []
        for b in binaries:
            if b.date is None:
                continue
            mentions.append (b.date)
            if b.fact:
                is_fact = True
                ctd_date = b.date
                break

        special_interest = is_special_interest (binaries[0])

        # bin these dates for a two year period before the ctd date

        # hack: make ctd date something if is a CTD fact for now.
        if is_fact and ctd_date is None and len(mentions) > 0:
            ctd_date = mentions [ len(mentions) - 1]
            print ("   >>>> Set ctd date to {0}".format (ctd_date))

        if ctd_date is None:
            result = []
            print ("    --- ctd date is NONE")
        elif len(mentions) < 300 and not special_interest:
            print ("    --- mentions < 300")
            result = mentions
        elif len(mentions) > 1:
            print ("    --- plotting mentions")
            result = mentions

            try:
                output_dir = "/projects/stars/var/chemotext/chart"
                with open ("{0}/log.txt".format (output_dir), "a") as stream:
                    stream.write ("{0} {1}\n".format (key, mentions))

                years = 10
                traceback_delta = datetime.timedelta (days=days_per_year * years)
                traceback = datetime.datetime.fromtimestamp (ctd_date) - traceback_delta
                start = int (time.mktime (traceback.timetuple ()))

                outfile = "{0}/before_t_{1}_{2}.png".format (output_dir, key, len(result))
                if special_interest:
                    outfile = "{0}/spec_before_t_{1}_{2}.png".format (output_dir, key, len(result))
                print "generating {0}".format (outfile)

                bins=range (start, ctd_date, quarter)
                plt.clf ()
                g = sns.distplot (result,
                                  bins=bins,
                                  rug=True,
                                  axlabel="Mention Date : {0}".format (key),
                                  label="Mention Density");
                g.axes.set_title('Mention Distribution', fontsize=14)
                g.set_xlabel("Time", size = 14)
                g.set_ylabel("Probability", size = 14)

                plt.savefig (outfile)
            except:
                traceback.print_exc ()

        return result

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
        before = sc.parallelize ([], numSlices=conf.spark_conf.parts)
        distances = sc.parallelize ([])

        for slice_n in range (0, conf.slices):

            ''' Read slices '''
            print "reading slice {0}".format (slice_n)
            files = ",".join ([
                os.path.join (conf.output_dir, "annotated", str(slice_n), "train"),
                os.path.join (conf.output_dir, "annotated", str(slice_n), "test")
            ])
            annotated = sc.textFile (files, minPartitions=conf.spark_conf.parts). \
                        map    (lambda t : BinaryDecoder().decode (t)). \
                        filter (lambda x : not isinstance (x, Fact))

            ''' Sum distances grouped by fact attribute (T/F) '''
            slice_distances = annotated.map (lambda x : ( x.fact, x.docDist, x.paraDist, x.sentDist) )
#                              map    (lambda x : ( x.fact, ( x.docDist, x.paraDist, x.sentDist) )). \
#                              map    (lambda x : ( x[0], x[1][0], x[1][1], x[1][2] ))
            distances = distances.union (slice_distances)
            print ("   Distances count({0}): {1}".format (slice_n, distances.count ()))

            # Find binaries detected before CTD reference article date
            # 1. Get binaries only - no facts.
            # 2. Then partition to find fact/non-fact sets.
            before = before.union (annotated. \
                                   map         (lambda b : ( simplekey(b), [ b ] )). \
                                   reduceByKey (lambda x,y : x + y))
            print ("   Before count({0}): {1}".format (slice_n, distances.count ()))
#            break

            '''
            # Count by year
            countByYear = annotated. \
                map (lambda b: ( datetime.datetime.fromtimestamp (b.date).year if b.date else 0, 1) ). \
                filter (lambda y : isinstance (y[0], int) and y[0] > min_year and y[0] <= max_year ). \
                reduceByKey (lambda x, y: x + y). \
                sample (withReplacement=False, fraction=sample, seed=12345)
            df = df.union (countByYear) if df else countByYear
            df = df.reduceByKey (lambda x, y: x + y)
            '''

        '''
        if distances.count () > 0:

            #https://stanford.edu/~mwaskom/software/seaborn/generated/seaborn.boxplot.html
            # Box and Whiskers plot:
            
            d = distances. \
                map (lambda x : ( x[0], x[1], x[2], x[3] ) ). \
                sample (False, 0.25). \
                toDF().toPandas ()

            print d

            d = d.rename (columns = { "_1" : "truth",
                                      "_2" : "doc_dist",
                                      "_3" : "par_dist",
                                      "_4" : "sen_dist"})

            #            print d
            fig, ax = plt.subplots()
            sns.set_style ("whitegrid")
            ax = sns.boxplot(x="truth", y="doc_dist", data=d, palette="Set3", ax=ax)
            plt.savefig ("doc-whisker.png")

            ax = sns.boxplot(x="truth", y="par_dist", data=d, palette="Set3", ax=ax)
            plt.savefig ("par-whisker.png")

            ax = sns.boxplot(x="truth", y="sen_dist", data=d, palette="Set3", ax=ax)
            plt.savefig ("sen-whisker.png")
        '''

        if before.count () > 0:
            before = before.reduceByKey (lambda x, y : x + y) 
            true_mentions = before. \
                            mapValues (lambda x : Evaluate.true_mention_histogram (x)). \
                            filter (lambda x : len(x[1]) > 0)
            print ("true mentions: {0}".format (true_mentions.count ()))
            false_mentions = before. \
                             subtractByKey (true_mentions). \
                             mapValues   (lambda x : Evaluate.false_mention_histogram (x))
            print ("false mentions: {0}".format (false_mentions.count ()))
        '''
        if False and df is not None and df.count () > 0:
            df = df.toDF().toPandas ()
            df = df.rename (columns = { '_1' : 'year' } )
            df = df.rename (columns = { '_2' : 'relationships' } )
            g = sns.factorplot (data=df, x="year", y="relationships",
                                kind="bar", hue="year",
                                size=10, aspect=1.5, order=years)
            g.set_xticklabels (step=2)
            g.savefig ("figure.png")
        '''

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


