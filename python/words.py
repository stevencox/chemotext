from __future__ import division
import argparse
import calendar
import datetime
import glob
import json
import os
#--------------------------------------------------------------------------------------------------------------
#-- @author: Steve Cox
#-- 
#-- Creates word2vec models of pubmed central full text articles. Uses gensim word2vec.
#--
#--------------------------------------------------------------------------------------------------------------
import gensim
import logging
import traceback
from chemotext_util import Article
from chemotext_util import CT2
from chemotext_util import SparkConf
from chemotext_util import EvaluateConf
from chemotext_util import LoggingUtil
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil
from itertools import islice

logger = LoggingUtil.init_logging (__file__)

#--------------------------------------------------------------------------------------------------------------
#-- An iterator for sentences in files in an arbitrary directory.
#--------------------------------------------------------------------------------------------------------------
class SentenceGenerator(object):
    def __init__(self, dir_name):
        self.dir_name = dir_name
    def __iter__(self):
        for fname in os.listdir(self.dir_name):
            for line in open(os.path.join(self.dir_name, fname)):
                print ("generate sentence... {0}".format (line))
                yield line.split()

class ArticleSentenceGenerator(object):
    def __init__(self, input_dir, file_list):
        self.input_dir = input_dir
        with open (file_list) as stream:
            self.files = json.loads (stream.read ())
            print ("================> sentence generator. filelist: {0}".format (len (self.files)))
    def fix_path (self, path):
        base = "{0}.json".format (os.path.basename (path))
        return os.path.join(self.input_dir, base)
    def __iter__(self):
        for file_name in self.files:
            if self.match (file_name):
                article_path = self.fix_path (file_name)
                article = SUtil.get_article (article_path)
                if article is not None:
                    sentences = [ s.split(' ') for p in article.paragraphs for s in p.sentences ]
                    for s in sentences:
                        yield s
    def match (self, article):
        return True

class YearArticleSentenceGenerator (ArticleSentenceGenerator):
    def __init__(self, input_dir, file_list, year):
        ArticleSentenceGenerator.__init__ (self, input_dir, file_list)
        self.year = year
    def match (self, article_path):
        return "_{0}_".format (self.year) in article_path

class YearSpanArticleSentenceGenerator (ArticleSentenceGenerator):
    def __init__(self, input_dir, file_list, years):
        ArticleSentenceGenerator.__init__ (self, input_dir, file_list)
        self.years = years
    def match (self, article_path):
        return any (map (lambda y : "_{0}_".format (y) in article_path, self.years) )

class MonthArticleSentenceGenerator (ArticleSentenceGenerator):
    def __init__(self, input_dir, file_list, year, month): 
        ArticleSentenceGenerator.__init__ (self, input_dir, file_list)
        self.year = year
        self.month_name = calendar.month_name [month][0:3]
        self.text = "_{0}_{1}_".format (year, self.month_name)
    def match (self, article_path):
        return self.text in article_path

def generate_year_model (in_dir, file_list, out_dir, year):
    result = 0
    try:
        sentences = YearArticleSentenceGenerator (in_dir, file_list, year)
        result = generate_model (out_dir, str(year), sentences)
    except:
        traceback.print_exc ()
    return result
def generate_span_model (in_dir, file_list, out_dir, years):
    result = 0
    try:
        sentences = YearSpanArticleSentenceGenerator (in_dir, file_list, years)
        result = generate_model (out_dir, '-'.join (map(lambda y : str(y), years)), sentences)
    except:
        traceback.print_exc ()
    return result
def generate_month_model (in_dir, file_list, out_dir, year, month):
    result = 0
    try:
        sentences = MonthArticleSentenceGenerator (in_dir, file_list, year, month)
        result = generate_model (out_dir, "{0}-{1}".format (year, month), sentences)
    except:
        traceback.print_exc ()
    return result

def build_year_models (sc, in_dir, file_list, out_dir):
    '''
    years = []
    for y in range (1900, datetime.datetime.now().year):
        years.append (y)

    months = [ ]
    for y in years:
        for m in range (1, 12):
            months.append ( ( y, m ) )
    '''

    years = [ y for y in range (1900, datetime.datetime.now().year) ]
    months = [ ( y, m ) for y in years for m in range (1, 12) ]
    windows = get_windows (years)
    composite = [ ( "year", y ) for y in years ] + \
                [ ( "month", m ) for m in months ] + \
                [ ( "span", s ) for s in windows ]
    '''
    composite = []
    for y in years:
        composite.append ( ( "year", y ) )
    for w in windows:
        composite.append ( ( "span", w ) )
    for m in months:
        composite.append ( ( "month", m ) )
    '''
    models = sc.parallelize (composite, numSlices=200). \
             map (lambda c : generate_model_dynamic (in_dir, file_list, out_dir, c)). \
             filter (lambda c : c is not None).\
             sum ()

def get_model_dir (out_dir, element_type):
    model_dir = os.path.join (out_dir, element_type)
    if not os.path.exists (model_dir):
        os.makedirs (model_dir)
    return model_dir

def generate_model_dynamic (in_dir, file_list, out_dir, element):
    result = 0
    element_type = element [0]
    print ("Element -> {0}".format (element_type))
    model_dir = get_model_dir (out_dir, element_type)
    if element_type == "year":
        result = generate_year_model (in_dir, file_list, model_dir, element[1])
    elif element_type == "span":
        result = generate_span_model (in_dir, file_list, model_dir, element[1])
    elif element_type == "month":
        pair = element [1]
        result = generate_month_model (in_dir, file_list, model_dir, pair[0], pair[1])
    return result

#--------------------------------------------------------------------------------------------------------------
#-- Generate a Gensim Word2Vec model
#--------------------------------------------------------------------------------------------------------------
def generate_model (w2v_dir, tag, sentences):
    result = 0
    if tag != NULL_WINDOW:
        try:
            file_name = os.path.join (w2v_dir, "pmc-{0}.w2v".format (tag))
            if os.path.exists (file_name):
                print ("  ====> Skipping existing model: {0}".format (file_name))
            else:
                print ("  ==== *** ==== Generating model file => {0}".format (file_name))
                model = gensim.models.Word2Vec (sentences, workers=16)
                print ("  ==== *** ==== Writing model file => {0}".format (file_name))
                model.save (file_name)
                result = 1
        except:
            print ("Unable to generate model for {0}".format (tag))
            traceback.print_exc ()
    return result

#--------------------------------------------------------------------------------------------------------------
#-- Year models
#--------------------------------------------------------------------------------------------------------------
def sentences_by_year (a):
    result = ( NULL_WINDOW, [] )
    year = a.get_year () if a is not None else None
    if year and year > MIN_PUB_YEAR:
        words = [ s.split(' ') for p in a.paragraphs for s in p.sentences ]
        result = ( year, words )
    return result

def write_sentence_slice (output_path, index, v):
    out_file = os.path.join (output_path, "0")
    print ("Saving sentences {0}".format (out_file))
    with open (out_file, "w") as stream:
        for sentence in v:
            stream.write ("{0}\n".format (' '.join (sentence)))

def generate_year_sentences (articles, model_dir):
    sent_dir = os.path.join (model_dir, "1_year", "sentences")
    if not os.path.exists (sent_dir):
        os.makedirs (sent_dir)
    sentences = articles.map (lambda a : sentences_by_year (a))
    counts = sentences.map (lambda t : len(t[1])).sum ()
    print ("num sentences: {0}".format (counts))
    sentences = sentences.\
                coalesce(NUM_CLUSTER_NODES).\
                reduceByKey (lambda x, y: x + y). \
                map (lambda p : write_sentences_worker (sent_dir, p)). \
                sum ()
    print ("Wrote {0} sentences".format (sentences))

#--------------------------------------------------------------------------------------------------------------
#-- Windowed models
#--------------------------------------------------------------------------------------------------------------
NULL_WINDOW = 0
MIN_PUB_YEAR = 1800
NUM_CLUSTER_NODES=6

def get_windows(seq, n=3):
    "Returns a sliding window (of width n) over data from the iterable"
    "   s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   "
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result

def sentences_by_years (a, window):
    result = ( NULL_WINDOW, [] )
    year = a.get_year () if a is not None else None
    if len(window) > 0 and year in window:
        words = [ s.split(' ') for p in a.paragraphs for s in p.sentences ]
        items = map (lambda y : str(y), window)
        result = ( '-'.join (items), words )
    return result

def generate_span_sentences (articles, model_dir, timeframe):
    years = sorted (articles.map (lambda a : a.get_year () if a is not None else None).distinct().collect ())
    sent_dir = os.path.join (model_dir, "3_year", "sentences")
    if not os.path.exists (sent_dir):
        os.makedirs (sent_dir)
    windows = get_windows (years)
    for window in windows:
        if any (map (lambda w : w is None, window)):
            continue
        print ("   --window=> {0}".format (window))
        sentences = articles.map (lambda a : sentences_by_years (a, window))
        counts = sentences.map (lambda t : len(t[1])).sum ()
        print ("      num sentences: {0}".format (counts))
        sentences = sentences.\
                    coalesce(NUM_CLUSTER_NODES).\
                    reduceByKey (lambda x, y: x + y). \
                    map (lambda p : write_sentences_worker (sent_dir, p)). \
                    sum ()
        print ("Wrote {0} sentences".format (sentences))

#---
# GENERIC

def write_sentences_worker (sent_dir, p):
    k = p[0]
    v = p[1]
    result = 0
    if k is not None:
        output_path = os.path.join (sent_dir, str(k))
        if os.path.exists (output_path):
            print ("   skipping output {0}. already exists.".format (output_path))
        else:
            os.makedirs (output_path)
            count = len (v)
            slice_size = 1000
            slice_n = int(count / slice_size)
            remainder = count - slice_size
            for x in range (0, slice_n):
                write_sentence_slice (output_path, x, v[x * slice_size : slice_size])
            write_sentence_slice (output_path, slice_n + 1, v[ slice_n * slice_size : len(v) - 1 ])
            result = count
    return result

def generate_model_worker (sent_dir, out_dir, tag):
    print ("Generating sentences for: {0}".format (out_dir))
    try:
        sentences = SentenceGenerator (sent_dir)
        generate_model (out_dir, tag, sentences)
    except:
        print ("Error generating model: {0}/{1}".format (out_dir, tag))
        traceback.print_exc ()

def generate_models (sc, model_dir, timeframe):
    out_dir = os.path.join (model_dir, timeframe)
    if not os.path.exists (out_dir):
        os.makedirs (out_dir)
    sent_dir = os.path.join (out_dir, "sentences")
    tags = os.listdir (sent_dir)
    partitions = 70
    dirs = sc.parallelize (tags, partitions). \
           map (lambda t : generate_model_worker (os.path.join (sent_dir, t),
                                                  out_dir,
                                                  t)).count ()

#--------------------------------------------------------------------------------------------------------------
#-- Month models
#--------------------------------------------------------------------------------------------------------------
def sentences_by_month (a):
    result = ( NULL_WINDOW, [] )
    key = a.get_month_year () if a is not None else None
    if key is not None:
        result = ( key, [ s.split(' ') for p in a.paragraphs for s in p.sentences ] )
    return result

# make month sentences...
def generate_month_sentences (articles, model_dir):
    sent_dir = os.path.join (model_dir, "month", "sentences")
    if not os.path.exists (sent_dir):
        os.makedirs (sent_dir)

    sentences = articles.map (lambda a : sentences_by_month (a))
    counts = sentences.map (lambda t : len(t[1])).sum ()
    print ("num sentences: {0}".format (counts))

    sentences = sentences.\
                coalesce(NUM_CLUSTER_NODES).\
                reduceByKey (lambda x, y: x + y). \
                map (lambda p : write_sentences_worker (sent_dir, p)). \
                sum ()
    print ("Wrote {0} sentences".format (sentences))

#-------------------------
def build_models (conf):
    root = os.path.dirname (conf.input_dir)
    model_dir = os.path.join (root, "w2v", "gensim")
    limit=500000#0
#    ct2 = CT2.from_conf (conf, limit=limit)

    '''
    timeframe = "1_year"
    generate_year_sentences (ct2.articles, model_dir)
    generate_models (ct2.sc, model_dir, timeframe)

    timeframe = "3_year"
    generate_span_sentences (ct2.articles, model_dir, timeframe)
    generate_models (ct2.sc, model_dir, timeframe)

    timeframe = "month"
    generate_month_sentences (ct2.articles, model_dir)
    generate_models (ct2.sc, model_dir, timeframe)
    '''

    sc = SparkUtil.get_spark_context (conf.spark_conf)
    file_list = "/projects/stars/app/chemotext/filelist.json"
    build_year_models (sc, conf.input_dir, file_list, model_dir)

def main ():
    '''
    Tools for running word2vec on the corpus.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",   help="Mesos master host")
    parser.add_argument("--name",   help="Spark framework name")
    parser.add_argument("--input",  help="Output directory for a Chemotext2 run.")
    parser.add_argument("--output", help="Output directory for evaluation.")
    parser.add_argument("--slices", help="Number of slices of files to iterate over.")
    parser.add_argument("--parts",  help="Number of partitions for the computation.")
    parser.add_argument("--venv",   help="Path to Python virtual environment to use")
    args = parser.parse_args()
    conf = EvaluateConf (
        spark_conf = SparkConf (host           = args.host,
                                venv           = args.venv,
                                framework_name = args.name,
                                parts          = int(args.parts)),
        input_dir      = args.input.replace ("file://", ""),
        output_dir     = args.output.replace ("file://", ""),
        slices         = int(args.slices))
    build_models (conf)
 
main ()
