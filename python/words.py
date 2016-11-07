#--------------------------------------------------------------------------------------------------------------
#-  @author: Steve Cox
#-  
#-  Creates word2vec models of pubmed central full text articles. Uses gensim word2vec.
#-
#-  Google Doc String: http://sphinxcontrib-napoleon.readthedocs.io/en/latest/index.html
#--------------------------------------------------------------------------------------------------------------
from __future__ import division
import argparse
import calendar
import datetime
import glob
import json
import os
import random
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
from datetime import date
from itertools import islice

logger = LoggingUtil.init_logging (__file__)

#--------------------------------------------------------------------------------------------------------------
#-- An iterator for sentences in files in an arbitrary directory.
#--------------------------------------------------------------------------------------------------------------
class ArticleSentenceGenerator(object):
    """
    Scalably generate a set of sentences from preprocessed articles.
    """
    def __init__(self, input_dir, file_list):        
        self.input_dir = input_dir
        with open (file_list) as stream:
            self.files = json.loads (stream.read ())
            print ("   ==> sentence generator. filelist: {0}".format (len (self.files)))
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

class MonthArticleSentenceGenerator (ArticleSentenceGenerator):
    """ Generate sentences by month represented as a year and month"""
    def __init__(self, input_dir, file_list, year, month, depth=1): 
        ArticleSentenceGenerator.__init__ (self, input_dir, file_list)
        self.tags = map (lambda d : self.get_month_tag (d),
                         [ date (year  = year - int(depth/12) + int(d/12),
                                 month = d % 12 + 1,
                                 day   = 1) for d in range (month, month + depth) ])

    def get_month_tag (self, d):
        month_name = calendar.month_name [d.month][0:3]
        return "_{0}_{1}_".format (d.year, month_name)

    def match (self, article_path):
        return any (map (lambda tag : tag in article_path, self.tags))

class YearArticleSentenceGenerator (ArticleSentenceGenerator):
    """ Generate sentences grouped by year """
    def __init__(self, input_dir, file_list, year):
        MonthArticleSentenceGenerator.__init__ (self, input_dir, file_list, year, month=1, depth=12)

class CumulativeYearArticleSentenceGenerator (ArticleSentenceGenerator):
    """ Generate all sentences from the corpus up to the specified year """
    def __init__(self, input_dir, file_list, year):
        ArticleSentenceGenerator.__init__ (self, input_dir, file_list)
        self.year = year
    def match (self, article_path):
        matches = False
        for y in range (1900, self.year + 1):
            matches = "_{0}_".format (y) in article_path
            if matches:
                break
        return matches

class YearSpanArticleSentenceGenerator (ArticleSentenceGenerator):
    """ Generate sentences grouped by a set of years """
    def __init__(self, input_dir, file_list, years):
        ArticleSentenceGenerator.__init__ (self, input_dir, file_list)
        self.years = years
    def match (self, article_path):
        return any (map (lambda y : "_{0}_".format (y) in article_path, self.years) )

def get_model_dir (out_dir, element_type):
    """ Constructs an output model directory path and creates the directory if it does not exist.

    Args:
        out_dir (str): Path to the top level model output directory.
        element_type (str): Type of model to generate.

    Returns:
        str: Path to the output model directory.
    """
    model_dir = os.path.join (out_dir, element_type)
    if not os.path.exists (model_dir):
        os.makedirs (model_dir)
    return model_dir

tag_pattern = {
    "year"       : "type-{0}-{1}.w2v",
    "2year"      : "type-{0}-{1}.w2v",
    "3year"      : "type-{0}-{1}.w2v",
    "month"      : "type-{0}-{1}.w2v",
    "2month"     : "type-{0}-{1}.w2v",
    "cumulative" : "type-{0}.w2v"
}

mode_filename = {
    "word"   : dict(map(lambda (k,v): (k, v.replace ("type", "pmc")), tag_pattern.iteritems())),
    "bigram" : dict(map(lambda (k,v): (k, v.replace ("type", "bigram")), tag_pattern.iteritems()))
}

def get_file_name (out_dir, model_type, mode, data):
    model_dir = get_model_dir (out_dir, model_type)
    pattern = mode_filename [mode][model_type]
    formatted_data = pattern.format (*data) if isinstance (data, list) or isinstance(data, tuple) else pattern.format (data)
    return os.path.join (model_dir, formatted_data)

def should_generate (out_dir, model_type, mode, data):
    """ Determine if the model indicated by the element arg exists or should be generated.

    Args:
        out_dir (str): Path to the top level model output directory.
        element (type,value): Tuple of element type to value.

    Returns:
        bool: Whether or not we should generate the model for this element.
    """
#    if mode == "bigram":
#        return False
    file_name = get_file_name (out_dir, model_type, mode, data)
    return os.path.exists (file_name)

def build_all_models (sc, in_dir, file_list, out_dir):
    """ Builds all word2vec models.

    Generates a list of timeframe identifiers such as year, month, and three-year-span.
    Concatenates these identifiers into a composite list.
    Parallelizes these as a Spark RDD.
    Executes parallel workers to create word2vec models for each timeframe.

    Args:
        sc (SparkContext): An Apache Spark context object.
        in_dir (str): A directory of JSON formatted articles.
        file_list (str): File name of a JSON array of article file names.

    Todo:
        * Partition work to avoid queues of big jobs being serilized.
        * List file list dynamically directly from the input directory.
        * Broadcast the file list so that jobs don't recreate it.
    Raises:
        Error: In principle, anything not caught by the workers.
    """
    years   = [ y for y in range (1900, datetime.datetime.now().year + 1) ]
    months  = [ ( y, m ) for y in years for m in range (1, 12 + 1) ]
    windows = get_windows (years)
    composite_elements = \
                         [ ( "year",       "word",   m ) for m in months  ] + \
                         [ ( "2year",      "word",   m ) for m in months  ] + \
                         [ ( "3year",      "word",   m ) for m in months  ] + \
                         \
                         [ ( "month",      "word",   m ) for m in months  ] + \
                         [ ( "2month",     "word",   m ) for m in months  ] + \
                         \
                         [ ( "cumulative", "word",   y ) for y in years   ]
    composite_elements = composite_elements + map (lambda t : (t[0], "bigram", t[2]), composite_elements)
    composite = filter (lambda e : should_generate (out_dir    = out_dir,
                                                    model_type = e[0],
                                                    mode       = e[1],
                                                    data       = e[2]),
                        composite_elements)
    random.shuffle (composite)
    print ("models: {0}".format (composite))
    print ("Generating {0} w2v models.".format (len(composite)))
    model_count = sc.parallelize (composite, numSlices=360). \
                  map (lambda c : generate_model (file_name = get_file_name (out_dir,
                                                                             model_type=c[0],
                                                                             mode=c[1],
                                                                             data=c[2]),
                                                  sentences = sentence_gen [c[0]](in_dir, file_list, c[2]))). \
                  filter (lambda c : c is not None). \
                  sum ()
    print ("Generated {0} models.".format (model_count))

sentence_gen = {
    "year"       : lambda i,f,d : MonthArticleSentenceGenerator (input_dir=i, file_list=f, year=d[0],   month=d[1], depth=12),
    "2year"      : lambda i,f,d : MonthArticleSentenceGenerator (input_dir=i, file_list=f, year=d[0],   month=d[1], depth=2 * 12),
    "3year"      : lambda i,f,d : MonthArticleSentenceGenerator (input_dir=i, file_list=f, year=d[0],   month=d[1], depth=3 * 12),
    "month"      : lambda i,f,d : MonthArticleSentenceGenerator (input_dir=i, file_list=f, year=d[0],   month=d[1], depth=1),
    "2month"     : lambda i,f,d : MonthArticleSentenceGenerator (input_dir=i, file_list=f, year=d[0],   month=d[1], depth=2),
    "cumulative" : lambda i,f,d : CumulativeYearArticleSentenceGenerator (input_dir=i, file_list=f, year=d)
}

def generate_model (file_name, sentences):
    """ Run Gensim word2vec on a set of sentences and write a model.

    Args:
        w2v_dir (str): Path to the model output directory.
        tag (str):     Time span identifier to incoporate into the output file name.
        sentences (ArticleSentenceGenerator): A iterable of sentences.
        mode (str): Generate for words or bigrams

    Returns:
        int: Number of models generated (0/1)
    """
    result = 0
    try:
        if os.path.exists (file_name):
            print ("  -- skipping existing model: {0}".format (file_name))
        else:
            print ("  ==== *** ==== Generating model file => {0}".format (file_name))
            if "bigram-" in file_name:
                bigram_transformer = gensim.models.Phrases(sentences)
                model = gensim.models.Word2Vec(bigram_transformer[sentences], workers=16)
            else:
                model = gensim.models.Word2Vec (sentences, workers=16)
            if model:
                print ("  ==== *** ==== Writing model file => {0}".format (file_name))
                model.save (file_name)
                result = 1
    except:
        print ("Unable to generate model: {0}".format (file_name))
        traceback.print_exc ()
    return result

def get_windows(seq, n=3):
    """ Returns a sliding window (of width n) over data from the iterable
    Args:
        seq (list): List to generate windows from.
        n (int) : Window size

    Returns:
        generator: Generator of windows: s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   
    """
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result

def main ():
    """
    Tools for running word2vec on the corpus.
    """
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

    root = os.path.dirname (conf.input_dir)
    model_dir = os.path.join (root, "w2v", "gensim")
    sc = SparkUtil.get_spark_context (conf.spark_conf)
    file_list = "/projects/stars/app/chemotext/filelist.json"
    build_all_models (sc, conf.input_dir, file_list, model_dir)
 
main ()
