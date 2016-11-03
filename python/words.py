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

class YearArticleSentenceGenerator (ArticleSentenceGenerator):
    """ Generate sentences grouped by year """
    def __init__(self, input_dir, file_list, year):
        ArticleSentenceGenerator.__init__ (self, input_dir, file_list)
        self.year = year
    def match (self, article_path):
        return "_{0}_".format (self.year) in article_path

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

class MonthArticleSentenceGenerator (ArticleSentenceGenerator):
    """ Generate sentences by month represented as a year and month"""
    def __init__(self, input_dir, file_list, year, month): 
        ArticleSentenceGenerator.__init__ (self, input_dir, file_list)
        self.year = year
        self.month_name = calendar.month_name [month][0:3]
        self.text = "_{0}_{1}_".format (year, self.month_name)
    def match (self, article_path):
        return self.text in article_path

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

def should_generate (out_dir, model_type, mode, data):
    """ Determine if the model indicated by the element arg exists or should be generated.

    Args:
        out_dir (str): Path to the top level model output directory.
        element (type,value): Tuple of element type to value.

    Returns:
        bool: Whether or not we should generate the model for this element.
    """
    model_dir = get_model_dir (out_dir, model_type)
    prefix = "pmc" if mode == "word" else "pmc-bigram"
    file_name = None
    if model_type == "year":
        file_name = os.path.join (model_dir, "{0}-{1}.w2v".format (prefix, data))
    elif model_type == "cumulative":
        file_name = os.path.join (model_dir, "{0}-{1}.w2v".format (prefix, data))
    elif model_type == "span":
        file_name = os.path.join (model_dir, "{0}-{1}.w2v".format (prefix, '-'.join (map (lambda i : str(i), data))))
    elif model_type == "month":
        file_name = os.path.join (model_dir, "{0}-{1}-{2}.w2v".format (prefix, data[0], data[1]))
    exists = os.path.exists (file_name)
    print ("exists ({0}) -> {1}".format (exists, file_name))
    return not exists

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
    composite_elements = [ ( "year",       "word",   y ) for y in years   ] + \
                         [ ( "cumulative", "word",   y ) for y in years   ] + \
                         [ ( "month",      "word",   m ) for m in months  ] + \
                         [ ( "span",       "word",   s ) for s in windows ] + \
                         [ ( "year",       "bigram", y ) for y in years   ] + \
                         [ ( "cumulative", "bigram", y ) for y in years   ] + \
                         [ ( "month",      "bigram", m ) for m in months  ] + \
                         [ ( "span",       "bigram", s ) for s in windows ]

    
    composite = filter (lambda e : should_generate (out_dir = out_dir, \
                                                    model_type = e[0], \
                                                    mode = e[1],       \
                                                    data = e[2]),      \
                        composite_elements)

    print ("composite =====> {0}".format (composite))
    random.shuffle (composite)

    model_count = sc.parallelize (composite, numSlices=360). \
                  map (lambda c : generate_model_dynamic (in_dir = in_dir, file_list = file_list, out_dir = out_dir, \
                                                          element_type = c[0],   \
                                                          mode         = c[1],   \
                                                          data         = c[2])). \
                  filter (lambda c : c is not None). \
                  sum ()

    print ("Generated {0} models.".format (model_count))

def generate_model_dynamic (in_dir, file_list, out_dir, element_type, mode, data):
    """ Generate a model based on the input element.

    Args:
        out_dir (str): Path to the top level model output directory.
        element_type (str): Type of model to generate.
        mode (str) : Mode to generate 
        data : data

    Returns:
        int: Number of models generated (0/1)
    """
    result = 0
    try:
        print ("Element -> {0}".format (element_type))
        model_dir = get_model_dir (out_dir, element_type)
        if element_type == "year":
            result = generate_model (model_dir, tag = str(data),
                                     sentences = YearArticleSentenceGenerator (in_dir, file_list, data),
                                     mode = mode)
        elif element_type == "cumulative":
            result = generate_model (model_dir, tag = str(data),
                                     sentences = CumulativeYearArticleSentenceGenerator (in_dir, file_list, data),
                                     mode = mode)
        elif element_type == "span":
            result = generate_model (model_dir, tag = '-'.join (map(lambda y : str(y), data)),
                                     sentences = YearSpanArticleSentenceGenerator (in_dir, file_list, data),
                                     mode = mode)
        elif element_type == "month":
            year = data [0]
            month = data [1]
            result = generate_model (model_dir, tag = "{0}-{1}".format (year, month),
                                     sentences = MonthArticleSentenceGenerator (in_dir, file_list, year, month),
                                     mode = mode)
    except:
        traceback.print_exc ()
    return result

def form_file_name (out_dir, mode, tag):
    prefix = "pmc" if mode == "word" else "pmc-bigram"
    return os.path.join (out_dir, "{0}-{1}.w2v".format (prefix, tag))
    # for f in $(find /projects/stars/var/chemotext/w2v/gensim/ -name "*bigram*" -print); do echo mv $f $(echo $f | sed -e "s,bigram,pmc-bigram,g"); done

def generate_model (w2v_dir, tag, sentences, mode):
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
        file_name = form_file_name (w2v_dir, mode, tag)
        if os.path.exists (file_name):
            print ("  ====> Skipping existing model: {0}".format (file_name))
        else:
            print ("  ==== *** ==== Generating model file => {0}".format (file_name))
            if mode == "word":
                model = gensim.models.Word2Vec (sentences, workers=16)
            elif mode == "bigram":
                bigram_transformer = gensim.models.Phrases(sentences)
                model = gensim.models.Word2Vec(bigram_transformer[sentences], workers=16)
            if model:
                print ("  ==== *** ==== Writing model file => {0}".format (file_name))
                model.save (file_name)
                result = 1
    except:
        print ("Unable to generate model for {0}".format (tag))
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
