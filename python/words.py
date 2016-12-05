"""
@author: Steve Cox 
Creates word2vec models of pubmed central full text articles. Uses gensim word2vec.

Docstring: Google Doc String: http://sphinxcontrib-napoleon.readthedocs.io/en/latest/index.html
"""
from __future__ import division
import argparse
import calendar
import csv
import datetime
import enchant
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
from nltk.tokenize import TreebankWordTokenizer
from nltk.tokenize import sent_tokenize

logger = LoggingUtil.init_logging (__file__)

class GeneSynonyms (object):
    def __init__(self, path):
        self.dict = enchant.Dict("en_US")
        self.map = {}
        with open (path, "rU") as stream:
            data = csv.reader(stream, delimiter=',', quotechar='"', dialect=csv.excel_tab)
            for row in data:
                print ', '.join (row)
                canonical = row[0].lower ()
                if len(row) < 2:
                    print ("len(row) {0}".format (len(row)))
                else:
                    synonyms = row[1]
                    if synonyms.startswith ('"') and synonyms.endswith ('"'):
                        synonyms.replace ('"', '')
                    synonyms = map (lambda s : s.strip ().lower(), synonyms.split (','))
                    for synonym in synonyms:
                        self.map [synonym] = canonical
        #print (json.dumps (self.map, sort_keys=True, indent=2))

    def get_canonical (self, text):
        return self.map[text] if text in self.map else None

    def make_canonical (self, text):
        output = text.lower().split () if isinstance(text, str) else text
        for i, k in enumerate (output):
            if self.dict.check (k):
                continue # don't do english words (for now).
            if k in self.map:
                print ("  --replacing {0}->{1}".format (k, self.map[k]))
                output[i] = self.map[k]
        return output

class ArticleSentenceGenerator(object):
    # http://www.nltk.org/api/nltk.tokenize.html#nltk.tokenize.treebank.TreebankWordTokenizer
    tokenizer = TreebankWordTokenizer()
    """
    Scalably generate a set of sentences from preprocessed articles.
    To be scalable, Gensim needs to read sentences in a stream, rather than attempting to read them all into
    memory at once. To do this, we create an iterable object to read sentences from our data source. In our
    case, that's JSON files created for each parsed article. We create a set of sub-classes to read specific
    subsets of these JSON objects to implement word embedding models based on a variety of timeframes.
    """
    def __init__(self, input_dir, file_list, hgnc):
        self.input_dir = input_dir
        with open (file_list) as stream:
            self.files = json.loads (stream.read ())
        print ("  -- initializing gene synonyms.")
        self.gene_syn = GeneSynonyms (hgnc)

    def __iter__(self):
        for file_name in self.files:
            if self.match (file_name):
                base = "{0}.json".format (os.path.basename (file_name))
                article_path = os.path.join(self.input_dir, base)
                article = SUtil.get_article (article_path)
                if article is not None:
                    # http://www.nltk.org/api/nltk.tokenize.html#module-nltk.tokenize
                    sentence_tokens = [ self.tokenizer.tokenize(s) for s in sent_tokenize (article.raw) ]
                    sentences = [ self.gene_syn.make_canonical (s) for s in sentence_tokens ]
                    #print (" sentences------------> {0}".format (sentences))
                    #sentences = [ self.tokenizer.tokenize(s) for p in article.paragraphs for s in p.sentences ]
                    #sentences = [ s.split(' ') for p in article.paragraphs for s in p.sentences ]
                    for s in sentences:
                        yield s
    def match (self, article):
        return True

class MonthArticleSentenceGenerator (ArticleSentenceGenerator):
    """
    Generate sentences by month represented as a year and month.
    Provide a configurable number of months of history.
    """
    def __init__(self, input_dir, file_list, year, month, hgnc, depth=1): 
        """
        This can be used to flexibly collect N months of sentences ending at
        the specified year and month.

        Args:
            input_dir (str): Input directory of files.
            file_list (str): List of files to use.
            year (int): End year
            month (int): End month
            depth (int): Number of months of history to include. Default: 1
        """
        ArticleSentenceGenerator.__init__ (self, input_dir, file_list, hgnc)
        self.tags = map (lambda d : self.get_month_tag (d),
                         [ date (year  = year - int(depth/12) + int(d/12),
                                 month = d % 12 + 1,
                                 day   = 1) for d in range (month, month + depth) ])

    def get_month_tag (self, d):
        """ Create a tag for matching a PubMed Central filename. """
        month_name = calendar.month_name [d.month][0:3]
        return "_{0}_{1}_".format (d.year, month_name)

    def match (self, article_path):
        """ Determine if an article matches our criteria. """
        return any (map (lambda tag : tag in article_path, self.tags))

class CumulativeYearArticleSentenceGenerator (ArticleSentenceGenerator):
    """ Generate all sentences from the corpus up to the specified year """
    def __init__(self, input_dir, file_list, year, hgnc):
        ArticleSentenceGenerator.__init__ (self, input_dir, file_list, hgnc)
        self.year = year
    def match (self, article_path):
        matches = False
        for y in range (1900, self.year + 1):
            matches = "_{0}_".format (y) in article_path
            if matches:
                break
        return matches

''' File name patterns for supported model types. '''
tag_pattern = {
    "year"       : "type-{0}-{1}.w2v",
    "2year"      : "type-{0}-{1}.w2v",
    "3year"      : "type-{0}-{1}.w2v",
    "month"      : "type-{0}-{1}.w2v",
    "2month"     : "type-{0}-{1}.w2v",
    "cumulative" : "type-{0}.w2v"
}

''' Map of filename patterns for model modes. '''
mode_filename = {
    "word"   : dict(map(lambda (k,v): (k, v.replace ("type", "pmc")), tag_pattern.iteritems())),
    "bigram" : dict(map(lambda (k,v): (k, v.replace ("type", "bigram")), tag_pattern.iteritems()))
}

def get_file_name (out_dir, model_type, mode, data):
    '''
    Get a file name given the model type, mode, and data.

    Args:
        out_dir (str): Path to generate models to.
        model_type (str): A supported model timeframe (must be a key in tag_pattern)
        mode (str): Controls use of word or phrase mode of the word2vec model.
        data (tuple/int): Type dependent representation of timeframe.

    Returns:
        A file name for the model to generate.
    '''
    model_dir = os.path.join (out_dir, model_type)
    if not os.path.exists (model_dir):
        os.makedirs (model_dir)
    pattern = mode_filename [mode][model_type]
    formatted_data = pattern.format (*data) if isinstance (data, list) or isinstance(data, tuple) else pattern.format (data)
    return os.path.join (model_dir, formatted_data)

def build_all_models (sc, in_dir, file_list, out_dir, hgnc):
    """ Builds all word2vec models.

    Generates a list of timeframe identifiers such as year, month, and three-year-span.
    Concatenates these identifiers into a composite list.
    Parallelizes these as a Spark RDD.
    Executes parallel workers to create word2vec models for each timeframe.

    Args:
        sc (SparkContext): An Apache Spark context object.
        in_dir (str): A directory of JSON formatted articles.
        file_list (str): File name of a JSON array of article file names.
        out_dir (str): Directory to generate models to.

    Todo:
        * List file list dynamically directly from the input directory.
        * Broadcast the file list so that jobs don't recreate it.
    Raises:
        Error: In principle, anything not caught by the workers.
    """
    years   = [ y for y in range (1900, datetime.datetime.now().year + 1) ]
    months  = [ ( y, m ) for y in years for m in range (1, 12 + 1) ]
    #windows = get_windows (years)

    ''' Generate parameters for all word level models '''
    composite_elements = [ ( "year",       "word",   m ) for m in months  ] + \
                         [ ( "2year",      "word",   m ) for m in months  ] + \
                         [ ( "3year",      "word",   m ) for m in months  ] + \
                         [ ( "month",      "word",   m ) for m in months  ] + \
                         [ ( "2month",     "word",   m ) for m in months  ] + \
                         [ ( "cumulative", "word",   y ) for y in years   ]

    ''' Extend to include permutations with bigram phrase models. '''
    composite_elements = composite_elements + map (lambda t : (t[0], "bigram", t[2]), composite_elements)

    ''' Remove any parameters corresponding to existing files. '''
    composite = filter (lambda e : not os.path.exists (get_file_name (out_dir, e[0], e[1], e[2])),
                        composite_elements)

    if len(composite) == 0:
        print ("Models corresponding to all parameters already exist at {0}".format (out_dir))
    else:
        print ("Generating {0} w2v models.".format (len(composite)))

        '''
        Shuffle parameters to avoid queus of long running models on one executor.
        Distribute all randomized parameters across all partitions. Invoke generate model for each. 
        Dynamically construct an appropriate streaming sentence iterator for each based on its type.
        '''
        random.shuffle (composite)
        model_count = sc.parallelize (composite, numSlices=360). \
                      map (lambda c : generate_model (file_name = get_file_name (out_dir,
                                                                                 model_type=c[0],
                                                                                 mode=c[1],
                                                                                 data=c[2]),
                                                      sentences = sentence_gen [c[0]](in_dir, file_list, c[2], hgnc))). \
                      filter (lambda c : c is not None). \
                      sum ()
        print ("Generated {0} models.".format (model_count))

''' A map of functions to generate scalable sentence iterators based on type of model to generate. '''
sentence_gen = {
    "year"       : lambda i,f,d,g : MonthArticleSentenceGenerator (input_dir=i, file_list=f, year=d[0],   month=d[1], hgnc=g, depth=1 * 12),
    "2year"      : lambda i,f,d,g : MonthArticleSentenceGenerator (input_dir=i, file_list=f, year=d[0],   month=d[1], hgnc=g, depth=2 * 12),
    "3year"      : lambda i,f,d,g : MonthArticleSentenceGenerator (input_dir=i, file_list=f, year=d[0],   month=d[1], hgnc=g, depth=3 * 12),
    "month"      : lambda i,f,d,g : MonthArticleSentenceGenerator (input_dir=i, file_list=f, year=d[0],   month=d[1], hgnc=g, depth=1),
    "2month"     : lambda i,f,d,g : MonthArticleSentenceGenerator (input_dir=i, file_list=f, year=d[0],   month=d[1], hgnc=g, depth=2),
    "cumulative" : lambda i,f,d,g : CumulativeYearArticleSentenceGenerator (input_dir=i, file_list=f, year=d, hgnc=g)
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

"""
def get_windows(seq, n=3):

    Returns a sliding window (of width n) over data from the iterable
    Args:
        seq (list): List to generate windows from.
        n (int) : Window size

    Returns:
        generator: Generator of windows: s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result
"""

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

#    hgnc = os.path.join (os.path.basedir (conf.spark_conf.otput_dir), "HGNC", "HGNCGeneFamilyDataSet.csv")
    hgnc = os.path.join (conf.output_dir, "HGNC", "HGNCGeneSynonyms.csv")
    print ("hgnc: {0}".format (hgnc))

    build_all_models (sc, conf.input_dir, file_list, model_dir, hgnc)

#    gene_syn = GeneSynonyms ("/projects/stars/var/chemotext/HGNC/HGNCGeneSynonyms.csv")
#    print (gene_syn.get_canonical ("SCAR1"))
#    print (gene_syn.make_canonical ("bob alice fred map2a map2b merlin skip big2"))

main ()
