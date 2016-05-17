import argparse
import json
import glob
import HTMLParser
import logging
import nltk
import nltk.data
import os
import string
import socket
import cStringIO
import sys
import traceback
import xml.parsers.expat
from mesh import MeSH
from nltk import word_tokenize
try:
    from lxml import etree as et
except ImportError:
    import xml.etree.cElementTree as et
from pyspark.mllib.feature import Word2Vec
from pyspark.mllib.feature import Word2VecModel
        
def init_logging ():
    FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    return logging.getLogger(__file__)

logger = init_logging ()

def get_spark_context (host, inst=0):
    os.environ['PYSPARK_PYTHON'] = "/projects/stars/venv/bin/python"
    from pyspark import SparkConf, SparkContext
    from StringIO import StringIO
    ip = socket.gethostbyname(socket.gethostname())
    conf = (SparkConf()
            .setMaster("mesos://{0}:5050".format (host))
            .setAppName("ChemoText2:[Python({0})]".format (inst))
            .set ("spark.default.parallelism", 100))
    return SparkContext(conf = conf)

class Cache(object):
    def __init__(self, root="."):
        self.path = os.path.join (root, "cache")
        if not os.path.isdir (self.path):
            os.makedirs (self.path)
    def get (self, name):
        val = None
        obj = os.path.join (self.path, name)
        if os.path.exists (obj):
            with open (obj, 'r') as stream:
                val = json.loads (stream.read ())
        return val
    def put (self, name, obj):
        obj_path = os.path.join (self.path, name)
        with open (obj_path, 'w') as stream:
            stream.write (json.dumps (obj, sort_keys=True, indent=2))

def get_dirs (root, ):
    dirs = []
    subs = glob.glob (root, "*")
    for s in subs:
        if os.path.isdir (s):
            dirs.append (s)
    return dirs

def get_article_dirs (articles, app_home):
    cache = Cache (app_home)
    dirs = cache.get ('pubmed_dirs.json')
    c = 0
    if dirs is None:
        dirs = []
        for root, dirnames, files in os.walk (articles):
            for d in dirnames:
                dirs.append (os.path.join (root, d))
                c = c + 1
                if c > 100:
                    break
        cache.put ('pubmed_dirs.json', dirs)
    return dirs

def get_corpus_sentences (article):
    results = []
    try:
        sentence_detector = nltk.data.load('tokenizers/punkt/english.pickle')
        print "@-article: {0}".format (article)
        with open (article) as stream:
            tree = et.parse (article)
            paragraphs = tree.findall ('.//p')
            if paragraphs is not None:
                for para in paragraphs:
                    try:
                        text = "".join( [ "" if para.text is None else para.text ] +
                                        [ et.tostring (e, encoding='UTF-8', method='text') for e in para.getchildren() ] )
                        text = delete_non_unicode_chars (text)
                        text = text.decode ("utf8", "ignore")
                        results.append ( sentence_detector.tokenize (text.strip ()) )
                    except:
                        traceback.print_exc ()
    except:
        traceback.print_exc ()
    return results

corpus_file_name = "pubmed.corpus"

def generate_corpus (articles, app_home):
    logger.info ("Generating PubMed sentence corpus")
    path = os.path.join (app_home, "cache", corpus_file_name)
    if not os.path.exists (path):
        logger.info ("Generating PubMed sentence corpus (did not already exist)...")
        sentences = articles.flatMap (get_corpus_sentences).cache ()
        corpus = sentences.collect ()
        with open (path, 'w') as stream:
            for sentence in corpus:
                stream.write ("{0}\n".format (sentence))

word2vec_model_path = "pubmed.word2vec"

def get_word2vec_model (sc, app_home):
    logger.info ("Getting word2vec model of corpus")
    w2v_path = os.path.join (app_home, "cache", word2vec_model_path)
    corpus_path = os.path.join (app_home, "cache", corpus_file_name)
    model = None
    logger.info ("w2v model path: {0}".format (w2v_path))
    logger.info ("corpus path: {0}".format (corpus_path))

    if os.path.exists (w2v_path):
        logger.info ("Loading existing word2vec model")
        model = Word2VecModel.load(sc, w2v_path)
    elif os.path.exists (corpus_path):
        logger.info ("Computing new word2vec model from corpus")
        inp = sc.textFile (corpus_path).map(lambda row: row.split(" "))
        word2vec = Word2Vec()
        word2vec.setNumPartitions (100)
        model = word2vec.fit (inp)
        model.save (sc, w2v_path)
    else:
        logger.error ("No existing word2vecd model found and no pubmed corpus file found at {0}.".format (corpus_path))
    return model

extended_mesh = "extended_mesh.json"
def find_synonyms (model, word, radius):
    results = []
    # https://issues.apache.org/jira/browse/SPARK-12016
    try:
        if not " " in word:
            results = model.findSynonyms (word, radius)
    except:
        #traceback.print_exc ()
        #logger.info ("unable to find word {0}".format (word))
        pass
    return results

def augment_vocab (terms, model, kind, radius=3, threshold=0.15):
    added = []
    for word in terms:
        synonyms = find_synonyms (model, word, radius)
        with open ('syn.log', 'w') as s:
            s.write ("{0} -> {1}".format (word, synonyms))
        for synonym, cosine_similarity in synonyms:
            if cosine_similarity < threshold:
                if not synonym in added and synonym.islower() and synonym.isalpha ():
                    print " {0} {1} syn-> {2} cs: {3}".format (kind, word, synonym, cosine_similarity)
                    added.append (synonym)
    terms.extend (added)

def get_synonyms (model, mesh_xml, app_home, radius=3):
    logger.info ("Computing MeSH synonyms using word2vec")
    e_mesh = os.path.join (app_home, "cache", extended_mesh)
    if os.path.exists (e_mesh):
        logger.info ("Loading existing extended MeSH file: {0}".format (e_mesh))
        mesh = MeSH (e_mesh)
    else:
        logger.info ("Executing synonym search (no cached version exists")
        mesh = MeSH (mesh_xml)
        augment_vocab (mesh.proteins, model, 'proteins')
        augment_vocab (mesh.chemicals, model, 'chemicals')
        augment_vocab (mesh.diseases, model, 'diseases')
        mesh.save (e_mesh)
    return mesh

def process_article (item):
    article = item [0]
    mesh_xml = item [1]
    results = []

    sentence_detector = nltk.data.load('tokenizers/punkt/english.pickle')

    def vocab_nlp (words, text, article, doc_pos):
        result = None
        if words and text:
            text_pos = 0
            sentences = sentence_detector.tokenize(text.strip())
            for sentence in sentences:
                for term in words:
                    if sentence.find (term) > -1:
                        tokens = word_tokenize (sentence)
                        for token in tokens:
                            if term == token:
                                result.append ( ( term, doc_pos + text_pos ) )
                            text_pos = text_pos + 1
        return result
        
    try:
        mesh = MeSH (mesh_xml)
        print "@-article: {0}".format (article)
        with open (article) as stream:
            '''
            data = delete_xml_char_refs (stream.read ())
            filtered = cStringIO.StringIO (data)
            tree = et.parse (filtered)
            '''
            a = []
            b = []
            c = []
            tree = et.parse (article)
            paragraphs = tree.findall ('.//p')
            if paragraphs is not None:
                for para in paragraphs:
                    try:
                        text = "".join( [ "" if para.text is None else para.text ] +
                                        [ et.tostring (e, encoding='UTF-8', method='text') for e in para.getchildren() ] )
                        text = delete_non_unicode_chars (text)
                        text = text.decode ("utf8", "ignore")

                        a.extend (vocab_nlp (mesh.chemicals, text, article))
                        b.extend (vocab_nlp (mesh.proteins, text, article))
                        c.extend (vocab_nlp (mesh.diseases, text, article))
                        results.append (article, a, b, c)
                    except:
                        traceback.print_exc ()
    except:
        traceback.print_exc ()
    return results

def merge_lists (a, b): 
    ab = []
    for aa in a:
        for bb in b:
            distance = abs (aa[1] - bb[1])
            if distance < 20:
                ab.append ( ( aa[0], bb[0], 0.8 ) )
            elif distance < 80:
                ab.append ( ( aa[0], bb[0], 0.2 ) )
    return ab

def merge_abc_lists (a, b, c):
    ab = merge_lists (a, b)
    bc = merge_lists (b, c)

def find_relationships (host, app_home, articles, mesh_xml):
    sc = get_spark_context (host)

    logger.info ("Getting Pubmed dirs")
    dirs = get_article_dirs (articles, app_home)
    logger.debug ("dirs: {0}".format (dirs))

    cache = Cache (app_home)
    article_list = cache.get ('articles')
    if article_list is None:
        articles = sc.parallelize (dirs)
        logger.debug ("dirs -> {0}".format (articles.count ()))
        articles = articles.flatMap (lambda d : glob.glob (os.path.join (d, "*.nxml") )).cache ()
        logger.info ("articles -> {0}".format (articles.count ()))
        article_list = articles.collect ()
        cache.put ('articles', article_list)

    articles = sc.parallelize (article_list, 100).sample(False, 0.001).cache ()
    logger.info ("Processing {0} articles".format (articles.count ()))

    generate_corpus (articles, app_home)
    model = get_word2vec_model (sc, app_home)
    get_synonyms (model, mesh_xml, app_home)


    e_mesh = os.path.join (app_home, "cache", extended_mesh)
    articles = articles.map (lambda a : ( a, e_mesh ), 200)

    logger.info ("PARTITIONS================> {0} ".format (articles.getNumPartitions ()))
    logger.info ("intermediate: {0} articles to process".format (articles.count ()))
    articles = articles.flatMap (process_article).cache ()


def delete_non_unicode_chars (text):
    c = 0
    while c < len(text):
        if ord(text[c]) > 128:
            text = text[0:c-1] + text[c+1:len(text)]
        c = c + 1
    return text

def delete_xml_char_refs (text):
    c = 0
    while c < len(text):
        if text[c] == '&' and c < len(text) + 1 and text[c+1] == '#':
            mark = c
            while text[c] != ';':
                c = c + 1
            text = text[0:mark] + text[c+1:len(text)]
        c = c + 1
    return text

def main ():
    print sys.argv
    prog, host, app_home, articles, mesh_xml = sys.argv
    find_relationships (host, app_home, articles, mesh_xml)
    #print process_article (articles, mesh_xml)

main ()

