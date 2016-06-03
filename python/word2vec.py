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
from chemotext_util import Word2VecConf
from chemotext_util import Cache
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil
from chemotext_util import LoggingUtil

logger = LoggingUtil.init_logging (__file__)

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
        #sentence_detector = nltk.data.load('tokenizers/punkt/english.pickle')
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
                        results.append (text)
                        '''
                        text = "".join( [ "" if para.text is None else para.text ] +
                                        [ et.tostring (e, encoding='UTF-8', method='text') for e in para.getchildren() ] )
                        text = delete_non_unicode_chars (text)
                        text = text.decode ("utf8", "ignore")
                        results.append ( sentence_detector.tokenize (text.strip ()) )
                        '''
                    except:
                        print "..bad thing processing corpus..."
                        #traceback.print_exc ()
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
        word2vec = Word2Vec ()
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
    logger = LoggingUtil.init_logging (__file__)
    added = []
    for word in terms:
        synonyms = find_synonyms (model, word, radius)
        with open ('syn.log', 'a') as s:
            s.write ("{0} -> {1}\n".format (word, synonyms))
        for synonym, cosine_similarity in synonyms:
            if cosine_similarity < threshold:
                if not synonym in added and synonym.islower() and synonym.isalpha ():
                    logger.info (" {0} {1} syn-> {2} cs: {3}".format (kind, word, synonym, cosine_similarity))
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

def find_relationships (conf, home): #host, app_home, articles, mesh_xml):
    sc = SparkUtil.get_spark_context (conf)

    logger.info ("Getting Pubmed dirs")
    dirs = get_article_dirs (conf.input_dir, home)
    logger.info ("dirs: {0}".format (dirs))

    cache = Cache (home)
    article_list = cache.get ('articles')
    if article_list is None or len(article_list) == 0:
        articles = sc.parallelize (dirs)
        logger.debug ("dirs -> {0}".format (articles.count ()))
        articles = articles.flatMap (lambda d : glob.glob (os.path.join (d, "*.nxml") )).cache ()
        logger.info ("articles -> {0}".format (articles.count ()))
        article_list = articles.collect ()
        cache.put ('articles', article_list)

    articles = sc.parallelize (article_list, 100).sample(False, 0.001).cache ()
    logger.info ("Processing {0} articles".format (articles.count ()))

    generate_corpus (articles, home)
    model = get_word2vec_model (sc, home)
    get_synonyms (model, conf.mesh, home)

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--master", help="Mesos master host")
    parser.add_argument("--name",   help="Spark framework name")
    parser.add_argument("--input",  help="Article root directory")
    parser.add_argument("--mesh",   help="MeSH XML file")
    parser.add_argument("--home",   help="App home")
    parser.add_argument("--venv",   help="Path to Python virtual environment to use")
    args = parser.parse_args()
    conf = Word2VecConf (args.master, args.venv, args.name, args.input, args.mesh)
    find_relationships (conf, args.home)

main ()

