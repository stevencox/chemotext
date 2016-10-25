from __future__ import division
import argparse
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

#--------------------------------------------------------------------------------------------------------------
#-- Generate a Gensim Word2Vec model
#--------------------------------------------------------------------------------------------------------------
def generate_model (w2v_dir, tag, sentences):
    model = None
    count = len (sentences)
    if tag != NULL_WINDOW and count > 0:
        workers = 16 if count > 1000 else 4
        model = gensim.models.Word2Vec (sentences, workers=workers)
        file_name = os.path.join (w2v_dir, "pmc-{0}.w2v".format (tag))
        print ("  ==== *** ==== Writing model file => {0}".format (file_name))
        model.save (file_name)
    return model
def generate_model (w2v_dir, tag, sentences):
    model = None
    if tag != NULL_WINDOW:
        file_name = os.path.join (w2v_dir, "pmc-{0}.w2v".format (tag))
        if os.path.exists (file_name):
            print ("  ====> Skipping existing model: {0}".format (file_name))
        else:
            print ("  ==== *** ==== Generating model file => {0}".format (file_name))
            model = gensim.models.Word2Vec (sentences, workers=16)
            print ("  ==== *** ==== Writing model file => {0}".format (file_name))
            model.save (file_name)
    return model

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

def write_year_sentences_worker (sent_dir, p):
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
                map (lambda p : write_year_sentences_worker (sent_dir, p)). \
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
'''
def generate_span_models0 (articles, model_dir):
    years = sorted (articles.map (lambda a : a.get_year () if a is not None else None).\
                    distinct().collect ())
    three_year_dir = os.path.join (model_dir, "3_year")
    os.makedirs (three_year_dir)

    windows = get_windows (years)
    for window in windows:
        if any (map (lambda w : w is None, window)):
            continue
        print ("Window=> {0}".format (window))
        count = articles.map (lambda a : sentences_by_years (a, window)). \
                reduceByKey (lambda x, y: x + y). \
                map (lambda x : generate_model (three_year_dir, x[0], x[1])).count ()
        print ("Generated {0} 3 year window models: {1}.".format (count, window))
'''

#---
def generate_span_sentences (articles, model_dir, timeframe):
    '''
    Generate a model for the input articles to the output directory.
    :param articles List of parsed article objects
    :type articles: pyspark.rdd.RDD
    :param model_dir Output directory for models
    :type model_dir String
    '''
    years = sorted (articles.map (lambda a : a.get_year () if a is not None else None).\
                    distinct().collect ())

    out_dir = os.path.join (model_dir, timeframe)
    if not os.path.exists (out_dir):
        os.makedirs (out_dir)
    sent_dir = os.path.join (out_dir, "sentences")

    windows = get_windows (years)
    for window in windows:
        if any (map (lambda w : w is None, window)):
            continue
        print ("   --window=> {0}".format (window))
        window_name = '-'.join ( map (lambda a : str(a), window) )
        output_path = os.path.join (sent_dir, window_name)
        if os.path.exists (output_path):
            print ("Sentences for {0} already exist".format (window_name))
        else:
            articles.map (lambda a : sentences_by_years (a, window)). \
                    reduceByKey (lambda x, y: x + y). \
                    map (lambda p : p[1]). \
                    saveAsTextFile ("file://{0}".format (output_path))
            print ("Generated 3 year window sentences: {0}".format (window_name))

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
                    map (lambda p : write_year_sentences_worker (sent_dir, p)). \
                    sum ()
        print ("Wrote {0} sentences".format (sentences))


#---

# GENERIC
def generate_model_worker (sent_dir, out_dir, tag):
    print ("Generating sentences for: {0}".format (out_dir))
    try:
        sentences = SentenceGenerator (sent_dir)
        generate_model (out_dir, tag, sentences)
    except:
        print ("Error generating model: {0}/{1}".format (out_dir, tag))
        traceback.print_exc ()

def generate_models (sc, model_dir, timeframe):
    '''
    Generate a model for the sentence directory to the output directory.
    :param model_dir Output directory for models
    :type model_dir String
    '''
    out_dir = os.path.join (model_dir, timeframe) # out_dir_name
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
                map (lambda p : write_year_sentences_worker (sent_dir, p)). \
                sum ()
    print ("Wrote {0} sentences".format (sentences))



#-------------------------
def build_models (conf):
    root = os.path.dirname (conf.input_dir)
    model_dir = os.path.join (root, "w2v", "gensim")
    limit=5000000
    ct2 = CT2.from_conf (conf, limit=limit)

    timeframe = "1_year"
    generate_year_sentences (ct2.articles, model_dir)
    generate_models (ct2.sc, model_dir, timeframe)

    timeframe = "3_year"
    generate_span_sentences (ct2.articles, model_dir, timeframe)
    generate_models (ct2.sc, model_dir, timeframe)

    timeframe = "month"
    generate_month_sentences (ct2.articles, model_dir)
    generate_models (ct2.sc, model_dir, timeframe)
    





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

'''
https://github.com/joblib/joblib/issues/122


16/10/20 11:37:59 ERROR Executor: Exception in task 0.0 in stage 1.2 (TID 359)
org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/scratch/mesos/slaves/20160711-145321-1946687916-5050-30249-S132114/frameworks/20160711-145321-1946687916-5050-30249-0518/executors/3/runs/4e72994f-4b97-4bdc-97e4-28163646eb6e/spark-1.6.0-bin-hadoop2.6/python/lib/pyspark.zip/pyspark/worker.py", line 111, in main
    process()
  File "/scratch/mesos/slaves/20160711-145321-1946687916-5050-30249-S132114/frameworks/20160711-145321-1946687916-5050-30249-0518/executors/3/runs/4e72994f-4b97-4bdc-97e4-28163646eb6e/spark-1.6.0-bin-hadoop2.6/python/lib/pyspark.zip/pyspark/worker.py", line 106, in process
    serializer.dump_stream(func(split_index, iterator), outfile)
  File "/projects/stars/stack/spark/current/python/lib/pyspark.zip/pyspark/rdd.py", line 2346, in pipeline_func
  File "/projects/stars/stack/spark/current/python/lib/pyspark.zip/pyspark/rdd.py", line 2346, in pipeline_func
  File "/projects/stars/stack/spark/current/python/lib/pyspark.zip/pyspark/rdd.py", line 2346, in pipeline_func
  File "/projects/stars/stack/spark/current/python/lib/pyspark.zip/pyspark/rdd.py", line 2346, in pipeline_func
  File "/projects/stars/stack/spark/current/python/lib/pyspark.zip/pyspark/rdd.py", line 317, in func
  File "/projects/stars/stack/spark/current/python/lib/pyspark.zip/pyspark/rdd.py", line 1784, in _mergeCombiners
  File "/scratch/mesos/slaves/20160711-145321-1946687916-5050-30249-S132114/frameworks/20160711-145321-1946687916-5050-30249-0518/executors/3/runs/4e72994f-4b97-4bdc-97e4-28163646eb6e/spark-1.6.0-bin-hadoop2.6/python/lib/pyspark.zip/pyspark/shuffle.py", line 287, in mergeCombiners
    self._spill()
  File "/scratch/mesos/slaves/20160711-145321-1946687916-5050-30249-S132114/frameworks/20160711-145321-1946687916-5050-30249-0518/executors/3/runs/4e72994f-4b97-4bdc-97e4-28163646eb6e/spark-1.6.0-bin-hadoop2.6/python/lib/pyspark.zip/pyspark/shuffle.py", line 315, in _spill
    self.serializer.dump_stream([(k, v)], streams[h])
  File "/scratch/mesos/slaves/20160711-145321-1946687916-5050-30249-S132114/frameworks/20160711-145321-1946687916-5050-30249-0518/executors/3/runs/4e72994f-4b97-4bdc-97e4-28163646eb6e/spark-1.6.0-bin-hadoop2.6/python/lib/pyspark.zip/pyspark/serializers.py", line 267, in dump_stream
    bytes = self.serializer.dumps(vs)
  File "/scratch/mesos/slaves/20160711-145321-1946687916-5050-30249-S132114/frameworks/20160711-145321-1946687916-5050-30249-0518/executors/3/runs/4e72994f-4b97-4bdc-97e4-28163646eb6e/spark-1.6.0-bin-hadoop2.6/python/lib/pyspark.zip/pyspark/serializers.py", line 487, in dumps
    return zlib.compress(self.serializer.dumps(obj), 1)
OverflowError: size does not fit in an int

at org.apache.spark.api.python.PythonRunner$$anon$1.read(PythonRDD.scala:166)
at org.apache.spark.api.python.PythonRunner$$anon$1.<init>(PythonRDD.scala:207)
at org.apache.spark.api.python.PythonRunner.compute(PythonRDD.scala:125)
at org.apache.spark.api.python.PythonRDD.compute(PythonRDD.scala:70)
at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:306)
at org.apache.spark.rdd.RDD.iterator(RDD.scala:270)
at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:66)
at org.apache.spark.scheduler.Task.run(Task.scala:89)
at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:213)
at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
at java.lang.Thread.run(Thread.java:745)
'''

#    with open ("out.txt", "w") as stream:
#        stream.write ("sentence_by_year keys: {0}".format (sent_by_year.map(lambda sy : sy[0]).collect ()))

