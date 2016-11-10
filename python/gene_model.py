"""
@author: Steve Cox 

Augments vectors of gene relationships with word embedding cosine similarity scores.

Docstring: Google Doc String: http://sphinxcontrib-napoleon.readthedocs.io/en/latest/index.html
"""
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
import time
import traceback
from chemotext_util import SparkConf
from chemotext_util import EvaluateConf
from chemotext_util import LoggingUtil
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil
from datetime import date

logger = LoggingUtil.init_logging (__file__)

def process_models (sc, in_dir, model_dir, out_dir):
    """
    Read each model, a list of vectors, and annotate with word embedding similarity.
    Args:
        sc (SparkContext): Access to the Spark compute fabric.
        in_dir (str): Path to Chemotext data storage including w2v models
        model_dir (str): Path to Gene model map files
        out_dir (str): Path to output
    """
    file_list = glob.glob (os.path.join (model_dir, "model*split*"))
    time_frames = [ "cumulative" ] #, "2month", "year", "2year", "3year" ]
    parameter_space = [ ( f, t ) for f in file_list for t in time_frames ]
    vector_count = sc.parallelize (parameter_space, numSlices=360). \
                   map (lambda c : process_chunk (in_dir = in_dir,
                                                  out_dir = out_dir,
                                                  file_name = c[0],
                                                  time_frame = c[1])). \
                   sum ()
    print ("Processed {0} vectors.".format (vector_count))

def process_chunk (in_dir, out_dir, file_name, time_frame):
    """
    Process a chunk of the model, annotating each vector with term cosine similarity
    derived from a precomputed word embedding model.
    Args:
        in_dir (str): Input directory for computed word embedding models
        out_dir (str): Output directory for annotated model chunks
        file_name (str): Name of the chunk of model to process
        time_frame (str): Model timeframe    
    """
    line_count = 0
    try:
        w2v_dir = os.path.join (in_dir, "w2v", "gensim", time_frame)
        w2v_model_paths = glob.glob (os.path.join (w2v_dir, "pmc-2009*.w2v"))
        model_path = w2v_model_paths [0]
        print ("Loading model: {0}...".format (model_path))
        start = time.time ()
        w2v = gensim.models.Word2Vec.load (model_path)
        print ("Loaded model {0} in {1} seconds.".format (model_path, time.time () - start))
        out_file = os.path.join (out_dir, "{0}.ext".format (os.path.basename (file_name)))
        print ("out file: {0}".format (out_file))
        with open (file_name, "r") as in_stream:
            with open (out_file, "w") as out_stream:
                for line in in_stream:
                    try:
                        vector = line.rstrip().split (",")
                        entity_a = vector [1].lower ()
                        entity_b = vector [2].lower ()
                        if entity_a in w2v.vocab and entity_b in w2v.vocab:
                            similarity = w2v.similarity (entity_a, entity_b)
                            vector.append (str(similarity))
                            out_line = "{0}\n".format (",".join (vector))
                            out_stream.write (out_line)
                            print (" (eA={0}, eB={1}) sim: {2}".format (entity_a, entity_b, similarity))
                            line_count = line_count + 1
                    except:
                        traceback.print_exc ()        
    except:
        traceback.print_exc ()
    return line_count
                                                                             
def main ():
    """
    Annotate model files with word embedding computed cosine similarity.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",   help="Mesos master host")
    parser.add_argument("--name",   help="Spark framework name")
    parser.add_argument("--input",  help="Output directory for a Chemotext2 run.")
    parser.add_argument("--model",  help="Location of model piece files.")
    parser.add_argument("--output", help="Output directory for evaluation.")
    parser.add_argument("--slices", help="Number of separate work chunks.")
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

    print ("Data home: {0}".format (args.input))

    sc = SparkUtil.get_spark_context (conf.spark_conf)
    process_models (sc, conf.input_dir, args.model, conf.output_dir)
 
main ()
