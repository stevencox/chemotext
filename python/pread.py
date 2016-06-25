from __future__ import division
import argparse
import datetime
import glob
import json
import os
import logging
import sys
import socket
import time
import traceback
import xml.parsers.expat
try:
    from lxml import etree as et
except ImportError:
    import xml.etree.cElementTree as et
from chemotext_util import Article
from chemotext_util import LoggingUtil
from chemotext_util import Medline
from chemotext_util import MedlineConf
from chemotext_util import MedlineQuant
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil
from pyspark.sql import SQLContext

logger = LoggingUtil.init_logging (__file__)

def create_pmid_map (medline):
    start = time.time()
    pmid_df = medline_conn.load_pmid_date_concurrent ()
    elapsed = time.time() - start
    print ("TIME: ------------> {0}".format (elapsed))

def load_pmid_map (medline):
    start = time.time()
    
    elapsed = time.time() - start
    print ("TIME: ------------> {0}".format (elapsed))

def analyze_medline (conf):
    logger.info ("conf: {0}".format (conf))
    sc = SparkUtil.get_spark_context (conf)
    #medline_conn = Medline (sc, conf.input_xml, use_mem_cache=True)
    # create_pmid_map (medline_conn)

    start = time.time()
    sqlContext = SQLContext (sc)
    #p = sqlContext.jsonFile("alluxio://stars-c0.edc.renci.org:19998/chemotext/pmid/pmid_date_2.json").collectAsMap ()
    with open ("/projects/stars/var/chemotext/pmid/pmid_date_2.json", "r") as stream:
        p = json.loads (stream.read ())
    elapsed = time.time() - start
    print ("TIME(load): ------------> {0}".format (elapsed))

    broadcastPMID = sc.broadcast (p)

    spots = sc.parallelize ([ 1, 2, 3 ])
    times = spots.map (lambda s : broadcastPMID.value [str(s)])
    print times.collect ()

    '''
    start = time.time()
    p = dict(map(lambda (k,v): (k, int(v)), p.iteritems()))
    elapsed = time.time() - start
    print ("TIME(convert): ------------> {0}".format (elapsed))

    start = time.time()
    print ("lookup: {0}".format (p["1"]))
    elapsed = time.time() - start
    print ("TIME(lookup): ------------> {0}".format (elapsed))
    '''

def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",  help="Mesos master host")
    parser.add_argument("--name",  help="Spark framework name")
    parser.add_argument("--input", help="Medline XML directory.")
    parser.add_argument("--venv",  help="Path to Python virtual environment to use")
    args = parser.parse_args()
    conf = MedlineConf (host           = args.host,
                        venv           = args.venv,
                        framework_name = args.name,
                        input_xml      = args.input)
    analyze_medline (conf)

if __name__ == "__main__":
    main ()

