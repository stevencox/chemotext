import argparse
import glob
import json
import os
import logging
import sys
import time
import xml.parsers.expat
try:
    from lxml import etree as et
except ImportError:
    import xml.etree.cElementTree as et
from chemotext_util import LoggingUtil
from chemotext_util import Medline
from chemotext_util import MedlineConf
from chemotext_util import MedlineQuant
from chemotext_util import SparkUtil

logger = LoggingUtil.init_logging (__file__)

def make_quant (citation):
    ''' Original chemotext algorithm: http://www.sciencedirect.com/science/article/pii/S1532046410000419 '''
    logger = LoggingUtil.init_logging (__file__)
    pmid = citation.pmid
    date = citation.date
    B, C = [], []
    for heading in citation.headings:
        if heading.ui.startswith ("D0"): # not in the paper
            C.append (heading.descriptor_name)
        if heading.major_topic in [ 'Y', 'N' ]:
            for qualifier in heading.qualifiers:
                if qualifier.ui[0] in [ 'C', 'F' ]:
                    C.append (val)
                elif qualifier.ui.startswith ('Q'): # not in the paper
                    B.append (val)
                elif qualifier.ui.startswith ("D12"):
                    B.append (val)
    return MedlineQuant ( citation.pmid, citation.date, citation.chemicals, B, C )

def make_triples (mquant):
    triples = []
    for a in mquant.A:
        for b in mquant.B:
            for c in mquant.C:
                triples.append ( [ a, b, c ] )
    return triples

def analyze_medline (conf):
    logger.info ("Conf: {0}".format (conf))
    sc = SparkUtil.get_spark_context (conf)
    medline_ctx = Medline (sc, conf, use_mem_cache=True)
    if conf.gen_pmid_map:
        start = time.time()
        pmid_df = medline_ctx.generate_pmid_date_map ()
        elapsed = time.time() - start
        logger.info ("Gen PMID->Date Map:Time elapsed:--> {0}".format (elapsed))
    else:
        logger.info ("Executing Chemotext1 Algorithm on Medline data")
        triples = medline_ctx.parse_citations (). \
                  map (lambda c : make_quant (c)). \
                  flatMap (lambda q : make_triples (q))
        with open (os.path.join (conf.data_root, "ct1.out"), "w") as stream:
            stream.write (json.dumps (triples.collect (), indent=2))
            stream.close ()

def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", help="Mesos master host")
    parser.add_argument("--name", help="Spark framework name")
    parser.add_argument("--data", help="Chemotext data root.")    
    parser.add_argument("--venv", help="Path to Python virtual environment to use")
    parser.add_argument('--pmid', action='store_true', default=False)
    args = parser.parse_args()

    analyze_medline (
        MedlineConf (host           = args.host,
                     venv           = args.venv,
                     framework_name = args.name,
                     data_root      = args.data.replace ("file://", ""),
                     gen_pmid_map   = args.pmid ))
    
if __name__ == "__main__":
    main ()

