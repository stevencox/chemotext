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
from chemotext_util import MedlineConf
from chemotext_util import LoggingUtil
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil
from chemotext_util import MedlineQuant
from pyspark.sql import SQLContext

logger = LoggingUtil.init_logging (__file__)

f = glob.glob ("/projects/stars/var/chemotext/pubmed/medline/medline16n0736.xml")
print "file----------------------------------------> {0}".format (f)

def parse_line (xml_string):
    root = ET.fromstring(xml_string.encode('utf-8'))

def translate_record (rec, vocab):
    logger = LoggingUtil.init_logging (__file__)
    result = None
    pmid = rec.PMID ["#VALUE"]
    A, B, C = [], [], []
    date = SUtil.parse_month_year_date (rec.Article.Journal.JournalIssue.PubDate.Month,
                                        rec.Article.Journal.JournalIssue.PubDate.Year)
    if date:
        mesh_headings = rec.MeshHeadingList
        if mesh_headings:
            for mesh_heading in mesh_headings:
                for heading in mesh_heading:
                    descriptor_name = heading.DescriptorName
                    val = descriptor_name["#VALUE"].lower ()
                    if val in vocab.value['A']:
                        A.append (val)
                    elif val in vocab.value['B']:
                        B.append (val)
                    elif val in vocab.value['C']:
                        C.append (val)
            result = MedlineQuant ( pmid, date, A, B, C )
    return result

'''
Recreate original chemotext algorithm.
http://www.sciencedirect.com/science/article/pii/S1532046410000419
'''
def translate_record (rec, vocab):
    logger = LoggingUtil.init_logging (__file__)
    result = None
    pmid = rec.PMID ["#VALUE"]
    A, B, C = [], [], []
    date = SUtil.parse_month_year_date (rec.Article.Journal.JournalIssue.PubDate.Month,
                                        rec.Article.Journal.JournalIssue.PubDate.Year)
    if date:
        if rec.ChemicalList:
            for chems in rec.ChemicalList:
                for chem in chems:
                    A.append (chem.NameOfSubstance["#VALUE"])
        if rec.MeshHeadingList:
            for mesh_heading in rec.MeshHeadingList:
                for heading in mesh_heading:
                    descriptor_name = heading.DescriptorName
                    val = descriptor_name["#VALUE"].lower ()                    
                    major_topic = descriptor_name ["@MajorTopicYN"]
                    heading_ui = descriptor_name["@UI"]
                    if heading_ui.startswith ("D0"): # not in the paper
                        C.append (val)
                    if major_topic in [ 'Y', 'N' ]:
                        if heading.QualifierName:
                            qualifier_ui = None
                            if isinstance (heading.QualifierName, list):
                                for qualifier in heading.QualifierName:
                                    qualifier_ui = qualifier["@UI"]
                            else:
                                qualifier_ui = heading.QualifierName ["@UI"]
                            if qualifier_ui:
                                if qualifier_ui[0] in [ 'C', 'F' ]:
                                    C.append (val)
                                elif qualifier_ui.startswith ('Q'): # not in the paper
                                    B.append (val)
                                elif qualifier_ui.startswith ("D12"):
                                    B.append (val)
            print "A=> {0} \nB=> {1} \nC=> {2}".format (A, B, C)
            result = MedlineQuant ( pmid, date, A, B, C )
    return result

def make_triples (mquant):
    triples = []
    for a in mquant.A:
        for b in mquant.B:
            for c in mquant.C:
                triples.append ( [ a, b, c ] )
    return triples

def analyze_medline (conf):
    logger.info ("conf: {0}".format (conf))
    sc = SparkUtil.get_spark_context (conf)
    sqlContext = SQLContext (sc)
    medline = sqlContext.read. \
        format ('com.databricks.spark.xml'). \
        options(rowTag='MedlineCitation'). \
        load(conf.input_xml).rdd

    vocab = { 'A' : [], 'B' : [], 'C' : [] }
#    with open ('vocabulary.json') as stream:
#        vocab = json.loads (stream.read ())
    vocabDistrib = sc.broadcast (vocab)

    terms = medline. \
            map (lambda r : translate_record(r, vocabDistrib)). \
            filter (lambda r : r). \
            collect()
    for mquant in terms:
        triples = make_triples (mquant)
        if len(triples) > 0:
            logger.info ("MeSH Terms: [pmid={0}/date={1}] {2}".format (
                mquant.pmid, mquant.date, triples))

def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",  help="Mesos master host")
    parser.add_argument("--name",  help="Spark framework name")
    parser.add_argument("--input", help="Medline XML.")
    parser.add_argument("--venv",  help="Path to Python virtual environment to use")
    args = parser.parse_args()
    conf = MedlineConf (host           = args.host,
                        venv           = args.venv,
                        framework_name = args.name,
                        input_xml      = args.input)
    analyze_medline (conf)

main ()

