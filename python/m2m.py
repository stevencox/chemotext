"""
@author: Steve Cox 

Reads and manipulates chem2bio2rdf documents.

Docstring: Google Doc String: http://sphinxcontrib-napoleon.readthedocs.io/en/latest/index.html
"""
from __future__ import division
import argparse
import glob
import json
import os
import pprint
import random
import re
import shutil
import time
import traceback
from chemotext_util import SparkConf
from chemotext_util import SparkUtil
from chemotext_util import EvaluateConf
from chemotext_util import LoggingUtil
from datetime import date
from graphframes import GraphFrame
from rdflib import Graph
from pyspark.sql import Row
from pyspark.sql import SQLContext

logger = LoggingUtil.init_logging (__file__)

def trim_uri (u):
    result = u
    if u.startswith ("<http://"):
        s = u.replace('>', '').split ('/')
        n = len (s)
        result ='/'.join (s[n-2:n])
    return result

def process_graphs (sc, in_dir, partitions):
    """
    Read graph vertices and edges from disk if already saved.
    Otherwise,
    Read chem2bio2rdf drugbank, pubchem, and other N3 RDF models.
    Save vertices and edges to disk.
    
    Traverse the resulting graph - calculating page rank, using
    SQL to get names and PDB links of drugs.

    Args:
        sc (SparkContext): Access to the Spark compute fabric.
        in_dir (str): Path to Chemotext data storage for raw chem2bio2rdf N3 RDF models.
        partitions (int): Number of data partitions.
    """
    sqlContext = SQLContext (sc)

    n3_dirs = [ os.path.join (in_dir, d) for d in [ "drugbank", "pubchem" ] ]

    vertices_path_posix = os.path.join (in_dir, "vertices")
    edges_path_posix = os.path.join (in_dir, "edges")
    vertices_path = "file://{0}".format (vertices_path_posix)
    edges_path = "file://{0}".format (edges_path_posix)

    triples = None
    vertices = None
    edges = None
    g = None

    if os.path.exists (vertices_path_posix) and os.path.exists (edges_path_posix):

        print ("Loading existing vertices: {0}".format (vertices_path))
        start = time.time ()
        vertices = sqlContext.read.parquet (vertices_path).repartition(partitions).cache ()
        print ("Elapsed time for loading precomputed vertices: {0} seconds.".format (
            time.time () - start))

        print ("Loading existing edges: {0}".format (edges_path))
        start = time.time ()
        edges = sqlContext.read.parquet (edges_path).repartition(partitions).cache ()
        print ("Elapsed time for loading precomputed edges: {0} seconds.".format (
            time.time () - start))

    else:
        print ("Constructing vertices and edges from chem2bio2rdf data sources")

        files = [ os.path.join (n3_dir, n3_file) for n3_dir in n3_dirs for n3_file in os.listdir (n3_dir) ]
        triples = sc.parallelize (files, numSlices=partitions). \
                  flatMap (lambda n3_file : process_chunk (n3_file))

        vertices = sqlContext.createDataFrame (
            data = triples.flatMap (lambda d : [
                ( trim_uri (d.S), "attr0" ),
                ( trim_uri (d.O), "attr1" ) ]),
            schema=[ "id", "attr" ]).\
            cache () 
        edges = sqlContext.createDataFrame (
            data = triples.map (lambda d : (
                trim_uri (d.S),
                trim_uri (d.O),
                trim_uri (d.P) )),
            schema = [ "src", "dst", "relationship" ]). \
            cache ()
 
        print ("Triples: {0}".format (triples.count ()))

        if os.path.exists (vertices_path_posix):
            shutil.rmtree (vertices_path_posix)
        if os.path.exists (edges_path_posix):
            shutil.rmtree (edges_path_posix)
        vertices.write.parquet (vertices_path)
        edges.write.parquet (edges_path)

    if vertices is not None and edges is not None:
        start = time.time ()
        vertices.printSchema ()
        edges.printSchema ()
        print ("Elapsed time for print schema: {0} seconds.".format (
            time.time () - start))

        start = time.time ()
        print (" Total of {0} edges.".format (edges.count ()))
        print ("Elapsed time for count edges: {0}".format (time.time () - start))

        g = GraphFrame(vertices, edges)

        print ("Query: Get in-degree of each vertex.")
        start = time.time ()
        g.inDegrees.\
            sort ("inDegree", ascending=False).\
            show(n=3, truncate=False)
        print ("Elapsed time for computing in-degree: {0} seconds.".format (
            time.time () - start))

        start = time.time ()
        print ("Query: Number of protein database relationships: {0}".format (
            g.edges.\
            filter("relationship LIKE '%resource/PDB_ID%' ").\
            count ()))
        print ("Elapsed time for edge filter and count query: {0} seconds.".format (
            time.time () - start))
        
        edges.registerTempTable ("edges")

        sqlContext.sql ("""
           SELECT substring(src, length(src)-7, 6) as Drug,
                  dst as Name
           FROM edges
           WHERE relationship LIKE '%resource/Name%'
        """).show (n=3, truncate=False)

        start = time.time ()
        sqlContext.sql ("""
           SELECT substring(src, length(src)-7, 6) as Compound,
                  dst as SMILES
           FROM edges
           WHERE relationship LIKE '%open%_smiles%'
        """).show (n=3, truncate=False)
        print ("Elapsed time for SQL query: {0} seconds.".format (
            time.time () - start))

        start = time.time ()
        g.find ("()-[Drug2PDB]->()"). \
            filter ("Drug2PDB.relationship LIKE '%/PDB_ID' "). \
            show (n=3, truncate=False)
        print ("Elapsed time for graph motif query: {0} seconds.".format (
            time.time () - start))

    return g

class Proposition (object):
    def __init__(self, S, P, O):
        self.S = S
        self.P = P
        self.O = O

def process_chunk (n3_file):
    """
    Read an N3 RDF model.
    Args:
        n3_file (str): Path to an N3 model.
    """
    result = []
    g = Graph()
    try:
        print ("Loading N3 file: {0}".format (n3_file))
        start = time.time ()
        g.parse (n3_file, format="n3")
        result = [ Proposition (str(subject.n3 ()),
                                str(predicate.n3 ()),
                                str(obj.n3 ())) for subject, predicate, obj in g ]
        print ("Elapsed time parsing {0} is {1} seconds.".format (
            time.time () - start,
            n3_file))
    except:
        print ("ERROR:")
        traceback.print_exc ()
        result = []
    return result

def main ():
    """
    Annotate model files with word embedding computed cosine similarity.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",   help="Mesos master host")
    parser.add_argument("--name",   help="Spark framework name")
    parser.add_argument("--input",  help="Output directory for a Chemotext2 run.")
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
    process_graphs (sc, conf.input_dir, conf.spark_conf.parts)

if __name__ == "__main__":
    main()







'''

http://go.databricks.com/hubfs/notebooks/3-GraphFrames-User-Guide-scala.html

http://graphframes.github.io/user-guide.html#motif-finding


// drug name...
<http://chem2bio2rdf.org/drugbank/resource/drugbank_interaction/5327> <http://chem2bio2rdf.org/drugbank/resource/PDB_ID> <http://chem2bio2rdf.org/uniprot/resource/pdb/1PSD> .
<http://chem2bio2rdf.org/drugbank/resource/drugbank_interaction/5327> <http://chem2bio2rdf.org/drugbank/resource/CID_GENE> <http://chem2bio2rdf.org/chemogenomics/resource/chemogenomics/449328:SERA> .
<http://chem2bio2rdf.org/drugbank/resource/drugbank_interaction/5327> <http://chem2bio2rdf.org/drugbank/resource/Name> "D-3-phosphoglycerate dehydrogenase" .
<http://chem2bio2rdf.org/drugbank/resource/drugbank_interaction/5327> <http://chem2bio2rdf.org/drugbank/resource/gene> <http://chem2bio2rdf.org/uniprot/resource/gene/SERA> .
<http://chem2bio2rdf.org/drugbank/resource/drugbank_interaction/5327> <http://www.w3.org/2000/01/rdf-schema#label> "c2b2r_DrugBankTarget #5327" .
<http://chem2bio2rdf.org/drugbank/resource/drugbank_interaction/5327> <http://chem2bio2rdf.org/drugbank/resource/SwissProt_ID> <http://chem2bio2rdf.org/uniprot/resource/uniprot/P0A9T0> .

// smiles
<http://chem2bio2rdf.org/pubchem/resource/pubchem_compound/5288796> <http://chem2bio2rdf.org/pubchem/resource/openeye_can_smiles> "CNC(=O)CCC(C(=O)[O-])N" .
<http://chem2bio2rdf.org/pubchem/resource/pubchem_compound/5288796> <http://chem2bio2rdf.org/pubchem/resource/openeye_iso_smiles> "CNC(=O)CC[C@@H](C(=O)[O-])N" . 
<http://chem2bio2rdf.org/pubchem/resource/pubchem_compound/5288799> <http://chem2bio2rdf.org/pubchem/resource/openeye_can_smiles> "CC1CN(CC(O1)C)CCCCCCCCNC=O" . 

<http://chem2bio2rdf.org/drugbank/resource/drugbank_drug/DB05246> <http://chem2bio2rdf.org/drugbank/resource/CID> <http://chem2bio2rdf.org/pubchem/resource/pubchem_compound/6476> .
<http://chem2bio2rdf.org/drugbank/resource/drugbank_drug/DB05259> <http://chem2bio2rdf.org/drugbank/resource/CID> <http://chem2bio2rdf.org/pubchem/resource/pubchem_compound/3081884> .
<http://chem2bio2rdf.org/drugbank/resource/drugbank_drug/DB05260> <http://chem2bio2rdf.org/drugbank/resource/CID> <http://chem2bio2rdf.org/pubchem/resource/pubchem_compound/61635> .

'''



