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
import shutil
import traceback
from chemotext_util import SparkConf
from chemotext_util import SparkUtil
from chemotext_util import EvaluateConf
from chemotext_util import LoggingUtil
from datetime import date
from graphframes import GraphFrame
from rdflib import Graph
from pyspark.sql import SQLContext

logger = LoggingUtil.init_logging (__file__)

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
    n3_dirs = [
        os.path.join (in_dir, "drugbank"),
        os.path.join (in_dir, "pubchem")
    ]
    sqlContext = SQLContext (sc)
    vertices_path_posix = os.path.join (in_dir, "vertices")
    edges_path_posix = os.path.join (in_dir, "edges")
    vertices_path = "file://{0}".format (vertices_path_posix)
    edges_path = "file://{0}".format (edges_path_posix)

    triples = None
    vertices = None
    edges = None
    g = None

    if os.path.exists (vertices_path_posix) and os.path.exists (edges_path_posix):
        print ("Loading existing vertices/edges: [{0}, {1}]".format (vertices_path, edges_path))

        vertices = sqlContext.read.parquet (vertices_path)
        edges = sqlContext.read.parquet (edges_path)
    else:
        print ("Constructing vertices and edges from chem2bio2rdf data sources")

        triples = sc.parallelize (n3_dirs, numSlices=partitions). \
                  flatMap (lambda d : map (lambda f : os.path.join (d, f), os.listdir (d))). \
                  repartition (numPartitions=partitions). \
                  flatMap (lambda n3_file : process_chunk (n3_file))

        vertices = sqlContext.createDataFrame (triples.flatMap (lambda d : [ ( d.S, "t1" ), ( d.O, "t2" ) ]),
                                               schema=[ "id", "term" ])
        vertices.printSchema ()
        print ("v: {0}".format (vertices.take (10)))

        edges = sqlContext.createDataFrame (triples.map (lambda d : (d.S, d.O, d.P)),
                                            schema=[ "src", "dst", "relationship" ])
        edges.printSchema ()
        print ("e: {0}".format (edges.take (10)))

        shutil.rmtree (vertices_path_posix)
        shutil.rmtree (edges_path_posix)
        vertices.write.parquet (vertices_path)
        edges.write.parquet (edges_path)

    if vertices is not None and edges is not None:
        g = GraphFrame(vertices, edges)

        print ("Query: Get in-degree of each vertex.")
        g.inDegrees.sort ("inDegree", ascending=False).show(truncate=False)
        


        print ("Query the number of protein database relationships")
        pdb_rels = g.edges.filter("relationship LIKE '%resource/PDB_ID>%' ").count()
        print ("PDB rels: {0}".format (pdb_rels))

        '''
        print ("Run PageRank algorithm, and show results.")
        results = g.pageRank(resetProbability=0.01, maxIter=20)
        results.vertices.select("id", "pagerank").show(truncate=False)
        '''

        edges.registerTempTable ("edges")

        print ("Query resource/Name:")
        sqlContext.sql ("SELECT src, dst FROM edges WHERE relationship LIKE '%resource/Name>%' ").show (truncate=False)

#        print ("Query resource/PDB_ID:")
#        sqlContext.sql ("SELECT src, dst FROM edges WHERE relationship LIKE '%resource/PDB_ID>%' ").show (truncate=False)

        print ("Query resource/openeye_%_smiles%:")
        sqlContext.sql (" SELECT src, dst FROM edges WHERE relationship LIKE '%openeye_%_smiles>%' ").show (truncate=False)

        print ("Query resource names")
        g.find("(a)-[e]->(b)").filter ("e.relationship = '<http://chem2bio2rdf.org/drugbank/resource/PDB_ID>' ").show (truncate=False)


    return g

'''

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



class Proposition (object):
    def __init__(self, S, P, O):
        self.S = S
        self.P = P
        self.O = O
class Drugbank(Proposition):
    pass
class PubChem(Proposition):
    pass
    
def process_chunk (n3_file):
    """
    Read an N3 RDF model.
    Args:
        n3_file (str): Path to an N3 model.
    """
    result = []
    g = Graph()
    try:
        g.parse (n3_file, format="n3")
        result = [ Proposition (str(subject.n3 ()),
                                str(predicate.n3 ()),
                                str(obj.n3 ())) for subject, predicate, obj in g ]
    except:
        print ("ERROR:")
        traceback.print_exc ()
        result = []
    return result

#    for subject, predicate, obj in g:
#        result.append (Drugbank (subject, predicate, obj) if "drugbank" in n3_file else PubChem(subject, predicate, obj))
#    return result
                                                                             
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

