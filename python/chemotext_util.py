import copy
import datetime
import glob
import json
import logging
import re
import os
import socket
from pyspark.sql import SQLContext

class LoggingUtil(object):
    @staticmethod
    def init_logging (name):
        FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
        logging.basicConfig(format=FORMAT, level=logging.INFO)
        return logging.getLogger(name)

logger = LoggingUtil.init_logging (__file__)

class Vocabulary(object):
    def __init__(self, A, B, C=None, ref=None):
        self.A = A
        self.B = B
        self.C = C
        self.ref = ref

class Quant(object):
    def __init__(self, before, not_before, false_positives):
        self.before = before
        self.not_before = not_before
        self.false_positives = false_positives

class WordPosition(object):
    def __init__(self, word, docPos, paraPos, sentPos):
        self.word = word
        self.docPos = docPos
        self.paraPos = paraPos
        self.sentPos = sentPos
    def __str__ (self):
        return """WordPosition(word:{0}, docPos:{1}, paraPos:{2}, sentPos:{3})""".format (
            self.word, self.docPos, self.paraPos, self.sentPos)

class Binary(object):
    def __init__(self, id, L, R, docDist, paraDist, sentDist, code, fact, refs):
        self.id = id
        self.L = L
        self.R = R
        self.docDist = docDist
        self.paraDist = paraDist
        self.sentDist = sentDist
        self.code = code
        self.fact = fact
        self.refs = refs
    def __str__ (self):
        return """ id:{0}, L:{1}, R:{2}, docDist:{3}, paraDist:{4}, sentDist:{5}, code:{6}, fact:{7}, refs:{8}""".format (
            self.id, self.L, self.R, self.docDist, self.paraDist, self.sentDist, self.code, self.fact, self.refs)

class KinaseBinary (Binary):
    def __init__(self, id, L, R, docDist, paraDist, sentDist, code, fact, refs, pmid, date, file_name):
        super(KinaseBinary, self).__init__(id, L, R, docDist, paraDist, sentDist, code, fact, refs)
        self.pmid = pmid
        self.date = date
        self.file_name = file_name
        self.ref_date = None
    def copy (self, ref_date):
        result = copy.deepcopy (self)
        self.fact = True
        self.ref_date = ref_date
        return self
    def __str__ (self):
        return """KinaseBinary(id:{0}, L:{1}, R:{2}, docDist:{3}, paraDist:{4}, sentDist:{5}, code:{6}, fact:{7}, refs:{8}, pmid:{9}, date:{10}, ref_date:{11}""".format (
            self.id, self.L, self.R, self.docDist, self.paraDist, self.sentDist, self.code, self.fact,
            self.refs, self.pmid, self.date, self.ref_date)

class Triple(object):
    def __init__(self, A, B, C):
        self.A = A
        self.B = B
        self.C = C

class Paragraph(object):
    def __init__(self, sentences):
        self.sentences = sentences

class Article(object):
    def __init__(self, fileName, date, id, generator, raw, paragraphs, A, B, C, AB, BC, AC, ABC):
        self.fileName = fileName
        self.date = date
        self.id = id
        self.generator = generator
        self.raw = raw
        self.paragraphs = paragraphs
        self.A = A
        self.B = B
        self.C = C
        self.AB = AB
        self.BC = BC
        self.AC = AC
        self.ABC = ABC

class ArticleDecoder(json.JSONDecoder):
    def decode(self, text):
        obj = super(ArticleDecoder, self).decode(text)

        paragraphs = []
        for p in obj["paragraphs"]:
            sentences = []
            for sentence in p["sentences"]:
                sentences.append (sentence)
            paragraphs.append (Paragraph (sentences))

        word_pos = {}
        for k in [ 'A', 'B', 'C' ]:
            word_pos [k] = []
            items = obj [k]
            for i in items:
                word_pos [k].append (WordPosition (
                    word = i['word'],
                    docPos = i['docPos'],
                    paraPos = i['paraPos'],
                    sentPos = i['sentPos']))

        binaries = {}
        for k in [ 'AB', 'BC', 'AC' ]:
            binaries[k] = []
            items = obj [k]
            for i in items:
                fact = i['fact'] if 'fact' in i else False
                refs = i['refs'] if 'refs' in i else None
                binaries [k].append (Binary (
                    id = i['id'],
                    L = i['L'],
                    R = i['R'],
                    docDist = i['docDist'],
                    paraDist = i['paraDist'],
                    sentDist = i['sentDist'],
                    code = i['code'],
                    fact = fact,
                    refs = refs))

        triples = []
        for triple in obj['ABC']:
            triples.append (Triple (
                A = triple['A'],
                B = triple['B'],
                C = triple['C']))

        return Article (
            fileName   = obj['fileName'],
            date       = obj['date'],
            id         = obj['id'],
            generator  = obj['generator'],
            raw        = obj['raw'],
            paragraphs = paragraphs, #obj['paragraphs'],
            A          = word_pos ['A'],
            B          = word_pos ['B'],
            C          = word_pos ['C'],
            AB         = binaries ['AB'],
            BC         = binaries ['BC'],
            AC         = binaries ['AC'],
            ABC        = triples
        )

def read_article (article_path):
    result = None
    with open (article_path) as stream:
        result = json.loads (stream.read (), cls=ArticleDecoder)
    return result

class SerializationUtil(object):
    @staticmethod
    def read_json_file (file_name):
        result = None
        with open (file_name) as stream:
            result = json.loads (stream.read ())
        return result
    @staticmethod
    def get_pmid_map (pmids):
        return SerializationUtil.read_json_file (pmids)
    @staticmethod
    def read_article (article_path):
        result = None
        with open (article_path) as stream:
            result = json.loads (stream.read (), cls=ArticleDecoder)
        return result
    @staticmethod
    def parse_date (date):
#        return datetime.datetime.strptime (date, "%d-%m-%Y")
        result = None
        try:
            result = datetime.datetime.strptime (date, "%d-%m-%Y")
        except ValueError:
            print "ERROR-> {0}".format (date)
        return result
    @staticmethod
    def parse_month_year_date (month, year):
        result = None
        if month and year:
            result = datetime.datetime.strptime ("{0}-{1}-{2}".format (1, month, year), "%d-%b-%Y")
        return result

class Conf(object):
    def __init__(self, host, venv, framework_name, input_dir):
        self.host = host
        self.venv = venv
        self.framework_name = framework_name
        self.input_dir = input_dir

class SparkConf(Conf):
    def __init__(self, host, venv, framework_name):
        self.host = host
        self.venv = venv
        self.framework_name = framework_name

class EvaluateConf(Conf):
    def __init__(self, host, venv, framework_name, input_dir, ctdAB, ctdBC, ctdAC):
        super(EvaluateConf, self).__init__(host, venv, framework_name, input_dir)
        self.ctdAB = ctdAB
        self.ctdBC = ctdBC
        self.ctdAC = ctdAC

class MedlineConf(object):
    def __init__(self, host, venv, framework_name, input_xml):
        self.host = host
        self.venv = venv
        self.framework_name = framework_name
        self.input_xml = input_xml


class Medline(object):
    def __init__(self, sc, medline_path):
        self.sc = sc
        self.medline_path = medline_path

    def load_pmid_date (self):
        logger.info ("Load medline data to determine dates by pubmed ids")
        sqlContext = SQLContext (self.sc)

        ''' Iterate over all Medline files adding them to the DataFrame '''
        medline_pieces = glob.glob (os.path.join (self.medline_path, "*.xml"))
        pmid_date = None
        for piece in medline_pieces:
            piece_rdd = sqlContext.read.format ('com.databricks.spark.xml'). \
                        options(rowTag='MedlineCitation').load (piece)
            pmid_date = pmid_date.unionAll (piece_rdd) if pmid_date else piece_rdd

        ''' For all Medline records, map pubmed id to creation date '''
        return pmid_date.                                         \
            select (pmid_date["DateCreated"], pmid_date["PMID"]). \
            rdd.                                                  \
            map (lambda r : Medline.map_date (r))

    @staticmethod
    def map_date (r):
        day = 0
        month = 0
        year = 0
        pmid = r["PMID"]["#VALUE"]
        date = r ["DateCreated"]
        if date:
            day = date["Day"]
            month = date["Month"]
            year = date["Year"]
        return ( pmid, SerializationUtil.parse_date ("{0}-{1}-{2}".format (day, month, year)) )

class Word2VecConf(Conf):
    def __init__(self, host, venv, framework_name, input_dir, mesh):
        super(Word2VecConf, self).__init__(host, venv, framework_name, input_dir)
        self.mesh = mesh

class DataLakeConf(object):
    def __init__(self, input_dir, intact, medline, proqinase_syn, mesh_syn, kin2prot):
        self.input_dir = input_dir
        self.intact = intact
        self.medline = medline
        self.proqinase_syn = proqinase_syn
        self.mesh_syn = mesh_syn
        self.kin2prot = kin2prot

class KinaseConf(Conf):
    def __init__(self, spark_conf, data_lake_conf, w2v_model):
        self.spark_conf = spark_conf
        self.data_lake_conf = data_lake_conf
        self.w2v_model = w2v_model

class KinaseMatch(object):
    def __init__(self, kinase, kloc, p53, ploc, pmid, date, file_name):
        self.kinase = kinase
        self.kloc = kloc
        self.p53 = p53
        self.ploc = ploc
        self.pmid = pmid
        self.date = date
        self.file_name = file_name

class ProQinaseSynonyms(object):
    def __init__(self, name, hgnc_name, syn):
        self.name = name
        self.hgnc_name = hgnc_name
        self.syn = syn
    def get_names (self):
        result = [ self.name, self.hgnc_name ]
        if self.syn:
            for n in self.syn.split (";"):
                result.append (n.lower ())
        return result

class P53Inter(object):
    def __init__(self, A, B, alt_A, alt_B, pmid):
        self.A = A
        self.B = B
        self.alt_A = alt_A
        self.alt_B = alt_B
        self.pmid = pmid

    def __str__ (self):
        return "P53Inter(A: {0}\n  B: {1}\n  alt_A: {2}\n  alt_B: {3} pmid: {4})".format (
            self.A, self.B, self.alt_A, self.alt_B, self.pmid)

class MedlineQuant(object):
    def __init__(self, pmid, date, A, B, C):
        self.pmid = pmid
        self.date = date
        self.A = A
        self.B = B
        self.C = C

class SparkUtil(object):
    @staticmethod
    def get_spark_context (conf):
        os.environ['PYSPARK_PYTHON'] = "{0}/bin/python".format (conf.venv)
        from pyspark import SparkConf, SparkContext
        from StringIO import StringIO
        ip = socket.gethostbyname(socket.gethostname())
        sparkConf = (SparkConf()
                     .setMaster(conf.host)
                     .setAppName(conf.framework_name))
        return SparkContext(conf = sparkConf)

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

class Intact (object):
    ''' Tools for managing the intact data base export '''
    @staticmethod
    def parse_pmid (field):
        logger = LoggingUtil.init_logging (__file__)
        result = None
        ids = field.split ("|")
        for i in ids:
            kv = i.split (":")
            for k in kv:
                if kv[0] == "pubmed":
                    result = kv[1]
                    break
            if result:
                break
        return result
    @staticmethod
    def parse_synonyms (synonyms, separator, result):
        logger = LoggingUtil.init_logging (__file__)
        synonym_pat = re.compile (r".*:([\w ]+)\(.*\)$", re.IGNORECASE)
        synonyms = synonyms.split (separator)
        for opt in synonyms:
            match = synonym_pat.search (opt)
            if match:
                text = match.group(1).lower ()
                result.append (text)
        return result
    @staticmethod
    def map_As (inter, target):
        result = []
        syns = Intact.get_As (inter, target)
        for syn in syns:
            result.append ( ( syn, inter ) )
        return result
    @staticmethod
    def get_As (inter, target):
        result = []
        synonyms = inter.alt_B if inter.A == target else inter.alt_A
        return Intact.parse_synonyms (synonyms, "|", result)
    @staticmethod
    def get_Bs (inter, target):
        result = []
        synonyms = inter.alt_B if inter.B == target else inter.alt_A
        return Intact.parse_synonyms (synonyms, "|", result)


class DataLake(object):
    def __init__(self, sc, conf):
        self.logger = LoggingUtil.init_logging ("DataLake")
        self.sc = sc
        self.conf = conf
    ''' Tools for connecting to data sources '''
    def load_articles (self):
        self.logger.info ("Load PubMed Central preprocessed to JSON as an RDD of article objects")
        articles = glob.glob (os.path.join (self.conf.input_dir, "*fxml.json"))
        return self.sc.parallelize (articles).         \
            map (lambda a : SerializationUtil.read_article (a)). \
            cache ()

    def load_intact (self):
        self.logger.info ("Load intact db...")
        sqlContext = SQLContext(self.sc)
        return sqlContext.read.                 \
            format('com.databricks.spark.csv'). \
            options(comment='#',                \
                    delimiter='\t').            \
            load(self.conf.intact).rdd.         \
            map (lambda r : P53Inter ( A = r.C0, B = r.C1, 
                                       alt_A = r.C4, alt_B = r.C5, 
                                       pmid = Intact.parse_pmid(r.C8) ) ) . \
            cache ()
    def load_pro_qinase (self):
        self.logger.info ("Load ProQinase Kinase synonym db...")
        sqlContext = SQLContext(self.sc)
        return sqlContext.read.                 \
            format('com.databricks.spark.csv'). \
            options (delimiter=' ').            \
            load (self.conf.proqinase_syn).rdd. \
            flatMap (lambda r : ProQinaseSynonyms (r.C1, r.C2, r.C3).get_names ()). \
            cache ()

    def load_pmid_date (self):
        medline = Medline (self.sc, self.conf.medline)
        return medline.load_pmid_date ()

    def load_vocabulary (self, kin2prot):
        self.logger.info ("Build A / B vocabulary from intact/pro_qinase/mesh...")
        intact = self.load_intact ()
        A = intact.flatMap (lambda inter : Intact.get_As (inter, 'uniprotkb:P04637')).distinct().cache ()
        B = intact.flatMap (lambda inter : Intact.get_Bs (inter, 'uniprotkb:P04637')).distinct().cache ()
        self.logger.info ("Kinases from intact: {0}".format (A.count ()))

        buf = []
        for p in kin2prot.collect ():
            p0 = p[0].lower ()
            if not p0 in buf:
                buf.append (p0)
            p1 = p[1].lower ()
            if not p1 in buf:
                buf.append (p1)
        K = self.sc.parallelize (buf)
        A = A.union (K)
        logger.info ("Kinases after adding Kinase2Uniprot: {0}".format (A.count ()))

        A = self.extend_A (A)

        return Vocabulary (A, B, ref=intact)
    def extend_A (self, A):
        A = A.union (self.load_pro_qinase ())
        self.logger.info ("Kinases from intact+ProQinase: {0}".format (A.count ()))
        skiplist = [ 'for', 'gene', 'complete', 'unsuitable', 'unambiguous', 'met', 'kit', 'name', 'yes',
                     'fast', 'fused', 'top', 'cuts', 'fragment', 'kind', 'factor', 'p53', None ]
        with open (self.conf.mesh_syn, "r") as stream:
            mesh = self.sc.parallelize (json.loads (stream.read ()))
            # TEST
            A = A.union (self.sc.parallelize ([ "protein kinase c inhibitor protein 1" ]) )
            self.logger.info ("Total kinases from intact/MeSH: {0}".format (A.count ()))

        return A.union (mesh).                     \
            map (lambda a : a.lower ()).           \
            filter (lambda a : a not in skiplist). \
            distinct ()
        
    def get_kin2prot (self):
        kin2prot = None
        with open (self.conf.kin2prot) as stream:
            kin2prot_list = json.loads (stream.read ())
            genes = []
            proteins = []
            for element in kin2prot_list:
                if "Genes" in element:
                    gene_map = element['Genes']
                    for key in gene_map:
                        syns = gene_map [key]
                        for syn in syns:
                            genes.append ( ( key, syn ) )
                if "Proteins" in element:
                    protein_map = element['Proteins']
                    for key in protein_map:
                        syns = protein_map [key]
                        for syn in syns:
                            proteins.append ( ( key, syn ) )
            kin2prot = self.sc.parallelize (genes + proteins)
            for k in kin2prot.collect ():
                print k
        return kin2prot
