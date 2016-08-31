import copy
import calendar
import datetime
import glob
import json
import logging
import re
import os
import socket
import sys
import time
import traceback
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from json import JSONEncoder
from json import JSONDecoder
import xml.parsers.expat
try:
    from lxml import etree as ET
except ImportError:
    import xml.etree.cElementTree as ET

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
    def __init__(self, id, L, R, docDist, paraDist, sentDist, code, fact, refs, pmid=-1,
                 leftDocPos=None, rightDocPos=None, dist=None, date=None):
        self.id = id
        self.L = L
        self.R = R
        self.docDist = docDist
        self.paraDist = paraDist
        self.sentDist = sentDist
        self.code = code
        self.fact = fact
        self.refs = refs
        self.pmid = pmid
        self.leftDocPos = leftDocPos
        self.rightDocPos = rightDocPos
        self.date = date
    def __str__ (self):
        return self.__repr__()
    def __repr__ (self):
        return """ id:{0}, L:{1}, R:{2}, docDist:{3}, paraDist:{4}, sentDist:{5}, code:{6}, fact:{7}, refs:{8}, pmid:{9}, ldpos:{10}, rdpos:{11}, date:{12}""".format (
            self.id, self.L, self.R, self.docDist, self.paraDist, self.sentDist, self.code, self.fact, self.refs, self.pmid,
            self.leftDocPos, self.rightDocPos, self.date)

class Fact (Binary):
    def __init__(self, id=0, L="", R="", docDist=0, paraDist=0, sentDist=0, code=0, fact=True, refs=[], pmid=-1,
                 leftDocPos=None, rightDocPos=None, dist=None, date=None):
        super(Fact, self).__init__(0, L, R, 0, 0, 0, 0, True, [], pmid)

#    def __init__(self, L, R, pmid):


class BinaryEncoder(JSONEncoder):
    def default(self, obj):
        # Convert objects to a dictionary of their representation
        d = { '__class__':obj.__class__.__name__, 
              '__module__':obj.__module__,
              }
        d.update(obj.__dict__)
        return d

class BinaryDecoder(JSONDecoder):
    def __init__(self, encoding=None):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object)
    def dict_to_object(self, d):
        if '__class__' in d:
            class_name = d.pop('__class__')
            module_name = d.pop('__module__')
            module = __import__(module_name)
#            print 'MODULE:', module
            class_ = getattr(module, class_name)
#            print 'CLASS:', class_
            args = dict( (key.encode('ascii'), value) for key, value in d.items())
#            print 'INSTANCE ARGS:', args
            inst = class_(**args)
        else:
            inst = d
        return inst

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
    def __init__(self, fileName, date, id, generator, raw, paragraphs, A, B, C, AB, BC, AC, BB, ABC):
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
        self.BB = BB
        self.ABC = ABC

class ArticleEncoder(JSONEncoder):
    def default(self, obj):
        # Convert objects to a dictionary of their representation
        d = { '__class__':obj.__class__.__name__, 
              '__module__':obj.__module__,
              }
        d.update(obj.__dict__)
        return d

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
        for k in [ 'AB', 'BC', 'AC', 'BB' ]:
            if not k in obj:
                continue
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
                    refs = refs,
                    pmid   = obj['id'],
                    leftDocPos = i['leftDocPos'] if 'leftDocPos' in i else None,
                    rightDocPos = i['rightDocPos'] if 'rightDocPos' in i else None))

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
            BB         = binaries ['BB'] if 'BB' in binaries else [],
            ABC        = triples
        )
'''
def read_article (article_path):
    result = None
    with open (article_path) as stream:
        try:
            result = json.loads (stream.read (), cls=ArticleDecoder)
        except ValueError:
            logger.error ("Unable to load article {0} (valueerror)".format (article_path))
            traceback.print_exc ()
        except IOError:
            logger.error ("Unable to load article {0} (ioerror)".format (article_path))
            traceback.print_exc ()
        except:
            traceback.print_exc ()
    return result
'''

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
    def get_article (article_path):
#        traceback.print_stack ()
        logger.info ("Article: @-- {0}".format (article_path))
        result = None
        try:
            with open (article_path) as stream:
                result = json.loads (stream.read (), cls=ArticleDecoder)
        except ValueError:
            logger.error ("Unable to load article {0}".format (article_path))
            traceback.print_exc ()
        except IOError:
            logger.error ("Unable to load article {0} (ioerror)".format (article_path))
            traceback.print_exc ()
        except:
            logger.error ("Unable to load article {0} (other error)".format (article_path))
            traceback.print_exc ()
        return result

    @staticmethod
    def get_article_paths (input_dir):
#       return glob.glob (os.path.join (input_dir, "Mol_Cancer_2008_May_12_7_37*fxml.json"))
        return glob.glob (os.path.join (input_dir, "*fxml.json"))

    @staticmethod
    def parse_date (date):
        result = None
        try:
            result = datetime.datetime.strptime (date, "%d-%m-%Y")
        except ValueError:
            print "ERROR-> {0}".format (date)
        logger.debug ("Parsed date: {0}, ts: {1}".format (result, calendar.timegm(result.timetuple())))
        return result
    @staticmethod
    def parse_month_year_date (month, year):
        result = None
        if month and year:
            logger.debug ("Parsing {0}-{1}-{2}".format (1, month, year))
            text = "{0}-{1}-{2}".format (1, month, year)
            try:
                result = datetime.datetime.strptime (text, "%d-%m-%Y")
            except ValueError:
                result = datetime.datetime.strptime (text, "%d-%b-%Y")
            logger.debug ("Parsed date: {0}, ts: {1}".format (result, calendar.timegm(result.timetuple())))
        return result
class Conf0(object):
    def __init__(self, host, venv, framework_name, input_dir, output_dir=None, parts=0):
        self.host = host
        self.venv = venv
        self.framework_name = framework_name
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.spark_conf = SparkConf (host, venv, framework_name, parts)
class Conf(object):
    def __init__(self, spark_conf, input_dir=None, output_dir=None):
        self.spark_conf = spark_conf
        self.input_dir = input_dir
        self.output_dir = output_dir
class SparkConf(object):
    def __init__(self, host, venv, framework_name, parts=0):
        self.host = host
        self.venv = venv
        self.framework_name = framework_name
        self.parts = parts
'''
class EquivConf(object):
    def __init__(self, spark_conf, input_dir, output_dir):
        self.spark_conf = spark_conf
        self.input_dir = input_dir
        self.output_dir = output_dir
'''
class EvaluateConf0(Conf):
    def __init__(self, host, venv, framework_name, input_dir, output_dir, slices, parts, ctdAB, ctdBC, ctdAC):
        super(EvaluateConf, self).__init__(host, venv, framework_name, input_dir, output_dir, parts)
        self.output_dir = output_dir
        self.slices = slices
        self.parts = parts
        self.ctdAB = ctdAB
        self.ctdBC = ctdBC
        self.ctdAC = ctdAC
class CTDConf(object):
    def __init__(self, ctdAB, ctdBC, ctdAC, geneGene):
        self.ctdAB = ctdAB
        self.ctdBC = ctdBC
        self.ctdAC = ctdAC
        self.geneGene = geneGene
class EvaluateConf(Conf):
    def __init__(self, spark_conf, input_dir, output_dir, slices, sample, ctd_conf):
        super(EvaluateConf, self).__init__(spark_conf, input_dir, output_dir)
        self.ctd_conf = ctd_conf
        self.slices = slices
        self.sample = sample
class MedlineConf0(object):
    def __init__(self, host, venv, framework_name, data_root, gen_pmid_map):
        self.host = host
        self.venv = venv
        self.framework_name = framework_name
        self.data_root = data_root
        self.gen_pmid_map = gen_pmid_map
class MedlineConf(object):
    def __init__(self, spark_conf, data_root, gen_pmid_map):
        super(MedlineConf, self).__init__(spark_conf)
        self.data_root = data_root
        self.gen_pmid_map = gen_pmid_map

class MedlineHeading(object):
    def __init__ (self, heading):
        descriptor = heading.find ("DescriptorName")                                     
        self.descriptor_name = descriptor.text.lower ()
        self.major_topic = descriptor.get ("MajorTopic")
        self.ui = descriptor.get ("UI")
        self.qualifiers = MedlineQualifier.from_heading (heading)
    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        return "name: {0} mt: {1} ui: {2} qual: {3}".format (
            self.descriptor_name, self.major_topic, self.ui, self.qualifiers)

class MedlineQualifier(object):
    def __init__(self, text, ui):
        self.text = text.lower ()
        self.ui = ui
    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        return "t:{0} ui:{1}".format (self.text, self.ui)
    @staticmethod
    def from_heading (heading):
        result = []
        qualifiers = heading.findall ("QualifierName")
        for qualifier in qualifiers:
            result.append (MedlineQualifier (qualifier.text, qualifier.get ("UI")))
        return result

class MedlineQuant(object):
    def __init__(self, pmid, date, A, B, C):
        self.pmid = pmid
        self.date = date
        self.A = A
        self.B = B
        self.C = C
    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        return "pmid:{0} dt:{1} A:{2} B:{3} C:{4}".format (
            self.pmid, self.date, self.A, self.B, self.C)

class Citation(object):
    ''' Represent a PubMed citation. '''
    @classmethod
    def from_node(cls, root):
        chemicals = [ n.text for n in root.findall ("ChemicalList/Chemical/NameOfSubstance") ]
        headings = [ MedlineHeading (h) for h in root.findall ("MeshHeadingList/MeshHeading") ]
        return cls (root.findtext ("PMID"),
                    root.find ("DateCreated"),
                    root.find ("Article/Journal/JournalIssue/PubDate"),
                    chemicals,
                    headings)

    def __init__(self, pmid, date_created, pub_date, chemicals, headings):
        self.pmid = pmid
        pubdate_timestamp = sys.maxint
        if len(pub_date) > 0:
            month = pub_date.findtext ("Month")
            year = pub_date.findtext ("Year")
            date_time = SerializationUtil.parse_month_year_date (month, year)
            if date_time:
                try:
                    pubdate_timestamp = int(calendar.timegm (date_time.timetuple ()))
                except ValueError:
                    logger.error ("Failed to parse date {0}".format (date_time))

        datecreated_timestamp = sys.maxint            
        if len(date_created) > 0:
            day = date_created.findtext ("Day")
            month = date_created.findtext ("Month")
            year = date_created.findtext ("Year")
            date_time = SerializationUtil.parse_date ("{0}-{1}-{2}".format (day, month, year))
            if date_time:
                try:
                    datecreated_timestamp = int(calendar.timegm (date_time.timetuple ()))
                except ValueError:
                    logger.error ("Failed to parse date {0}".format (date_time))
        
        '''
        logger.info ("pmid: {0} date_created: {1} pubdate {2}".format (
            self.pmid,
            datetime.fromtimestamp (datecreated_timestamp),
            datetime.fromtimestamp (pubdate_timestamp)))
        '''
        self.date = min (datecreated_timestamp, pubdate_timestamp)

        if self.date is datecreated_timestamp:
            logger.debug ("{0} went with datecreated".format (self.pmid))
        else:
            logger.debug ("{0} went with pubdate".format (self.pmid))

        self.chemicals = chemicals
        self.headings = headings

    def get_pmid_date (self):
        return ( self.pmid, self.date )

class Medline(object):

    def __init__(self, sc, conf, use_mem_cache=False):
        self.sc = sc
        self.use_mem_cache = use_mem_cache
        self.conf = conf
        self.medline_path = os.path.join (conf.data_root, "pubmed", "medline")
        self.pmid_root = os.path.join (conf.data_root, "pmid")
        
    @staticmethod
    def parse (file_name):
        logger.info ("Parse XML: {0}".format (file_name))
        result = []
        root = ET.parse (file_name).getroot ()
        citations = root.findall ("MedlineCitation")
        for citation in citations:
            result.append (Citation.from_node ( citation ))
        return result

    def parse_citations (self):
        logger.info ("Load medline data to determine dates by pubmed ids")
        logger.info ("   ** Medline path: {0}".format (self.medline_path))
        archives = glob.glob (os.path.join (self.medline_path, "*.xml"))
        logger.info ("   ** Found {0} XML citation files".format (len (archives)))
        return self.sc. \
            parallelize (archives). \
            flatMap (lambda file_name : Medline.parse (file_name))

    def generate_pmid_date_map (self):
        pairs = self.parse_citations (). \
                map (lambda citation : citation.get_pmid_date ())
        json_path = os.path.join (self.conf.data_root, "pmid", "pmid_date.json")
        with open(json_path, "w") as stream:
            data = pairs.collect ()
            stream.write (json.dumps (dict (data)))
            stream.close ()
            logger.info ("** Writing: {0}".format (json_path))

class Word2VecConf(Conf):
    def __init__(self, host, venv, framework_name, input_dir, mesh):
        super(Word2VecConf, self).__init__(SparkConf(host, venv, framework_name), input_dir)
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
        super(KinaseConf, self).__init__(spark_conf, input_dir)
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
            map (lambda a : SerializationUtil.get_article (a)). \
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
