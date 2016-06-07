import copy
import datetime
import json
import logging
import os
import socket

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
            paragraphs = obj['paragraphs'],
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

class Word2VecConf(Conf):
    def __init__(self, host, venv, framework_name, input_dir, mesh):
        super(Word2VecConf, self).__init__(host, venv, framework_name, input_dir)
        self.mesh = mesh

class KinaseConf(Conf):
    def __init__(self, host, venv, framework_name, input_dir, inact, medline, kinase_synonyms):
        super(KinaseConf, self).__init__(host, venv, framework_name, input_dir)
        self.inact = inact
        self.medline = medline
        self.kinase_synonyms = kinase_synonyms

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
                result.append (n)
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

class LoggingUtil(object):
    @staticmethod
    def init_logging (name):
        FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
        logging.basicConfig(format=FORMAT, level=logging.INFO)
        return logging.getLogger(name)


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
