import datetime
import json

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
        return """ word:{0}, docPos:{1}, paraPos:{2}, sentPos:{3}""".format (
            self.id, self.L, self.R, self.docDist, self.paraDist, self.sentDist, self.code, self.fact, self.refs)

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
        return datetime.datetime.strptime (date, "%d-%m-%Y")
