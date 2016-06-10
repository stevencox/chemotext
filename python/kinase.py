import argparse
import glob
import json
import os
import re
import string
import traceback
from chemotext_util import DataLakeConf
from chemotext_util import KinaseConf
from chemotext_util import KinaseBinary
from chemotext_util import ProQinaseSynonyms
from chemotext_util import P53Inter
from chemotext_util import SerializationUtil as SerUtil
from chemotext_util import SparkConf
from chemotext_util import SparkUtil
from chemotext_util import LoggingUtil
from chemotext_util import Vocabulary
from chemotext_util import WordPosition
from pyspark.mllib.feature import Word2Vec
from pyspark.mllib.feature import Word2VecModel
from pyspark.sql import SQLContext

logger = LoggingUtil.init_logging (__file__)

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

class Ctext(object):
    ''' Chemotext core logic '''
    @staticmethod
    def metalexer (article, A, B):
        As = Ctext.lexer (article, A)
        Bs = Ctext.lexer (article, B)
        return Ctext.make_binaries (article, As, Bs)
    @staticmethod
    def lexer (article, terms):
        logger = LoggingUtil.init_logging (__file__)
        logger.debug ("Parsing @ {0}.json".format (article.fileName))
        result = []
        doc_pos = 0
        para_pos = 0
        sent_pos = 0
        for para in article.paragraphs:
            for sentence in para.sentences: #['sentences']:
                sentence = sentence.replace (".", " ")
                for term in terms.value:
                    if not term or len(term) < 3:
                        continue
                    pos = sentence.find (term) if " " in term else sentence.find (" %s " % term)
                    if pos > -1:
                        result.append (WordPosition (word    = term,
                                                     docPos  = doc_pos + pos,
                                                     paraPos = para_pos,
                                                     sentPos = sent_pos))
                sent_pos = sent_pos + 1
                doc_pos = doc_pos + len (sentence)
            para_pos = para_pos + 1
        for r in result:
            logger.info ("word: {0} file: {1}".format (r, article.fileName))
        return result
    @staticmethod
    def make_binaries (article, L, R, threshold=8000):
        logger = LoggingUtil.init_logging (__file__)
        result = []
        for l in L:
            for r in R:
                distance = abs(l.docPos - r.docPos)
                logger.debug ("Distance - {0}".format (distance))
                if distance < threshold:
                    if article.date.find ("--") > -1:
                        article.date = "0-0-9000"
                    binary = KinaseBinary (id = 0,
                                           L = l.word,
                                           R = r.word,
                                           docDist  = abs(l.docPos - r.docPos),
                                           paraDist = abs(l.paraPos - r.paraPos),
                                           sentDist = abs(l.sentPos - r.sentPos),
                                           code = 0,
                                           fact = False,
                                           refs = [],
                                           pmid = article.id,
                                           date = SerUtil.parse_date (article.date),
                                           file_name = article.fileName)
                    logger.info ("Binary: {0}".format (binary))
                    result.append (binary)
        return result

class Medline(object):
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
        return ( pmid, SerUtil.parse_date ("{0}-{1}-{2}".format (day, month, year)) )

class DataLake(object):
    def __init__(self, sc, conf):
        self.sc = sc
        self.conf = conf
    ''' Tools for connecting to data sources '''
    def load_articles (self):
        logger.info ("Load PubMed Central preprocessed to JSON as an RDD of article objects")
        articles = glob.glob (os.path.join (self.conf.input_dir, "*fxml.json"))
        return self.sc.parallelize (articles). \
            map (lambda a : SerUtil.read_article (a)). \
            cache ()
    def load_intact (self):
        logger.info ("Load intact db...")
        sqlContext = SQLContext(self.sc)
        return sqlContext.read.                 \
            format('com.databricks.spark.csv'). \
            options(comment='#',                \
                    delimiter='\t').            \
            load(self.conf.intact).rdd.          \
            map (lambda r : P53Inter ( A = r.C0, B = r.C1, 
                                       alt_A = r.C4, alt_B = r.C5, 
                                       pmid = Intact.parse_pmid(r.C8) ) ) . \
            cache ()
    def load_pro_qinase (self):
        logger.info ("Load ProQinase Kinase synonym db...")
        sqlContext = SQLContext(self.sc)
        return sqlContext.read. \
            format('com.databricks.spark.csv'). \
            options (delimiter=' '). \
            load (self.conf.proqinase_syn).rdd. \
            flatMap (lambda r : ProQinaseSynonyms (r.C1, r.C2, r.C3).get_names ()). \
            cache ()
    def load_pmid_date (self):
        logger.info ("Load medline data to determine dates by pubmed ids")
        sqlContext = SQLContext (self.sc)
        pmid_date = sqlContext.read.format ('com.databricks.spark.xml'). \
        options(rowTag='MedlineCitation').load(self.conf.medline)
        #sample (False, 0.02)
        return pmid_date. \
            select (pmid_date["DateCreated"], pmid_date["PMID"]). \
            rdd. \
            map (lambda r : Medline.map_date (r))
    def load_vocabulary (self, kin2prot):
        logger.info ("Build A / B vocabulary from intact/pro_qinase/mesh...")
        intact = self.load_intact ()
        A = intact.flatMap (lambda inter : Intact.get_As (inter, 'uniprotkb:P04637')).distinct().cache ()
        B = intact.flatMap (lambda inter : Intact.get_Bs (inter, 'uniprotkb:P04637')).distinct().cache ()
        logger.info ("Kinases from intact: {0}".format (A.count ()))

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
        logger.info ("Kinases from intact+ProQinase: {0}".format (A.count ()))

        logger.info ("Add MeSH derived kinase terms to list of As...")
        skiplist = [ 'for', 'gene', 'complete', 'unsuitable', 'unambiguous', 'met', 'kit', 'name', 'yes',
                     'fast', 'fused', 'top', 'cuts', 'fragment', 'kind', 'factor', None ]
        with open (self.conf.mesh_syn, "r") as stream:
            mesh = self.sc.parallelize (json.loads (stream.read ()))
            # TEST
            A = A.union (self.sc.parallelize ([ "protein kinase c inhibitor protein 1" ]) )
            logger.info ("Total kinases from intact/MeSH: {0}".format (A.count ()))

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

class WordEmbed(object):
    def __init__(self, sc, conf, articles):
        self.sc = sc
        self.conf = conf
        if os.path.exists (conf.w2v_model):
            logger.info ("Load existing word2vec model: {0}".format (self.conf.w2v_model))
            '''
            https://issues.apache.org/jira/browse/SPARK-12016
            
            '''
            self.model = Word2VecModel.load (self.sc, self.conf.w2v_model)
        else:
            logger.info ("Compute word2vec word embedding model...")
            text = articles. \
                   flatMap (lambda a : a.paragraphs ). \
                   flatMap (lambda p : p.sentences ). \
                   map (lambda s : s.replace(".", " ").split (" ") )
            print "text: {0}".format (text.collect ())
            self.model = Word2Vec (). \
                         setNumPartitions (100). \
                         fit (text)
            self.model.save (self.sc, self.conf.w2v_model)
    def find_syn (self, word, radius=10):
        results = []
        # https://issues.apache.org/jira/browse/SPARK-12016
        try:
            if not " " in word:
                results = self.model.findSynonyms (word, radius)
        except:
            pass
            #logger.info ("word embedding unable to find word {0}".format (word))
        return results

class LitCrawl(object):
    ''' Crawl the literature in search of interactions '''
    @staticmethod
    def find_interactions (sc, vocabulary, articles):
        logger.info ("Find kinase-p53 interactions in article text.")
        broadcast_A = sc.broadcast (vocabulary.A.collect ())
        broadcast_B = sc.broadcast (vocabulary.B.collect ())
        return articles.flatMap (lambda a : Ctext.metalexer (a, broadcast_A, broadcast_B) ).cache ()
    @staticmethod
    def find_facts (vocabulary, binaries):
        logger.info ("Join matches from full text with the intact database to find 'facts'.")
        binaries_map = binaries.map (lambda r : ( r.L, r) ) # ( A -> KinaseBinary.L )
        intact_map = vocabulary.ref.flatMap (lambda inter : Intact.map_As(inter, 'uniprotkb:P04637') ) # ( A -> P53Inter )
        return binaries_map.join (intact_map) # ( A -> ( KinaseBinary, P53Inter ) )    
    @staticmethod
    def find_before (pmid_date, facts):
        logger.info ("Join facts with the pmid->date map to find interactions noticed before published discovery.")
        ref_pmid_to_binary = facts.map (lambda r : ( r[1][1].pmid, r[1][0] ) ) # ( intact.REF[pmid] -> KinaseBinary )
        # TEST. Add reference pmids with late dates.
        pmid_date = pmid_date.union (ref_pmid_to_binary.map (lambda r : ( r[0], SerUtil.parse_date ("1-1-2300") )))
        before = ref_pmid_to_binary.                             \
                 join (pmid_date).                                    \
                 map (lambda r : r[1][0].copy (ref_date = r[1][1]) ). \
                 filter (lambda k : k.date and k.ref_date and k.date < k.ref_date).             \
                 distinct ()
        return before

def execute (conf, home):
    sc = SparkUtil.get_spark_context (conf.spark_conf)

    data_lake = DataLake (sc, conf.data_lake_conf)
    kin2prot = data_lake.get_kin2prot ()
    articles = data_lake.load_articles ()
    vocabulary = data_lake.load_vocabulary (kin2prot)
    pmid_date = data_lake.load_pmid_date () # ( pmid -> date )

    binaries = LitCrawl.find_interactions (sc, vocabulary, articles)
    facts = LitCrawl.find_facts (vocabulary, binaries)
    before = LitCrawl.find_before (pmid_date, facts)

    for m in before.collect ():
        logger.info ("Before-Ref-Date:> {0}".format (m))

    embed = WordEmbed (sc, conf, articles)
    for w in vocabulary.A.collect ():
        for syn in embed.find_syn (w, radius=800):
            if "kinase" in syn or "p53" in syn:
                print "   -- {0}:syn>> {1}".format (w, syn)
            
def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master",    help="Mesos master host")
    parser.add_argument("--name",      help="Spark framework name")
    parser.add_argument("--input",     help="Data root directory")
    parser.add_argument("--home",      help="App home")
    parser.add_argument("--venv",      help="Path to Python virtual environment to use")
    parser.add_argument("--intact",    help="Path to intact data")
    parser.add_argument("--medline",   help="Path to Medline data")
    parser.add_argument("--mesh",      help="File containing JSON array of MeSH synonyms")
    parser.add_argument("--proqinase", help="Kinase synonyms from Pro Qinase")
    parser.add_argument("--kin2prot",  help="Kinase name to uniprot id mapping")    
    parser.add_argument("--w2v",       help="Word embedding model file")
    args = parser.parse_args()
    conf = KinaseConf (spark_conf         = SparkConf (
                           host           = args.master,
                           venv           = args.venv,
                           framework_name = args.name),
                       data_lake_conf     = DataLakeConf (
                           input_dir      = args.input,
                           intact         = args.intact,
                           medline        = args.medline, 
                           proqinase_syn  = args.proqinase,
                           mesh_syn       = args.mesh,
                           kin2prot       = args.kin2prot),
                       w2v_model          = args.w2v)
    execute (conf, args.home)

main ()


''' Medline data frame schema:
root                                                                            
 |-- @Owner: string (nullable = true)
 |-- @Status: string (nullable = true)
 |-- @VersionDate: string (nullable = true)
 |-- @VersionID: long (nullable = true)
 |-- Article: struct (nullable = true)
 |    |-- @PubModel: string (nullable = true)
 |    |-- Abstract: struct (nullable = true)
 |    |    |-- AbstractText: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- #VALUE: string (nullable = true)
 |    |    |    |    |-- @Label: string (nullable = true)
 |    |    |    |    |-- @NlmCategory: string (nullable = true)
 |    |    |-- CopyrightInformation: string (nullable = true)
 |    |-- ArticleDate: struct (nullable = true)
 |    |    |-- @DateType: string (nullable = true)
 |    |    |-- Day: long (nullable = true)
 |    |    |-- Month: long (nullable = true)
 |    |    |-- Year: long (nullable = true)
 |    |-- ArticleTitle: string (nullable = true)
 |    |-- AuthorList: struct (nullable = true)
 |    |    |-- @CompleteYN: string (nullable = true)
 |    |    |-- Author: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- @ValidYN: string (nullable = true)
 |    |    |    |    |-- AffiliationInfo: struct (nullable = true)
 |    |    |    |    |    |-- Affiliation: string (nullable = true)
 |    |    |    |    |-- CollectiveName: string (nullable = true)
 |    |    |    |    |-- ForeName: string (nullable = true)
 |    |    |    |    |-- Initials: string (nullable = true)
 |    |    |    |    |-- LastName: string (nullable = true)
 |    |    |    |    |-- Suffix: string (nullable = true)
 |    |-- DataBankList: struct (nullable = true)
 |    |    |-- @CompleteYN: string (nullable = true)
 |    |    |-- DataBank: struct (nullable = true)
 |    |    |    |-- AccessionNumberList: struct (nullable = true)
 |    |    |    |    |-- AccessionNumber: array (nullable = true)
 |    |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- DataBankName: string (nullable = true)
 |    |-- ELocationID: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- #VALUE: string (nullable = true)
 |    |    |    |-- @EIdType: string (nullable = true)
 |    |    |    |-- @ValidYN: string (nullable = true)
 |    |-- GrantList: struct (nullable = true)
 |    |    |-- @CompleteYN: string (nullable = true)
 |    |    |-- Grant: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- Acronym: string (nullable = true)
 |    |    |    |    |-- Agency: string (nullable = true)
 |    |    |    |    |-- Country: string (nullable = true)
 |    |    |    |    |-- GrantID: string (nullable = true)
 |    |-- Journal: struct (nullable = true)
 |    |    |-- ISOAbbreviation: string (nullable = true)
 |    |    |-- ISSN: struct (nullable = true)
 |    |    |    |-- #VALUE: string (nullable = true)
 |    |    |    |-- @IssnType: string (nullable = true)
 |    |    |-- JournalIssue: struct (nullable = true)
 |    |    |    |-- @CitedMedium: string (nullable = true)
 |    |    |    |-- Issue: string (nullable = true)
 |    |    |    |-- PubDate: struct (nullable = true)
 |    |    |    |    |-- Day: long (nullable = true)
 |    |    |    |    |-- MedlineDate: string (nullable = true)
 |    |    |    |    |-- Month: string (nullable = true)
 |    |    |    |    |-- Season: string (nullable = true)
 |    |    |    |    |-- Year: long (nullable = true)
 |    |    |    |-- Volume: string (nullable = true)
 |    |    |-- Title: string (nullable = true)
 |    |-- Language: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- Pagination: struct (nullable = true)
 |    |    |-- MedlinePgn: string (nullable = true)
 |    |-- PublicationTypeList: struct (nullable = true)
 |    |    |-- PublicationType: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- #VALUE: string (nullable = true)
 |    |    |    |    |-- @UI: string (nullable = true)
 |    |-- VernacularTitle: string (nullable = true)
 |-- ChemicalList: struct (nullable = true)
 |    |-- Chemical: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- NameOfSubstance: struct (nullable = true)
 |    |    |    |    |-- #VALUE: string (nullable = true)
 |    |    |    |    |-- @UI: string (nullable = true)
 |    |    |    |-- RegistryNumber: string (nullable = true)
 |-- CitationSubset: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- CommentsCorrectionsList: struct (nullable = true)
 |    |-- CommentsCorrections: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- @RefType: string (nullable = true)
 |    |    |    |-- Note: string (nullable = true)
 |    |    |    |-- PMID: struct (nullable = true)
 |    |    |    |    |-- #VALUE: long (nullable = true)
 |    |    |    |    |-- @Version: long (nullable = true)
 |    |    |    |-- RefSource: string (nullable = true)
 |-- DateCompleted: struct (nullable = true)
 |    |-- Day: long (nullable = true)
 |    |-- Month: long (nullable = true)
 |    |-- Year: long (nullable = true)
 |-- DateCreated: struct (nullable = true)
 |    |-- Day: long (nullable = true)
 |    |-- Month: long (nullable = true)
 |    |-- Year: long (nullable = true)
 |-- DateRevised: struct (nullable = true)
 |    |-- Day: long (nullable = true)
 |    |-- Month: long (nullable = true)
 |    |-- Year: long (nullable = true)
 |-- GeneralNote: struct (nullable = true)
 |    |-- #VALUE: string (nullable = true)
 |    |-- @Owner: string (nullable = true)
 |-- InvestigatorList: struct (nullable = true)
 |    |-- Investigator: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- @ValidYN: string (nullable = true)
 |    |    |    |-- ForeName: string (nullable = true)
 |    |    |    |-- Initials: string (nullable = true)
 |    |    |    |-- LastName: string (nullable = true)
 |    |    |    |-- Suffix: string (nullable = true)
 |-- KeywordList: struct (nullable = true)
 |    |-- @Owner: string (nullable = true)
 |    |-- Keyword: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- #VALUE: string (nullable = true)
 |    |    |    |-- @MajorTopicYN: string (nullable = true)
 |-- MedlineJournalInfo: struct (nullable = true)
 |    |-- Country: string (nullable = true)
 |    |-- ISSNLinking: string (nullable = true)
 |    |-- MedlineTA: string (nullable = true)
 |    |-- NlmUniqueID: string (nullable = true)
 |-- MeshHeadingList: struct (nullable = true)
 |    |-- MeshHeading: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- DescriptorName: struct (nullable = true)
 |    |    |    |    |-- #VALUE: string (nullable = true)
 |    |    |    |    |-- @MajorTopicYN: string (nullable = true)
 |    |    |    |    |-- @Type: string (nullable = true)
 |    |    |    |    |-- @UI: string (nullable = true)
 |    |    |    |-- QualifierName: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- #VALUE: string (nullable = true)
 |    |    |    |    |    |-- @MajorTopicYN: string (nullable = true)
 |    |    |    |    |    |-- @UI: string (nullable = true)
 |-- OtherAbstract: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- @Language: string (nullable = true)
 |    |    |-- @Type: string (nullable = true)
 |    |    |-- AbstractText: string (nullable = true)
 |-- OtherID: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- #VALUE: string (nullable = true)
 |    |    |-- @Source: string (nullable = true)
 |-- PMID: struct (nullable = true)
 |    |-- #VALUE: long (nullable = true)
 |    |-- @Version: long (nullable = true)
 |-- PersonalNameSubjectList: struct (nullable = true)
 |    |-- PersonalNameSubject: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- ForeName: string (nullable = true)
 |    |    |    |-- Initials: string (nullable = true)
 |    |    |    |-- LastName: string (nullable = true)
 |-- SupplMeshList: struct (nullable = true)
 |    |-- SupplMeshName: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- #VALUE: string (nullable = true)
 |    |    |    |-- @Type: string (nullable = true)
 |    |    |    |-- @UI: string (nullable = true)
'''
