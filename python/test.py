import json
import unittest
import os
import fnmatch
import traceback
import equiv_set as eset_mod
from equiv_set import EquivalentSet
from evaluate import Evaluate
from chemotext_util import WordPosition
from chemotext_util import Binary
from chemotext_util import Fact
from chemotext_util import BinaryEncoder
from chemotext_util import BinaryDecoder
from chemotext_util import EvaluateConf
from chemotext_util import SparkConf
from chemotext_util import CTDConf

eset_mod.log_trace = True

class TestEquivalentSets(unittest.TestCase):

    def validate_eset (test, eset, index, L, R, docDist, leftDocPos, rightDocPos):
        item = eset [index]
        test.assertTrue (item.L is L and item.R is R,
                         msg = "Wrong words in record {0}. Expected {1}/{2} and got {3}/{4}".format (
                             index, L, R, item.L, item.R))
        test.assertTrue (item.docDist is docDist and 
                         item.leftDocPos is leftDocPos and 
                         item.rightDocPos is rightDocPos,
                         msg = "".join ([
                             "Wrong doc dists in record {0}.".format (index),
                             "Expected docPos:{0}, ldpos:{1}, rdpos:{2}. ".format (docDist, leftDocPos, rightDocPos),
                             "Instead got docPos:{0}, ldpos:{1}, rdpos:{2}. ".format (
                                 item.docDist, item.leftDocPos, item.rightDocPos)
                         ]))

    def test_make_equivalent_sets_0 (self):
        left = [
            WordPosition (word = "LeftWord0", docPos = 100, paraPos = 0, sentPos = 0)
        ]
        right = [
            WordPosition (word = "RightWord0", docPos = 150, paraPos = 0, sentPos = 0)
        ]
        
        #eset = eset_mod.make_equiv_set (left, right)
        eset = EquivalentSet.make_equiv_set (left, right)

        self.assertEqual (len (eset), 1, "incorrect result set size")

        item = eset [0]
        self.assertEqual (item.L, left[0].word, "wrong left word")
        self.assertEqual (item.R, right[0].word, "wrong right word")
        self.assertEqual (item.leftDocPos, left[0].docPos, "wrong left doc pos")
        self.assertEqual (item.rightDocPos, right[0].docPos, "wrong right doc pos")
        self.assertEqual (item.docDist, abs(right[0].docPos - left[0].docPos), "wrong distance")

    def test_make_equivalent_sets_1 (self):
        left = [
            WordPosition (word = "LeftWord0", docPos = 100, paraPos = 0, sentPos = 0),
            WordPosition (word = "LeftWord0", docPos = 150, paraPos = 0, sentPos = 0),

            WordPosition (word = "LeftWord1", docPos = 150, paraPos = 0, sentPos = 0)
        ]
        right = [
            WordPosition (word = "RightWord0", docPos = 115, paraPos = 0, sentPos = 0),
            WordPosition (word = "RightWord0", docPos = 160, paraPos = 0, sentPos = 0)
        ]
        eset = EquivalentSet.make_equiv_set (left, right)
        #eset = eset_mod.make_equiv_set (left, right)
        for e in eset:
            print "   --> {0}".format (e)

        self.assertEqual (len (eset), 3, "incorrect result set size")
        expect = [
            Binary (id=0, L="LeftWord1", R="RightWord0", docDist=10, sentDist=0, paraDist=0,
                    code=1, fact=False, refs=[], leftDocPos=150, rightDocPos=160),
            Binary (id=0, L="LeftWord0", R="RightWord0", docDist=10, sentDist=0, paraDist=0,
                    code=1, fact=False, refs=[], leftDocPos=150, rightDocPos=160),
            Binary (id=0, L="LeftWord0", R="RightWord0", docDist=15, sentDist=0, paraDist=0,
                    code=1, fact=False, refs=[], leftDocPos=100, rightDocPos=115)
        ]
        for idx, b in enumerate (expect):
            self.validate_eset (eset, idx, b.L, b.R, docDist=b.docDist,
                                leftDocPos=b.leftDocPos, rightDocPos=b.rightDocPos) 


class TestEvaluateion(unittest.TestCase):

    def test_evaluation (self):
        stars_home = os.path.abspath ("../..")
        data_home = os.path.abspath ("./data")
        ctd_home = os.path.join (data_home, "ctd")
        output_dir = os.path.abspath ("./data/eval")

        Evaluate.evaluate (
            EvaluateConf (
                spark_conf = SparkConf (host           = "local[2]",
                                        venv           = os.path.join (stars_home, "venv"),
                                        framework_name = "test-evaluate",
                                        parts          = 4),
                input_dir  = os.path.abspath ("./data/output.mesh.full"),
                output_dir = output_dir,
                slices     = 3,
                ctd_conf = CTDConf (
                    ctdAB = os.path.join (ctd_home, "CTD_chem_gene_ixns.csv"),
                    ctdBC = os.path.join (ctd_home, "CTD_genes_diseases.csv"),
                    ctdAC = os.path.join (ctd_home, "CTD_chemicals_diseases.csv"))))

        binaries = []
        for root, dirnames, filenames in os.walk (output_dir):
            for filename in fnmatch.filter(filenames, '*part-*'):
                if not "crc" in filename:
                    file_name = os.path.join(root, filename)
                    with open (file_name, "r") as stream:
                        for line in stream:
                            line = line.strip ()
                            if len(line) > 0:
                                try:
                                    obj = BinaryDecoder().decode (line)
                                    if not isinstance (obj, Fact):
                                        binaries.append (obj)
                                    #print ("    -> {0}".format (json.dumps (obj, cls=BinaryEncoder)))
                                except:
                                    print line
                                    traceback.print_exc ()
        patterns = [
            [ "10,10-bis(4-pyridinylmethyl)-9(10h)-anthracenone", "kcnq1", "18568022", 1281744000, True ]
        ]
        for pattern in patterns:
            self.assertContains (binaries, pattern)
            
    def assertContains (self, binaries, pattern):
        match = False
        L = pattern [0]
        R = pattern [1]
        pmid = pattern [2]
        date = pattern [3]
        fact = pattern [4]
        print "   -- assert: L:{0}, R:{1}, pmid:{2}, date:{3}, fact:{4}".format (L, R, pmid, date, fact)
        for binary in binaries:
            print binary
            if L == binary.L and R == binary.R and pmid == binary.pmid and date == binary.date and fact == binary.fact:
                match = True
                break
        self.assertTrue (match)

if __name__ == '__main__':
    unittest.main()
