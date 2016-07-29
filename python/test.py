import unittest

from chemotext_util import WordPosition
from chemotext_util import Binary
#import eset as eset_mod
from equiv_set import EquivalentSet
import equiv_set as eset_mod

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

if __name__ == '__main__':
    unittest.main()
