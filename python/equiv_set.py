from __future__ import division
import argparse
import datetime
import glob
import json
import os
import logging
import math
import re
import shutil
import sys
import socket
import time
import traceback
from chemotext_util import Article
from chemotext_util import ArticleEncoder
from chemotext_util import Binary
from chemotext_util import BinaryEncoder
from chemotext_util import BinaryDecoder
from chemotext_util import SparkConf
#from chemotext_util import EquivConf
from chemotext_util import LoggingUtil
from chemotext_util import SerializationUtil as SUtil
from chemotext_util import SparkUtil

logger = LoggingUtil.init_logging (__file__)

class EqBinary(object):
    def __init__(self, binary, L, R):
        self.binary = binary
        self.L = L
        self.R = R
    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        return "l:{0} r:{1} dist:{2} lpos:{3} rpos:{4}".format (
            self.binary.L, self.binary.R, self.binary.docDist, self.L.docPos, self.R.docPos)

skiplist = [ 'for', 'was', 'long', 'her' ]
log_trace = False

class EquivalentSet(object):

    @staticmethod
    def log_sorted (s):
        if log_trace:
            print ("   Sorted")
            for k in s:
                print ("     1. sorted: {0}".format (k))
                
    @staticmethod
    def log_discard (e):
        if log_trace:
            print ("     3. discard: l:{0} r:{1} dist:{2} lpos:{3} rpos:{4}".format (
                e.binary.L, e.binary.R, e.binary.docDist, e.L.docPos, e.R.docPos))

    @staticmethod
    def log_reduced_set (REk_key, original_length, key):
        if log_trace:
            print ("Reduced length from {0} to {1} for key {2}".format (original_length, len (REk_key), key))
            for e in REk_key:
                print ("     4. REk_key: l:{0} r:{1} dist:{2}".format (e.L, e.R, e.docDist))

    @staticmethod
    def make_equiv_set (L, R, threshold=200):
        logger = LoggingUtil.init_logging (__file__)
        pairs = {}
        # Create all possible pairs
        for left in L:
            if left.word in skiplist:
                continue
            for right in R:
                if right.word in skiplist or right.word is left.word:
                    continue
                docDist = abs (left.docPos - right.docPos)
                if docDist < threshold:
                    key = "{0}@{1}".format (left.word, right.word)
                    binary = Binary (
                        id = 0,
                        L = left.word,
                        R = right.word,
                        docDist = docDist,
                        sentDist = abs ( left.sentPos - right.sentPos ),
                        paraDist = abs ( left.paraPos - right.paraPos ),
                        code = 1,
                        fact = False,
                        refs = [],
                        leftDocPos = left.docPos,
                        rightDocPos = right.docPos) 
                    if key in pairs:
                        pairs[key].append ( EqBinary (binary, left, right) )
                    else:
                        pairs[key] = [ EqBinary (binary, left, right) ]

        if log_trace:
            print ("")
            for k,v in pairs.iteritems ():
                for val in v: 
                    print ("  --: [{0}] -> [{1}]".format (k, val))

        # GroupBy (x,y)
        REk = []
        for key in pairs:
            REk_key = []
            if log_trace:
                print ("key: {0}".format (key))
            Ek = pairs[key]
            original_length = len (Ek)
            # Sort
            Ek.sort (key = lambda p: p.binary.docDist)
            while len(Ek) > 0:
                EquivalentSet.log_sorted (Ek)
                canonical = Ek[0]
                if log_trace:
                    print ("     2. canonical: {0}".format (canonical))
                # Min distance pair
                REk_key.append (canonical.binary)
                old = Ek
                Ek = [ e for e in Ek if not (e.L.docPos == canonical.L.docPos or 
                                             e.R.docPos == canonical.R.docPos) ]
                if log_trace:
                    for o in old:
                        if o not in Ek:
                            print "     3. discard {0}".format (o)
                EquivalentSet.log_reduced_set (REk_key, original_length, key)
            REk = REk + REk_key
        if log_trace:
            print ("REk: {0}".format (REk))
        return REk

    @staticmethod
    def get_article_equiv_set (article):
        if article:
            article.AB = EquivalentSet.make_equiv_set (article.A, article.B)
            article.BC = EquivalentSet.make_equiv_set (article.B, article.C)
            article.AC = EquivalentSet.make_equiv_set (article.A, article.C)
            article.BB = EquivalentSet.make_equiv_set (article.B, article.B)
        return article

    @staticmethod
    def get_article (article_path):
        article = EquivalentSet.get_article_equiv_set (SUtil.get_article (article_path))
        return [] if not article else [ article ]
        
