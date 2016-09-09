from __future__ import division
import ast
import argparse
import datetime
import os
import sys
import time
import traceback
from chemotext_util import LoggingUtil
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

logger = LoggingUtil.init_logging (__file__)

days_per_year = 365
quarter = int(days_per_year / 8) * 24 * 60 * 60

sns.set (style="white")

tp53_targets = map (lambda x : x.lower (), [
    "AKAP10",
    "AKAP11",
    "AKAP12",
    "AKAP2",
    "AKAP6",
    "CASK",
    "CCNH",
    "CDC42BPA",
    "CDC42BPG",
    "CDK1",
    "CHUK",
    "CKB",
    "CKS1B",
    "CKS2",
    "CLK1",
    "CLK2",
    "FES",
    "FGFR1",
    "FGFR2",
    "FGFR3",
    "ILKAP",
    "INSR",
    "IRAK1",
    "IRAK2",
    "IRAK3",
    "STK16",
    "STK24",
    "STK25",
    "STK26",
    "STK36",
    "STK38",
    "SYK",
    "TAB1",
    "TAB2",
    "TAB3",
    "TAOK1",
    "TBK1",
    "TEK",
    "TJP2",
    "WASF1",
    "WEE1",
    "WNK2",
    "ZAP70",
    "ATM",
    "ATR",
    "BMX",
    "CDK2",
    "CDKN1A",
    "CDKN2A",
    "CDKN2C",
    "CHEK1",
    "CHEK2",
    "CLK3",
    "CSNK1D",
    "CSNK1E",
    "CSNK2A1",
    "CSNK2A2",
    "CSNK2B",
    "GSK3B",
    "HIPK1",
    "HIPK2",
    "HIPK3",
    "ICK",
    "IKBKB",
    "LRRK2",
    "MAPK1",
    "MAPK11",
    "MAPK14",
    "MAPK8",
    "MAPKAPK5",
    "MPP5",
    "MTOR",
    "NUAK1",
    "PBK",
    "PLK1",
    "PRKAB2",
    "PRKCD",
    "PTK2",
    "RB1CC1",
    "SKP1",
    "SRPK1",
    "STK11",
    "STK4",
    "TK1",
    "TP53",
    "VRK1"
])

def is_special_interest (b):
    return b is not None and ( b.L in tp53_targets or b.R in tp53_targets )
        
class Plot(object):

    @staticmethod
    def plot_distances (d):
        fig, ax = plt.subplots ()
        sns.set_style ("whitegrid")
        ax = sns.boxplot(x="truth", y="doc_dist", data=d, palette="Set3", ax=ax)
        plt.savefig ("doc-whisker.png")
            
        ax = sns.boxplot(x="truth", y="par_dist", data=d, palette="Set3", ax=ax)
        plt.savefig ("par-whisker.png")
        
        ax = sns.boxplot(x="truth", y="sen_dist", data=d, palette="Set3", ax=ax)
        plt.savefig ("sen-whisker.png")

    @staticmethod
    def false_mention_histogram (binaries, output_dir):
        import numpy
        import matplotlib.pyplot as plt
        import seaborn as sns
        binaries = sorted (binaries, key=lambda b : b.date)
        dates = map (lambda b : b.date, binaries)
        dates = [ d for d in dates if d is not None ]
        diffs = [abs(v - dates[(i+1)%len(dates)]) for i, v in enumerate(dates)]

        if len(diffs) > 1000:
            key = "{0}@{1}".format (binaries[0].L, binaries[0].R)
            outfile = "{0}/false_{1}_{2}.png".format (output_dir, key, len(diffs))
            try:
                if len(diffs) > 0:
                    diffs.insert (0, diffs.pop ())
                plt.clf ()
                g = sns.distplot(diffs,
                                 bins=10,
                                 rug=True,
                                 axlabel="Mention Frequency : {0}".format (key));
                g.axes.set_title('Mention Frequency Distribution', fontsize=14)
                g.set_xlabel("Time",size = 14)
                g.set_ylabel("Probability",size = 14)
                plt.savefig (outfile)
            except:
                traceback.print_exc ()

        return []
    
    @staticmethod
    def plot_true_mentions (binaries, output_dir):
        is_true = any ([ b and b.fact for b in binaries ])
        is_worth_it = len(binaries) > 500 or is_special_interest (binaries[0])
        return Plot.plot_mentions (binaries, output_dir, "T") if is_true and is_worth_it else []
 
    @staticmethod
    def plot_false_mentions (binaries, output_dir):
        size = len(binaries)
        is_worth_it = size > 1000 or ( is_special_interest(binaries [0]) and len(binaries) > 200) 
        return Plot.plot_mentions (binaries, output_dir, "F") if is_worth_it else []
 
    @staticmethod
    def plot_mentions (binaries, output_dir, prefix):
        import numpy
        import matplotlib.pyplot as plt
        import seaborn as sns

        if len(binaries) == 0:
            print ("plot_mentions: len(binaries)==0, returning.\n")
            return []

        key = "{0}@{1}".format (binaries[0].L, binaries[0].R).\
              replace ("\n", "_").\
              replace (" ", "_")
        special_interest = is_special_interest (binaries[0])
        mentions = map (lambda b : b.date, binaries)
        mentions = sorted (filter (lambda d : d is not None, mentions))

        if len(mentions) < 100 and not special_interest:
            pass #print ("plot_mentions: len(binaries)<300, returning.\n")
        elif len(mentions) > 1:
            try:
                with open ("{0}/log.txt".format (output_dir), "a") as stream:
                    stream.write ("{0} {1}\n".format (key, mentions))
                file_pat = "{0}/spec_{1}_{2}_{3}.png" if special_interest else "{0}/{1}_{2}_{3}.png"
                outfile = file_pat.format (output_dir, prefix, key, len(mentions))
                print ("plot_mentions: generating {0}\n".format (outfile))
                Plot.plot_dates (key, mentions, outfile)
            except:
                print ("plot_mentions: ERROR: len(binaries): {0}.\n".format (len(binaries)))
                traceback.print_exc ()
        return mentions

    @staticmethod
    def plot_dates (key, mentions, outfile):
        plt.clf ()
        g = sns.distplot (mentions, 
                          bins=range (mentions[0],
                                      mentions [ len(mentions) - 1 ],
                                      quarter),
                          rug=True)
        g.axes.set_title("Mention Distribution: {0}".format (key), fontsize=14)
        g.set_xlabel("Time", size = 14)
        g.set_ylabel("Probability", size = 14)
        plt.savefig (outfile)

def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",   help="list of input pair / data point arrays")
    args = parser.parse_args()
    print ("Example arg: {0}".format (args.input))

    plt.figure (figsize=(16, 8))
    with open (args.input) as stream:
        key = None
        for line in stream:
            index = line.rfind (" [")
            if '@' not in line or index == -1:
                continue
            try:
                key = line[0:index]
                array = line[index:].strip ()
                result = sorted (ast.literal_eval (array))
                if len(result) < 400:
                    continue
                print ("key: {0}".format (key))
                Plot.plot_dates (key, result, "chart-{0}_{1}".format (key, len(result)))
            except:
                traceback.print_exc ()

if __name__ == "__main__":           
    main()


