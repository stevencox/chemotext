import json
import sys
import glob
import os

path = sys.argv[1]
print "Analyzing preprocessed files in: {0}".format (path)

files = glob.glob (os.path.join (path, "*"))
for f in files:
    with open (f) as stream:
        obj = json.loads(stream.read ())
        triples = obj ["ABC"]
        if len (triples) > 0:
            print "{0} -> \n    {1}".format (f, triples)
sys.exit (0)
