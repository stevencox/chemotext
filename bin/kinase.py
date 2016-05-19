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
        b = obj ['B']
        for sub in b:
            if sub['word'].find ('kinase') > -1:
                print "{0} -> \n    {1}".format (f, sub)
sys.exit (0)
