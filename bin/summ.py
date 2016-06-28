import json
import sys
import glob
import os
import traceback

path = sys.argv[1]
print "Analyzing preprocessed files in: {0}".format (path)

def read_set (kind, obj):
    count = {}
    alist = []
    if kind in obj:
        for a in obj[kind]:
            alist.append (a['word'])

    if len(alist) > 0:
        for w in alist:
            if w in count:
                count[w] = count[w] + 1
            else:
                count[w] = 1
    return count

def read_triples (obj):
    result = []
    if "ABC" in obj:
        result = obj["ABC"]
    return result

files = glob.glob (os.path.join (path, "*"))
for f in files:
    print f
    with open (f) as stream:
        obj = json.loads(stream.read ())
        try:
            print ("{0} -> {1}".format (f, json.dumps ({
                "A" : read_set ("A", obj),
                "B" : read_set ("B", obj),
                "C" : read_set ("C", obj),
                "ABC" : read_triples (obj)
            }, sort_keys = False, indent=2)))            
        except:
            traceback.print_exc () #pass
sys.exit (0)
