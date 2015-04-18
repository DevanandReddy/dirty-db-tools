#!/usr/bin/python
import os
import linecache
import sys
import json
import threading

"""
 1.Read the file
 2.Read the key and line no
 3.Write the key and line no to a map
 4.Repeat the step 2
 5.Update the key with the newly found line no.
 6.Repeat step 4-5 until the eof.
 7.Write the file with the keys seeking the file position.
"""
class Compactor(threading.Thread):
    def __init__(self,src):
        super(Compactor,self).__init__()
        self.src = src
        self.dest = self.src+'.bk'
        self.map = {}

    def prepareMap(self):
        print 'finding unique records..'
        lno = 1
        fr = open(self.src)
        for line in fr:
            if len(line.strip()) ==  0 or not line:
                break
            rec = json.loads(line)
            self.map[rec['key']] = lno
            lno+=1
        fr.close()
        print 'done'

    def compact(self):
        self.prepareMap()
        print 'starting compaction..'
        fw = open(self.dest,'w')

        values = self.map.values()
        values.sort()
        for lno in values:
            line = linecache.getline(self.src,lno)
            try:
                rec = json.loads(line)
                val = rec['val']
                fw.write(line)
            except Exception, e:
                pass

        fw.close()
        linecache.clearcache()
        print 'done'
        self.swap()
        print('renaming %s to %s, now compacted file is %s' % (self.src,self.src+'.orig',self.src))

    def swap(self):
        os.rename(self.src,self.src+'.orig')
        os.rename(self.dest,self.src)

    def run(self):
        self.compact()


if __name__=="__main__":
    compactors = []
    for file in sys.argv[1:len(sys.argv)]:
        if os.path.isfile(file):
            cpt = Compactor(file)
            compactors.append(cpt)
            cpt.start()
        else:
            print "%s not a file..ignored" %(file)

    for compactor in compactors:
        compactor.join()
