#!/usr/bin/python
import linecache
import cmd
import sys
import os
import sqlite3
import json

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def warn(str):
    print bcolors.WARNING+'Warning: '+str+bcolors.ENDC

def error(str):
    print bcolors.FAIL+'Error: '+ str+bcolors.ENDC

def info(str):
    print bcolors.OKGREEN+'Info: '+str+bcolors.ENDC

class TablePrinter(object):
    def __init__(self, fmt, sep=' ', ul=None):
        """
        Print a list of dicts as a table
        @param fmt: list of tuple(heading, key, width)
        heading: str, column label
        key: dictionary key to value to print
        width: int, column width in chars
        @param sep: string, separation between columns
        @param ul: string, character to underline column label, or None for no underlining
    """

        super(TablePrinter,self).__init__()
        self.fmt   = str(sep).join('{lb}{0}:{1}{rb}'.format(key, width, lb='{', rb='}') for heading,key,width in fmt)
        self.head  = {key:heading for heading,key,width in fmt}
        self.ul    = {key:str(ul)*width for heading,key,width in fmt} if ul else None
        self.width = {key:width for heading,key,width in fmt}

    def row(self, data):
        return self.fmt.format(**{ k:str(data.get(k,''))[:w] for k,w in self.width.iteritems() })

    def __call__(self, dataList):
        _r = self.row
        res = [_r(data) for data in dataList]
        res.insert(0, _r(self.head))
        if self.ul:
            res.insert(1, _r(self.ul))
            return '\n'.join(res)

class DirtyQuery(cmd.Cmd):
    """ A simple command based query to search flat files (nodejs dirty-db) format"""
    prompt = 'dirty-sql:|> '
    tmp = '/tmp'

    def __init__(self):
        #cmd is a old style class
        cmd.Cmd.__init__(self)
        os.unlink(DirtyQuery.tmp+'/dirty-query.db')
        self.tmpdb = sqlite3.connect(DirtyQuery.tmp+'/dirty-query.db')
        self.cur = self.tmpdb.cursor()
    def dropTable(self,name):
        self.cur.execute('drop table if exists '+name)
        self.tmpdb.commit()
    def do_use(self,collection):
        if not collection:
            error('Must specify existing collection')
            return
        self.collections =[s.strip() for s in  collection.split(',')]

        for collection in self.collections:
            if os.path.isfile(collection+'.db'):
                Compactor(collection+'.db').compact()
                fr = open(DirtyQuery.tmp+'/'+collection+'.db')
                recordKey = collection[0:len(collection)-1]
                lno = 1
                cols=[]
                self.dropTable(collection)

                for line in fr:
                    if len(line.strip()) ==  0 or not line:
                        print 'skipping..'
                        break
                    rec = json.loads(line)
                    if lno == 1:
                        tableStmt, cols =  self.createTable(collection,rec['val'][recordKey])
                        self.cur.execute(tableStmt)
                    istmt,values=self.convertToSql(collection,cols,rec['val'][recordKey],lno)
                    self.cur.execute(istmt,values)
                    lno+=1
                self.tmpdb.commit()
                # cur.close()
                fr.close()
            else:
                error('No collection exists')

    def typeOf(self,obj):
        if type(obj) is list:
            return 'list'
        elif type(obj) is dict:
            return 'dict'
        elif type(obj) is int:
            return 'int'
        elif type(obj) is float:
            return 'real'
        elif type(obj) is str or type(obj) is unicode:
            return 'text'
        elif type(obj) is bool:
            return 'int'
        return 'text'

    def isKeyWord(self,key):
        if key == 'group':
            key='group_'
        elif key == 'index':
            key='index_'
        return key

    def createTable(self,name,jsonObj):
        sqlCreate = 'create table '+name +' (lno int, '
        columns = ['lno']
        for key in jsonObj.keys():
            obj = jsonObj[key]
            if type(obj) is list or type(obj) is dict:
                continue
            sqlCreate = sqlCreate+self.isKeyWord(key)+'  '+self.typeOf(obj)+','
            columns.append(self.isKeyWord(key))
        sqlCreate = sqlCreate[0:len(sqlCreate)-1] + ' )'

        return sqlCreate, columns


    def listToStr(self,lst):
        return ",".join(['"'+str(item)+'"' for item in lst])

    def convertToSql(self,name,cols,record,lno):
        values = [lno]

        questions='(?,'
        for col in cols[1:len(cols)]:
            if col.endswith('_'):
                col = col[0:len(col)-1]

            try:
                values.append(record[col])
            except Exception, e:
                values.append(None)
            questions = questions + '?,'
        questions = questions[0:len(questions)-1]+')'

        istmt = 'insert into '+name+'('+self.listToStr(cols)+') values '+questions
        return istmt,values

    def do_select(self,query):
        if not query:
            error('Must specify a query statement, refer standard '+bcolors.BOLD+'SQL SELECT')
            return
        query = 'select '+ query+';'
        if sqlite3.complete_statement(query):
            try:
                query = query.strip()
                self.cur.execute(query)
                print self.cur.fetchall()
            except sqlite3.Error as e:
                error('Query Error: ' + e.args[0])

    def do_quit(self,line):
        self.tmpdb.close()
        return True
    def do_EOF(self,line):
        return self.do_quit(line)

class Compactor(object):
    def __init__(self,src):
        super(Compactor,self).__init__()
        self.src = src
        self.dest = '/tmp/'+self.src+'.bk'
        self.map = {}

    def prepareMap(self):
        print 'finding unique records.. in %s' % (self.src)
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
        print 'starting compaction on %s' % (self.src)
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
        # os.rename(self.src,self.src+'.orig')
        os.rename(self.dest,'/tmp/'+self.src)

if __name__ == '__main__':
    DirtyQuery().cmdloop()
