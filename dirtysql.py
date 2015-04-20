#!/usr/bin/python
import linecache
import cmd
import sys
import os
import sqlite3
import json
import pprint


class bcolors:
    HEADER= '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def warn(str):
    return bcolors.WARNING+'Warning: '+str+bcolors.ENDC
def error(str):
    return bcolors.FAIL+'Error: '+ str+bcolors.ENDC
def info(str):
    return bcolors.OKGREEN+'Info: '+str+bcolors.ENDC
def header(str):
    return bcolors.HEADER+str+bcolors.ENDC
def blue(str):
    return bcolors.OKBLUE+str+bcolors.ENDC
def green(str):
    return bcolors.OKGREEN+str+bcolors.ENDC
def bold(str):
    return bcolors.BOLD+str+bcolors.ENDC

class TablePrinter(object):
    def __init__(self, records):
        """
        Print a list of tuples as a table
    """
        super(TablePrinter,self).__init__()
        self.headers = records[0].keys()
        self.rows = records
        self.formatStr=self.format(records[0])
        self.__call__()
    def format(self,cols):
        formats=[]
        align='<'
        width = 0

        for col in cols:
            if type(col) == int:
                align='^'
                width = 5
            elif type(col) == str or type(col) == unicode:
                align = '<'
                width = len(col) + 4
            formats.append('{:'+align+str(width)+'}')

        return ' '.join( [f for f in formats])

    def __call__(self):
        # print header
        print header(self.formatStr.format(*self.headers))

        for row in self.rows:
            print blue(self.formatStr.format(*row))


class DirtyQuery(cmd.Cmd):
    """ A simple command based query to search flat files (nodejs dirty-db) format"""
    prompt = 'dirty-sql:|> '
    tmp = '/tmp'

    def __init__(self):
        #cmd is a old style class
        cmd.Cmd.__init__(self)
        os.unlink(DirtyQuery.tmp+'/dirty-query.db')
        self.tmpdb = sqlite3.connect(DirtyQuery.tmp+'/dirty-query.db')
        self.tmpdb.row_factory = sqlite3.Row
        self.cur = self.tmpdb.cursor()
        self.pretty = pprint.PrettyPrinter(indent=2)


    def dropTable(self,name):
        self.cur.execute('drop table if exists '+name)
        self.tmpdb.commit()

    def help_use(self):
        msg=""" use <<filename1,filename2....filenameN>>
        Ex: say users.db will be used as
        use users
        """
        print info(msg)
    def help_select(self):
        msg='''Please refer standard SQL SELECT syntax here http://www.sqlite.org/lang_select.html
        For showing complete object. select ** from <<table>> where <<some condition>>
        '''
        print info(msg)

    def do_use(self,collection):
        if not collection:
            print error('Must specify existing collection')
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
                        print info('skipping..')
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
                print error('No collection exists')

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

    def showObject(self,query):
        table = query[query.index('from')+4:query.index('where')].strip()
        try:
            recordKey = table[0:len(table)-1]
            query = query.replace('**','lno')
            query = self.cur.execute(query)
            for row in self.cur.fetchall():
                print green(self.pretty.pformat(json.loads(linecache.getline(DirtyQuery.tmp+'/'+table+'.db',row['lno']))['val'][recordKey]))
        except Exception as e:
            print error('Query Error:' + e.args[0])


    def do_select(self,query):
        if not query:
            print error('Must specify a query statement, refer standard '+bcolors.BOLD+'SQL SELECT')
            return
        if query.strip().startswith('**'):
            query = 'select '+ query+';'
            self.showObject(query)
            return
        query = 'select '+ query+';'
        if sqlite3.complete_statement(query):
                try:
                    query = query.strip()
                    self.cur.execute(query)
                    TablePrinter(self.cur.fetchall())
                # for row in self.cur.fetchall():
                #     if count == 1:
                #         print info(str(row.keys()))
                #         count = count+1
                #     print row
                except Exception as e:
                    print error('Query Error: ' + e.args[0])

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
        print info('finding unique records.. in %s' % (self.src))
        lno = 1
        fr = open(self.src)
        for line in fr:
            if len(line.strip()) ==  0 or not line:
                break
            rec = json.loads(line)
            self.map[rec['key']] = lno
            lno+=1
        fr.close()
        print info('done')

    def compact(self):
        self.prepareMap()
        print info('starting compaction on %s' % (self.src))
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
        print info('done')
        self.swap()
        print info('ready to use %s' % (self.src))

    def swap(self):
        # os.rename(self.src,self.src+'.orig')
        os.rename(self.dest,'/tmp/'+self.src)

if __name__ == '__main__':
    DirtyQuery().cmdloop()
