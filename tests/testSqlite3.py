'''
Created on 2020-08-24

@author: wf
'''
import unittest
import sqlite3
import time
from storage.sample import Sample

class TestSqlite3(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testSqllite3(self):
        c = sqlite3.connect(':memory:')
        startTime=time.time()
        limit=100000
        listOfDicts=Sample.getSample(limit)
        print("Sample size is %d" % len(listOfDicts))
        c.execute("""CREATE TABLE sample(pkey text primary key, pindex integer)""")
        elapsed=time.time()-startTime
        for record in listOfDicts:
            cmd="""INSERT INTO sample(pkey, pindex)
              values('%s', %d)""" % ( record["pkey"],record["index"])
            #print(cmd)
            c.execute(cmd)
        print ("adding %d records took %5.3f s => %5.f records/s" % (limit,elapsed,limit/elapsed)) 
        startTime=time.time()
        sql = "SELECT * FROM sample"
        recs = c.execute(sql)
        resultList=[]
        for row in recs:
            result={}
            result["pkey"]=row[0]
            result["pindex"]=row[1]
            resultList.append(result)
        print ("selecting %d records took %5.3f s => %5.f records/s" % (len(resultList),elapsed,limit/elapsed)) 
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testSqllit3']
    unittest.main()