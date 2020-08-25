'''
Created on 2020-08-24

@author: wf
'''
import unittest
import time
from storage.sample import Sample
from storage.sql import SQLDB

class TestSQLDB(unittest.TestCase):
    '''
    Test the SQLDB database wrapper
    '''

    def setUp(self):
        self.debug=True
        pass


    def tearDown(self):
        pass
    
    def checkListOfRecords(self,listOfRecords,entityName,primaryKey=None,debug=False):
        '''
        check the handling of the given list of Records
        
        Args:
          
           listOfRecords(list): a list of dicts that contain the data to be stored
           entityName(string): the name of the entity type to be used as a table name
           primaryKey(string): the name of the key / column to be used as a primary key
           debug(boolean): True if debug information e.g. CREATE TABLE and INSERT INTO commands should be shown
      
        '''     
        size=len(listOfRecords)
        print("%s size is %d" % (entityName,size))
        sqlDB=SQLDB(debug=debug)
        entityInfo=sqlDB.createTable(listOfRecords,entityName,primaryKey)
        startTime=time.time()
        sqlDB.store(listOfRecords,entityInfo)
        elapsed=time.time()-startTime
        print ("adding %d %s records took %5.3f s => %5.f records/s" % (size,entityName,elapsed,size/elapsed)) 
        resultList=sqlDB.queryAll(entityInfo.name)    
        print ("selecting %d %s records took %5.3f s => %5.f records/s" % (len(resultList),entityName,elapsed,len(resultList)/elapsed)) 
        sqlDB.close()
        return resultList
    
    def testSqlite3(self):
        '''
        test sqlite3 with a few records from the royal family
        '''
        listOfRecords=Sample.getRoyals()
        resultList=self.checkListOfRecords(listOfRecords, 'Person', 'name',debug=True)
        if self.debug:
            print(resultList)
        #self.assertEquals(listOfRecords,resultList)
        
    def testListOfCities(self):
        '''
        test sqlite3 with some 120000 city records
        '''
        listOfRecords=Sample.getCities()
        self.checkListOfRecords(listOfRecords,'City')
        
    def testSqllite3Speed(self):
        '''
        test sqlite3 speed with some 100000 artificial sample records
        consisting of two columns with a running index
        '''
        limit=100000
        listOfRecords=Sample.getSample(limit)
        self.checkListOfRecords(listOfRecords, 'Sample', 'pKey')
     

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testSqllit3']
    unittest.main()