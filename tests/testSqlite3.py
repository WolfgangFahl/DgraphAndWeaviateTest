'''
Created on 2020-08-24

@author: wf
'''
import unittest
import time
from storage.sample import Sample
from storage.sql import SQLDB, EntityInfo

class TestSQLDB(unittest.TestCase):
    '''
    Test the SQLDB database wrapper
    '''

    def setUp(self):
        self.debug=True
        pass

    def tearDown(self):
        pass
    
    def checkListOfRecords(self,listOfRecords,entityName,primaryKey=None,fixDates=False,debug=False):
        '''
        check the handling of the given list of Records
        
        Args:
          
           listOfRecords(list): a list of dicts that contain the data to be stored
           entityName(string): the name of the entity type to be used as a table name
           primaryKey(string): the name of the key / column to be used as a primary key
           debug(boolean): True if debug information e.g. CREATE TABLE and INSERT INTO commands should be shown
      
        '''     
        size=len(listOfRecords)
        print("%s size is %d fixDates is: %r" % (entityName,size,fixDates))
        sqlDB=SQLDB(debug=debug)
        entityInfo=sqlDB.createTable(listOfRecords,entityName,primaryKey)
        startTime=time.time()
        sqlDB.store(listOfRecords,entityInfo)
        elapsed=time.time()-startTime
        print ("adding %d %s records took %5.3f s => %5.f records/s" % (size,entityName,elapsed,size/elapsed)) 
        resultList=sqlDB.queryAll(entityInfo,fixDates=fixDates)    
        print ("selecting %d %s records took %5.3f s => %5.f records/s" % (len(resultList),entityName,elapsed,len(resultList)/elapsed)) 
        sqlDB.close()
        return resultList
    
    def testEntityInfo(self):
        '''
        test creating entityInfo from the sample record
        '''
        listOfRecords=Sample.getRoyals()
        entityInfo=EntityInfo(listOfRecords[0],'Person','name',debug=True)
        self.assertEqual("CREATE TABLE Person(name TEXT PRIMARY KEY,born DATE,numberInLine INTEGER,wikidataurl TEXT,age FLOAT,ofAge BOOLEAN)",entityInfo.createTableCmd)
        self.assertEqual("INSERT INTO Person (name,born,numberInLine,wikidataurl,age,ofAge) values (:name,:born,:numberInLine,:wikidataurl,:age,:ofAge)",entityInfo.insertCmd)
    
    def testSqlite3(self):
        '''
        test sqlite3 with a few records from the royal family
        '''
        listOfRecords=Sample.getRoyals()
        resultList=self.checkListOfRecords(listOfRecords, 'Person', 'name',fixDates=True,debug=True)
        if self.debug:
            print(resultList)
        self.assertEquals(listOfRecords,resultList)
        
    def testListOfCities(self):
        '''
        test sqlite3 with some 120000 city records
        '''
        listOfRecords=Sample.getCities()
        for fixDates in [True,False]:
            retrievedList=self.checkListOfRecords(listOfRecords,'City',fixDates=fixDates)
            self.assertEqual(len(listOfRecords),len(retrievedList))
        
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