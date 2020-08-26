'''
Created on 2020-08-24

@author: wf
'''
import unittest
import time
import os
import sys
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
    
    def checkListOfRecords(self,listOfRecords,entityName,primaryKey=None,fixDates=False,debug=False,doClose=True):
        '''
        check the handling of the given list of Records
        
        Args:
          
           listOfRecords(list): a list of dicts that contain the data to be stored
           entityName(string): the name of the entity type to be used as a table name
           primaryKey(string): the name of the key / column to be used as a primary key
           debug(boolean): True if debug information e.g. CREATE TABLE and INSERT INTO commands should be shown
           doClose(boolean): True if the connection should be closed
      
        '''     
        size=len(listOfRecords)
        print("%s size is %d fixDates is: %r" % (entityName,size,fixDates))
        self.sqlDB=SQLDB(debug=debug)
        entityInfo=self.sqlDB.createTable(listOfRecords,entityName,primaryKey)
        startTime=time.time()
        self.sqlDB.store(listOfRecords,entityInfo)
        elapsed=time.time()-startTime
        print ("adding %d %s records took %5.3f s => %5.f records/s" % (size,entityName,elapsed,size/elapsed)) 
        resultList=self.sqlDB.queryAll(entityInfo,fixDates=fixDates)    
        print ("selecting %d %s records took %5.3f s => %5.f records/s" % (len(resultList),entityName,elapsed,len(resultList)/elapsed)) 
        if doClose:
            self.sqlDB.close()
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
        
    def testBindingError(self):
        '''
        test list of Records with incomplete record leading to
        "You did not supply a value for binding 2"
        see https://bugs.python.org/issue41638
        '''
        listOfRecords=[{'name':'Pikachu', 'type':'Electric'},{'name':'Raichu' }]
        try:
            self.checkListOfRecords(listOfRecords,'Pokemon','name')
            self.fail("There should be an exception")
        except Exception as ex:
            if self.debug:
                print(str(ex))
            self.assertTrue('no value supplied for column' in str(ex))                                                         
        
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

    def testBackup(self):
        '''
        test creating a backup of the SQL database
        '''
        if sys.version_info >= (3, 7):
            listOfRecords=Sample.getCities()
            self.checkListOfRecords(listOfRecords,'City',fixDates=True,doClose=False)
            backupDB="/tmp/testSqlite.db"
            self.sqlDB.backup(backupDB,profile=True,showProgress=200)
            size=os.stat(backupDB).st_size
            print ("size of backup DB is %d" % size)
            self.assertTrue(size>600000)
            self.sqlDB.close()
            # restore
            ramDB=SQLDB.restore(backupDB, SQLDB.RAM, profile=True)
            entityInfo=EntityInfo(listOfRecords[0],'City',debug=True)
            allCities=ramDB.queryAll(entityInfo)
            self.assertEqual(len(allCities),len(listOfRecords))
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testSqllit3']
    unittest.main()