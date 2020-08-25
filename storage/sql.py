'''
Created on 2020-08-24

@author: wf
'''
# python standard library
import sqlite3
import datetime
import time
import sys

class SQLDB(object):
    '''
    Structured Query Language Database wrapper
    
    :ivar dbname(string): name of the database
    :ivar debug(boolean): True if debug info should be provided
    '''

    def __init__(self,dbname=':memory:',debug=False):
        '''
        Construct me for the given dbname and debug
        
        Args:
        
           dbname(string): name of the database - default is a RAM based database
        
           debug(boolean): if True switch on debug
        '''
        self.dbname=dbname
        self.debug=debug
        self.c=sqlite3.connect(dbname)
        
    def close(self):
        ''' close my connection '''
        self.c.close()    
        
    def createTable(self,listOfRecords,entityName,primaryKey):
        '''
        derive  Data Definition Language CREATE TABLE command from list of Records by examining first recorda
        as defining sample record and execute DDL command
        
        auto detect column types see e.g. https://stackoverflow.com/a/57072280/1497139
        
        Args:
           listOfRecords(list): a list of Dicts
           entityName(string): the entity / table name to use
           primaryKey(string): the key/column to use as a  primary key
           
        Returns:
           EntityInfo: meta data information for the created table
        '''
        if len(listOfRecords)<1:
            raise Exception("Need a sample record to createTable")
        sampleRecord=listOfRecords[0]
        entityInfo=EntityInfo(sampleRecord,entityName,primaryKey,debug=self.debug) 
        self.c.execute(entityInfo.createTableCmd)
        return entityInfo
       
    def store(self,listOfRecords,entityInfo):
        '''
        store the given list of records based on the given entityInfo
        
        Args:
          
           listOfRecords(list): the list of Dicts to be stored
           entityInfo(EntityInfo): the meta data to be used for storing
        '''
        insertCmd=entityInfo.insertCmd
        self.c.executemany(insertCmd,listOfRecords)
        self.c.commit()
        
    def query(self,sqlQuery):
        '''
        run the given sqlQuery and return a list of Dicts
        
        Args:
            
            sqlQuery(string): the SQL query to be executed
                
        Returns:
            list: a list of Dicts
        '''
        if self.debug:
            print(sqlQuery)
        # https://stackoverflow.com/a/13735506/1497139
        cur=self.c.cursor()
        query = cur.execute(sqlQuery)
        colname = [ d[0] for d in query.description ]
        resultList=[]
        for row in query:
            record=dict(zip(colname, row))
            resultList.append(record)
        cur.close()
        return resultList
        
    def queryAll(self,entityInfo,fixDates=True):
        '''
        query all records for the given entityName/tableName
        
        Args:
           entityName(string): name of the entity/table to qury
           fixDates(boolean): True if date entries should be returned as such and not as strings
        '''  
        sqlQuery='SELECT * FROM %s' % entityInfo.name
        resultList=self.query(sqlQuery)
        if fixDates:
            entityInfo.fixDates(resultList)
        return resultList
    
    def progress(self,status, remaining, total):
        '''
        show progress
        '''
        print('Backup %s at %5.0f%%' % ("... " if status==0 else "done",(total-remaining)/total*100)) 
    
    def backup(self,backupDB,profile=False,showProgress=10):
        '''
        create backup of this SQLDB to the given backup db
        
        see https://stackoverflow.com/a/59042442/1497139
        '''
        if sys.version_info <= (3, 6):
            raise Exception("backup via stdlibrary not available in python <=3.6 see https://stackoverflow.com/a/49884210/1497139 for an alternative")
        startTime=time.time()
        bck=sqlite3.connect(backupDB)
        if showProgress>0:
            progress=self.progress
        else:
            progress=None
        with bck:
            self.c.backup(bck,pages=showProgress,progress=progress)
        bck.close()    
        elapsed=time.time()-startTime
        if profile:
            print("Backup to %s took %5.1f s" % (backupDB,elapsed))
        
class EntityInfo(object):
    """
    holds entity meta Info 
    
    :ivar name(string): entity name = table name
    
    :ivar primaryKey(string): the name of the primary key column
    
    :ivar typeMap(dict): maps column names to python types
    
    :ivar debug(boolean): True if debug information should be shown
    
    """
        
    def __init__(self,sampleRecord,name,primaryKey=None,debug=False):
        '''
        construct me from the given name and primary key
        
        Args:
           name(string): the name of the entity
           primaryKey(string): the name of the primary key column
           debug(boolean): True if debug information should be shown
        '''
        self.sampleRecord=sampleRecord
        self.name=name
        self.primaryKey=primaryKey
        self.debug=debug
        self.typeMap={}
        self.createTableCmd=self.getCreateTableCmd(sampleRecord)
        self.insertCmd=self.getInsertCmd()
        
    def getCreateTableCmd(self,sampleRecord):
        '''
        get the CREATE TABLE DDL command for the given sample record
        
        Args:
            sampleRecord(dict): a sample Record    
            
        Returns:
            string: CREATE TABLE DDL command for this entity info 
            
        Example:   
      
        .. code-block:: sql
            
            CREATE TABLE Person(name TEXT PRIMARY KEY,born DATE,numberInLine INTEGER,wikidataurl TEXT,age FLOAT,ofAge BOOLEAN)
    
        '''
        ddlCmd="CREATE TABLE %s(" %self.name
        delim=""
        for key,value in sampleRecord.items():
            valueType=type(value)
            if valueType == str:
                sqlType="TEXT"
            elif valueType == int:
                sqlType="INTEGER"
            elif valueType == float:
                sqlType="FLOAT"
            elif valueType == bool:
                sqlType="BOOLEAN"      
            elif valueType == datetime.date:
                sqlType="DATE"    
            else:
                raise Exception("unsupported type %s " % str(valueType))
            ddlCmd+="%s%s %s%s" % (delim,key,sqlType," PRIMARY KEY" if key==self.primaryKey else "")
            self.addType(key,valueType)
            delim=","
        ddlCmd+=")"  
        if self.debug:
            print (ddlCmd)    
        return ddlCmd
        
    def getInsertCmd(self):
        '''
        get the INSERT command for this entityInfo
        
        Returns:
            the INSERT INTO SQL command for his entityInfo e.g.
                 
        Example:   
      
        .. code-block:: sql

            INSERT INTO Person (name,born,numberInLine,wikidataurl,age,ofAge) values (?,?,?,?,?,?).

        '''
        columns =','.join(self.typeMap.keys())
        placeholders=':'+',:'.join(self.typeMap.keys())
        insertCmd="INSERT INTO %s (%s) values (%s)" % (self.name, columns,placeholders)
        if self.debug:
            print(insertCmd)
        return insertCmd
        
    def addType(self,column,valueType):
        '''
        add the python type for the given column to the typeMap
        
        Args:
           column(string): the name of the column
           
           valueType(type): the python type of the column
        '''
        self.typeMap[column]=valueType     
        
    def fixDates(self,resultList):
        '''
        fix date entries in the given resultList by parsing the date content e.g.
        converting '1926-04-21' back to datetime.date(1926, 4, 21)
        
        Args:
            resultList(list): the list of records to be fixed
        '''
        for record in resultList:
            for key,valueType in self.typeMap.items():
                if valueType==datetime.date:
                    dt=datetime.datetime.strptime(record[key],"%Y-%m-%d")  
                    dateValue=dt.date()  
                    record[key]=dateValue
      
    def getValueList(self,record,index):
        valueList=[]
        for key,value in record.items():
            if not key in self.typeMap:
                raise("unknown column %s" ,key)
            mapValueType=self.typeMap[key]
            valueType=type(value)
            if mapValueType!=valueType:
                raise("expected type %s but got %s for record %d of %s" % (str(mapValueType),str(valueType),index,self.name))
            if valueType==str:
                strValue="'%s'" % value
            elif valueType==datetime.date:
                strValue="'%s'" % value   
            else:
                strValue=str(value)
            if self.debug:
                print("%s(%s)=%s" % (key,str(valueType),strValue))
            valueList.append(strValue)
        return valueList   
    
   