'''
Created on 2020-08-24

@author: wf
'''
# python standard library
import sqlite3
import datetime

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
        ddlCmd="CREATE TABLE %s(" %entityName
        if len(listOfRecords)<1:
            raise Exception("Need a sample record to createTable")
        sampleRecord=listOfRecords[0]
        delim=""
        entityInfo=EntityInfo(entityName,primaryKey,debug=self.debug)
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
            ddlCmd+="%s%s %s%s" % (delim,key,sqlType," PRIMARY KEY" if key==primaryKey else "")
            entityInfo.addType(key,valueType)
            delim=", "
        ddlCmd+=")"  
        if self.debug:
            print (ddlCmd) 
        self.c.execute(ddlCmd)
        return entityInfo
       
    def store(self,listOfRecords,entityInfo):
        '''
        store the given list of records based on the given entityInfo
        
        Args:
          
           listOfRecords(list): the list of Dicts to be stored
           entityInfo(EntityInfo): the meta data to be used for storing
        '''
        index=0;
        for record in listOfRecords:
            cmd="INSERT INTO %s" % entityInfo.name
            cmd+="(%s)" % (",".join(record.keys()))
            cmd+="values(%s)" % (",".join(entityInfo.getValueList(record,index)))
            index+=1 
            if self.debug:
                print(cmd)
            try:    
                self.c.execute(cmd) 
            except Exception as ex:
                print(ex)
                print(cmd)
                 
        
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
        
    def queryAll(self,entityName):
        '''
        query all records for the given entityName/tableName
        '''  
        sqlQuery='SELECT * FROM %s' % entityName
        resultList=self.query(sqlQuery)
        return resultList
        
class EntityInfo(object):
    """
    holds entity meta Info 
    
    :ivar name(string): entity name = table name
    
    :ivar primaryKey(string): the name of the primary key column
    
    :ivar typeMap(dict): maps column names to python types
    
    :ivar debug(boolean): True if debug information should be shown
    
    """
        
    def __init__(self,name,primaryKey=None,debug=False):
        '''
        construct me from the given name and primary key
        
        Args:
           name(string): the name of the entity
           primaryKey(string): the name of the primary key column
           debug(boolean): True if debug information should be shown
        '''
        self.name=name
        self.primaryKey=primaryKey
        self.debug=debug
        self.typeMap={}
        
    def addType(self,column,valueType):
        '''
        add the python type for the given column to the typeMap
        
        Args:
           column(string): the name of the column
           
           valueType(type): the python type of the column
        '''
        self.typeMap[column]=valueType     
        
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