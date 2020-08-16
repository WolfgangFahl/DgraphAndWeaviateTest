'''
Created on 2020-08-14

@author: wf
'''
from SPARQLWrapper import SPARQLWrapper, JSON
from SPARQLWrapper.Wrapper import POSTDIRECTLY, POST

class Jena(object):
    '''
    wrapper for apache Jana
    '''

    def __init__(self,url,mode='query',returnFormat=JSON):
        '''
        Constructor
        '''
        self.url="url%s" % (mode)
        self.mode=mode
        self.sparql=SPARQLWrapper(url,returnFormat=returnFormat)
        
    def rawQuery(self,queryString,method='POST'):
        '''
        query with the given query string
        '''
        self.sparql.setQuery(queryString)
        self.sparql.method=method
        queryResult = self.sparql.query()
        return queryResult 
    
    def getResults(self,jsonResult):
        '''
        get the result from the given jsonResult
        '''
        return jsonResult["results"]["bindings"]
    
    def insert(self,insertCommand):
        '''
        run an insert
        '''
        self.sparql.setRequestMethod(POSTDIRECTLY)
        response=self.rawQuery(insertCommand, method=POST)
        return response

    def query(self,queryString,method=POST):
        '''
        get a list of results for the given query
        '''
        queryResult=self.rawQuery(queryString,method=method) 
        jsonResult=queryResult.convert()
        return self.getResults(jsonResult)