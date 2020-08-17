'''
Created on 2020-08-14

@author: wf
'''
import unittest
import getpass
from dg.jena import Jena
import time
from datetime import datetime,date

class TestJena(unittest.TestCase):
    ''' Test Apache Jena access via Wrapper'''

    def setUp(self):
        self.debug=False
        pass


    def tearDown(self):
        pass

    def getJena(self,mode='query',debug=False,typedLiterals=False):
        '''
        get the jena endpoint for the given mode
        '''
        endpoint="http://localhost:3030/example"
        jena=Jena(endpoint,mode=mode,debug=debug,typedLiterals=typedLiterals)
        return jena

    def testJenaQuery(self):
        '''
        test Apache Jena Fuseki SPARQL endpoint with example SELECT query 
        '''
        jena=self.getJena()
        queryString = "SELECT * WHERE { ?s ?p ?o. }"
        results=jena.query(queryString)
        self.assertTrue(len(results)>20)
        pass
    
    def testJenaInsert(self):
        '''
        test a Jena INSERT DATA
        '''
        jena=self.getJena(mode="update")
        insertCommands = [ """
        PREFIX cr: <http://cr.bitplan.com/>
        INSERT DATA { 
          cr:version cr:author "Wolfgang Fahl". 
        }
        """,'INVALID COMMAND']
        for index,insertCommand in enumerate(insertCommands):
            try:
                result=jena.insert(insertCommand)
                self.assertTrue(index==0)
                print(result)
            except Exception as ex:
                self.assertTrue(index==1)
                msg=ex.args[0]
                self.assertTrue("QueryBadFormed" in msg)
                self.assertTrue("Error 400" in msg)
                pass
            
    def checkErrors(self,errors):      
        if len(errors)>0:
            print("ERRORS:")
            for error in errors:
                print(error)
        self.assertEquals(0,len(errors))    
        
        
    def dob(self,isoDateString):
        ''' get the date of birth from the given iso date state'''
        #if sys.version_info >= (3, 7):
        #    dt=datetime.fromisoformat(isoDateString)
        #else:
        dt=datetime.strptime(isoDateString,"%Y-%m-%d")  
        return dt.date()    
    
    def testDob(self):
        dt=self.dob("1926-04-21")
        self.assertEqual(1926,dt.year)
        self.assertEqual(4,dt.month)
        self.assertEqual(21,dt.day)
            
    def testListOfDictInsert(self):
        '''
        test inserting a list of Dicts using FOAF example
        https://en.wikipedia.org/wiki/FOAF_(ontology)
        
        we use an object oriented derivate of FOAF 
        '''
        listofDicts=[
            {'name': 'Elizabeth Alexandra Mary Windsor', 'born': self.dob('1926-04-21'), 'numberInLine': 0, 'wikidataurl': 'https://www.wikidata.org/wiki/Q9682' },
            {'name': 'Charles, Prince of Wales',         'born': self.dob('1948-11-14'), 'numberInLine': 1, 'wikidataurl': 'https://www.wikidata.org/wiki/Q43274' },
            {'name': 'George of Cambridge',              'born': self.dob('2013-07-22'), 'numberInLine': 3, 'wikidataurl': 'https://www.wikidata.org/wiki/Q1359041'},
            {'name': 'Harry Duke of Sussex',             'born': self.dob('1984-09-15'), 'numberInLine': 5, 'wikidataurl': 'https://www.wikidata.org/wiki/Q152316'}
        ]
        today=date.today()
        for person in listofDicts:
            born=person['born']
            age=(today - born).days / 365.2425
            person['age']=age
            person['ofAge']=age>=18
        typedLiteralModes=[True,False]
        entityType='foafo:Person'
        primaryKey='name'
        prefixes='PREFIX foafo: <http://foafo.bitplan.com/foafo/0.1/>'
        for typedLiteralMode in typedLiteralModes:
            jena=self.getJena(mode='update',typedLiterals=typedLiteralMode,debug=True)
            errors=jena.insertListOfDicts(listofDicts,entityType,primaryKey,prefixes)
            self.checkErrors(errors)
             
        
    def testListOfDictSpeed(self):
        '''
        test the speed of adding data
        ''' 
        listOfDicts=[]
        limit=1000
        for index in range(limit):
            listOfDicts.append({'pkey': "index%d" %index, 'index': "%d" %index})
        jena=self.getJena(mode='update',debug=True)
        entityType="ex:TestRecord"
        primaryKey='pkey'
        prefixes='PREFIX ex: <http://example.com/>'
        startTime=time.time()
        errors=jena.insertListOfDicts(listOfDicts, entityType, primaryKey, prefixes)   
        self.checkErrors(errors)
        elapsed=time.time()-startTime
        print ("adding %d records took %5.3f s => %5.f records/s" % (limit,elapsed,limit/elapsed))
    
    def testLocalWikdata(self):
        '''
        check local wikidata
        '''
        # check we have local wikidata copy:
        if getpass.getuser()=="wf":
            # use 2018 wikidata copy
            endpoint="http://blazegraph.bitplan.com/sparql"
            jena=Jena(endpoint)
            queryString="""
            SELECT ?item ?coord 
WHERE 
{
  # instance of whisky distillery
  ?item wdt:P31 wd:Q10373548.
  # get the coordindate
  ?item wdt:P625 ?coord.
}"""
            results=jena.query(queryString)
            self.assertEqual(238,len(results))


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
