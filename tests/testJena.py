'''
Created on 2020-08-14

@author: wf
'''
import unittest
import getpass
from dg.jena import Jena


class TestJena(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testJena(self):
        '''
        test Apache Jena Fuseki SPARQL endpoint with example data
        '''
        endpoint="http://localhost:3030/example"
        jena=Jena(endpoint)
        queryString = "SELECT * WHERE { ?s ?p ?o. }"
        results=jena.query(queryString)
        self.assertEqual(20,len(results))
        pass
    
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