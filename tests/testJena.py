'''
Created on 2020-08-14

@author: wf
'''
import unittest
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


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()