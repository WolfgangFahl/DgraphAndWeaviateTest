'''
Created on 2020-08-14

@author: wf
'''
import unittest
import getpass
from dg.jena import Jena


class TestJena(unittest.TestCase):
    ''' Test Apache Jena access '''

    def setUp(self):
        pass


    def tearDown(self):
        pass

    def getJena(self,mode='query'):
        endpoint="http://localhost:3030/example/%s" % mode
        jena=Jena(endpoint)
        return jena

    def testJenaQuery(self):
        '''
        test Apache Jena Fuseki SPARQL endpoint with example data
        '''
        jena=self.getJena()
        queryString = "SELECT * WHERE { ?s ?p ?o. }"
        results=jena.query(queryString)
        self.assertTrue(len(results)>20)
        pass
    
    def testJenaInsert(self):
        jena=self.getJena(mode="update")
        insertString = """
        PREFIX cr: <http://cr.bitplan.com/>
        INSERT DATA { 
          cr:version cr:author "Wolfgang Fahl". 
        }
        """
        results=jena.rawQuery(insertString)
        print (results)
    
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