'''
Created on 23.07.2020

@author: wf
'''
import unittest
from dg.simple import Simple
from dg.dgraph import Dgraph
import urllib.request
import json

class TestDgraph(unittest.TestCase):
    ''' test Dgraph database '''


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testCountries(self):
        ''' 
        test handling countries
        '''
        countryJsonUrl="https://pkgstore.datahub.io/core/country-list/data_json/data/8c458f2d15d9f2119654b29ede6e45b8/data_json.json"
        with urllib.request.urlopen(countryJsonUrl) as url:
            countryDict=json.loads(url.read().decode())
        #print(countryDict)    
        cg=Dgraph(debug=True)
        cg.drop_all()
        schema='''
name: string @index(exact) .
code: string @index(exact) .        
type Country {
   code
   name
}'''
        cg.addSchema(schema)
        for country in countryDict:
            # rename dictionary keys
            country['name']=country.pop('Name')
            country['code']=country.pop('Code')
            country['dgraph.type']='Country'
            print(country) 
            cg.addData(country)
        cg.close
        
    def testSimple(self):
        ''' test dgraph with simple example modified in OO fashion see https://github.com/dgraph-io/pydgraph/blob/master/examples/simple/simple.py'''
        simple=Simple(debug=True)
        simple.drop_all()
        simple.set_schema()
        simple.create_data()
        simple.query_name('Alice')  # query for Alice
        simple.query_name('Bob')  # query for Bob
        simple.delete_data('Bob')  # delete Bob
        simple.query_name('Alice')  # query for Alice
        simple.query_name('Bob')  # query for Bob

        # Close the client stub.
        simple.close()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()