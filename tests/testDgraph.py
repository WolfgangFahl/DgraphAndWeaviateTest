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
        countryJsonUrl="https://gist.githubusercontent.com/erdem/8c7d26765831d0f9a8c62f02782ae00d/raw/248037cd701af0a4957cce340dabb0fd04e38f4c/countries.json"
        with urllib.request.urlopen(countryJsonUrl) as url:
            countryDict=json.loads(url.read().decode())
        #print(countryDict)    
        cg=Dgraph(debug=True)
        cg.drop_all()
        schema='''
name: string @index(exact) .
code: string @index(exact) .     
capital: string .   
location: geo .
type Country {
   code
   name
   location
   capital
}'''
        cg.addSchema(schema)
        for country in countryDict:
            # rename dictionary keys
            #country['name']=country.pop('Name')
            country['code']=country.pop('country_code')
            country['dgraph.type']='Country'
            lat,lng=country.pop('latlng')
            country['location']={'type': 'Point', 'coordinates': [lng,lat] }
            print(country) 
            cg.addData(country)
            
        query='''{
# list of countries
  countries(func: has(code)) {
    uid
    name
    code
    capital
    location
  }
}'''
        queryResult=cg.query(query) 
        self.assertTrue("countries" in queryResult)
        countries=queryResult["countries"]
        self.assertEqual(247,len(countries))
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