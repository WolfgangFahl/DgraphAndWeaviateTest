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
        schemaResult=cg.query("schema{}")
        print(schemaResult)
        cg.close
        
    def testSchema(self):   
        '''
        test  schema creation and query
        see https://dgraph.io/tour/schema/1/
        '''
        cg=Dgraph(debug=True)
        cg.drop_all()
        schemaDefinition='''
        # Define Types

type Person {
    name
    boss_of
    works_for
}

type Company {
    name
    industry
    work_here #this is an alias
}

# Define Directives and index

industry: string @index(term) .
boss_of: [uid] .
name: string @index(exact, term) .
works_for: [uid] .
work_here: [uid] .''' 
        cg.addSchema(schemaDefinition)
        schemaResult=cg.query("schema{}")
        self.assertTrue("schema" in schemaResult)
        schema=schemaResult["schema"]
        print(schema)
        # There should be 7 schema elements
        self.assertEqual(7, len(schema))
        nquads='''_:company1 <name> "CompanyABC" .
    _:company1 <dgraph.type> "Company" .
    _:company2 <name> "The other company" .
    _:company2 <dgraph.type> "Company" .

    _:company1 <industry> "Machinery" .

    _:company2 <industry> "High Tech" .

    _:jack <works_for> _:company1 .
    _:jack <dgraph.type> "Person" .

    _:ivy <works_for> _:company1 .
    _:ivy <dgraph.type> "Person" .

    _:zoe <works_for> _:company1 .
    _:zoe <dgraph.type> "Person" .

    _:jack <name> "Jack" .
    _:ivy <name> "Ivy" .
    _:zoe <name> "Zoe" .
    _:jose <name> "Jose" .
    _:alexei <name> "Alexei" .

    _:jose <works_for> _:company2 .
    _:jose <dgraph.type> "Person" .
    _:alexei <works_for> _:company2 .
    _:alexei <dgraph.type> "Person" .

    _:ivy <boss_of> _:jack .

    _:alexei <boss_of> _:jose .'''
        cg.addData(nquads=nquads)
        query='''
        {
    personCompanyAffiliation(func: has(works_for)) {
    uid
    name
    works_for {
       uid
       name
    }
  }
}
'''
        queryResult=cg.query(query)
        self.assertTrue('personCompanyAffiliation' in queryResult)
        pca=queryResult['personCompanyAffiliation']
        print(pca)
        self.assertEquals(5,len(pca))
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