'''
Created on 2020-07-23

@author: wf
'''
import unittest
from dg.simple import Simple
from dg.dgraph import Dgraph
import urllib.request
import json
import time
import getpass

class TestDgraph(unittest.TestCase):
    ''' test Dgraph database '''


    def setUp(self):
        self.host='localhost'
        if getpass.getuser()=="wf":
            self.host='merkur'
        pass

    def tearDown(self):
        pass
    
    def getDGraph(self):
        dgraph=Dgraph(profile=True,host=self.host)
        return dgraph
        
    def testCities(self):
        '''
        test a list of cities
        '''
        cityJsonUrl="https://raw.githubusercontent.com/lutangar/cities.json/master/cities.json"
        with urllib.request.urlopen(cityJsonUrl) as url:
            cityList=json.loads(url.read().decode())
        self.assertEqual(128769,(len(cityList)))
        cityIter=iter(cityList)
        #limit=len(cityList)
        limit=1000
        if getpass.getuser()=="travis":
            limit=4000
        for i in range(limit):
            city=next(cityIter)
            city['dgraph.type']='City'
            lat=float(city['lat'])
            lng=float(city['lng'])
            city['location']={'type': 'Point', 'coordinates': [lng,lat] }
            #print("%d: %s" % (i,city))
        dgraph=self.getDGraph()
        dgraph.drop_all()
        schema='''
name: string @index(exact) .
country: string .   
lat: float .
lng: float .
location: geo .
type City {
   name
   lat
   lng
   location
   country
}'''
        dgraph.addSchema(schema)
        dgraph.addData(obj=cityList,limit=limit,batchSize=250)
        query='''{ 
  # get cities
  cities(func: has(name)) {
        country
        name
        lat
        lng
        location
  }
}
        '''
        queryResult=dgraph.query(query)
        self.assertTrue('cities' in queryResult)
        qCityList=queryResult['cities']
        self.assertEqual(limit,len(qCityList))
        dgraph.close()
            

    def testCountries(self):
        ''' 
        test handling countries
        '''
        countryJsonUrl="https://gist.githubusercontent.com/erdem/8c7d26765831d0f9a8c62f02782ae00d/raw/248037cd701af0a4957cce340dabb0fd04e38f4c/countries.json"
        with urllib.request.urlopen(countryJsonUrl) as url:
            countryList=json.loads(url.read().decode())
        #print(countryList)    
        dgraph=self.getDGraph()
        dgraph.drop_all()
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
        dgraph.addSchema(schema)
        startTime=time.time()
        for country in countryList:
            # rename dictionary keys
            #country['name']=country.pop('Name')
            country['code']=country.pop('country_code')
            country['dgraph.type']='Country'
            lat,lng=country.pop('latlng')
            country['location']={'type': 'Point', 'coordinates': [lng,lat] }
            print(country) 
        dgraph.addData(countryList)
        elapsed=time.time() - startTime
        print("adding %d countries took %5.1f s" % (len(countryList),elapsed)) 
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
        queryResult=dgraph.query(query) 
        self.assertTrue("countries" in queryResult)
        countries=queryResult["countries"]
        self.assertEqual(247,len(countries))
        schemaResult=dgraph.query("schema{}")
        print(schemaResult)
        self.assertTrue("schema" in schemaResult)
        schema=schemaResult["schema"]
        self.assertTrue(len(schema)>=7)
        # see https://discuss.dgraph.io/t/running-upsert-in-python/9364
        """mutation='''
        upsert {  
  query {
    # get the uids of all Country nodes
     countries as var (func: has(<dgraph.type>)) @filter(eq(<dgraph.type>, "Country")) {
        uid
    }
  }
  mutation {
    delete {
      uid(countries) * * .
    }
  }
}'''
        dgraph.mutate(mutation)"""
        dgraph.close
        
    def testSchema(self):   
        '''
        test  schema creation and query
        see https://dgraph.io/tour/schema/1/
        '''
        dgraph=self.getDGraph()
        dgraph.drop_all()
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
        dgraph.addSchema(schemaDefinition)
        schemaResult=dgraph.query("schema{}")
        self.assertTrue("schema" in schemaResult)
        schema=schemaResult["schema"]
        print(schema)
        # There should be 8 schema elements
        self.assertTrue(len(schema)>=7)
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
        dgraph.addData(nquads=nquads)
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
        queryResult=dgraph.query(query)
        self.assertTrue('personCompanyAffiliation' in queryResult)
        pca=queryResult['personCompanyAffiliation']
        print(pca)
        self.assertEqual(5,len(pca))
        dgraph.close
        
    def testSimple(self):
        ''' test dgraph with simple example modified in OO fashion see https://github.com/dgraph-io/pydgraph/blob/master/examples/simple/simple.py'''
        simple=Simple(debug=True,host=self.host)
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