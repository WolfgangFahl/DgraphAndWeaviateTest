'''
Created on 24.07.2020

@author: wf
'''
import unittest
import weaviate
import time

class TestWeaviate(unittest.TestCase):
# https://www.semi.technology/documentation/weaviate/current/client-libs/python.html

    def setUp(self):
        self.port=8080
        self.host="localhost"
        pass
    
    def getClient(self):
        self.client=weaviate.Client("http://%s:%d" % (self.host,self.port))
        return self.client

    def tearDown(self):
        pass
        

    def testWeaviate(self):
        ''' see https://www.semi.technology/documentation/weaviate/current/client-libs/python.html '''
        return
        client = self.getClient()
        try:
            client.create_schema("https://raw.githubusercontent.com/semi-technologies/weaviate-python-client/master/documentation/getting_started/people_schema.json")
        except:
            pass
        entries=[
           [ {"name": "John von Neumann"}, "Person", "b36268d4-a6b5-5274-985f-45f13ce0c642"],
           [ {"name": "Alan Turing"}, "Person", "1c9cd584-88fe-5010-83d0-017cb3fcb446"],
           [ {"name": "Legends"}, "Group", "2db436b5-0557-5016-9c5f-531412adf9c6" ]
        ]
        for entry in entries:
            dict,type,uid=entry
            try:
                client.create(dict,type,uid)
            except weaviate.exceptions.ThingAlreadyExistsException as taee:
                print ("%s already created" % dict['name'])
            
        pass
    
    def testPersons(self):
        return
        w = self.getClient()

        schema = {
        "actions": {"classes": [],"type": "action"},
        "things": {"classes": [{
            "class": "Person",
            "description": "A person such as humans or personality known through culture",
            "properties": [
                {
                    "cardinality": "atMostOne",
                    "dataType": ["text"],
                    "description": "The name of this person",
                    "name": "name"
                }
            ]}],
            "type": "thing"
        }
        }
        w.create_schema(schema)
        
        w.create_thing({"name": "Andrew S. Tanenbaum"}, "Person")
        w.create_thing({"name": "Alan Turing"}, "Person")
        w.create_thing({"name": "John von Neumann"}, "Person")
        w.create_thing({"name": "Tim Berners-Lee"}, "Person")
        
    def testEventSchema(self):    
        return
        schema = {
          "things": {
            "type": "thing",
            "classes": [
              {
                "class": "Event",
                "description": "event",
                "properties": [
                  {
                    "name": "acronym",
                    "description": "acronym",
                    "dataType": [
                      "text"
                    ]
                  },
                  {
                    "name": "inCity",
                    "description": "city reference",
                    "dataType": [
                      "City"
                    ],
                    "cardinality": "many"
                  }
                ]
              },
              {
                "class": "City",
                "description": "city",
                "properties": [
                  {
                    "name": "name",
                    "description": "name",
                    "dataType": [
                      "text"
                    ]
                  },
                  {
                    "name": "hasEvent",
                    "description": "event references",
                    "dataType": [
                      "Event"
                    ],
                    "cardinality": "many"
                  }
                ]
              }
            ]
          }
        }


        client = self.getClient()

        if not client.contains_schema():
            client.create_schema(schema)

        event = {"acronym": "example"}
        client.create(event, "Event", "2a8d56b7-2dd5-4e68-aa40-53c9196aecde")
        city = {"name": "Amsterdam"}
        client.create(city, "City", "c60505f9-8271-4eec-b998-81d016648d85")

        time.sleep(2.0)
        client.add_reference("c60505f9-8271-4eec-b998-81d016648d85", "hasEvent", "2a8d56b7-2dd5-4e68-aa40-53c9196aecde")


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()