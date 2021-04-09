import unittest
import weaviate
import time
#import getpass

class TestWeaviate(unittest.TestCase):
# https://www.semi.technology/documentation/weaviate/current/client-libs/python.html

    def setUp(self):
        self.port=8080
        self.host="localhost"
        #if getpass.getuser()=="wf":
        #    self.host="zeus"
        #    self.port=8080
        pass
    
    def getClient(self):
        self.client=weaviate.Client("http://%s:%d" % (self.host,self.port))
        return self.client

    def tearDown(self):
        pass
        
    def testRunning(self):
        '''
        make sure weaviate is running
        '''
        w=self.getClient()
        self.assertTrue(w.is_live())
        self.assertTrue(w.is_ready())
            

    def testWeaviateSchema(self):
        ''' see https://www.semi.technology/documentation/weaviate/current/client-libs/python.html '''
        w = self.getClient()
        if w.schema.contains():
            w.schema.delete_all()
        try:
            w.schema.create("https://raw.githubusercontent.com/semi-technologies/weaviate-python-client/master/documentation/getting_started/people_schema.json") # instead of w.create_schema, see https://www.semi.technology/documentation/weaviate/current/how-tos/how-to-create-a-schema.html#creating-your-first-schema-with-the-python-client
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
                w.data_object.create(dict,type,uid) # instead of w.create(dict,type,uid), see https://www.semi.technology/documentation/weaviate/current/restful-api-references/semantic-kind.html#example-request-1
            except weaviate.exceptions.ThingAlreadyExistsException as taee:
                print ("%s already created" % dict['name'])
            
        pass
    
    def testPersons(self):
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
        w.schema.create(schema) # instead of w.create_schema(schema)
        
        w.data_object.create({"name": "Andrew S. Tanenbaum"}, "Person") # instead of  w.create_thing({"name": "Andrew S. Tanenbaum"}, "Person")
        w.data_object.create({"name": "Alan Turing"}, "Person")
        w.data_object.create({"name": "John von Neumann"}, "Person")
        w.data_object.create({"name": "Tim Berners-Lee"}, "Person")
        
    def testEventSchema(self):    
        '''
        https://stackoverflow.com/a/63077495/1497139
        '''
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

        if not client.schema.contains(schema):
            client.schema.create(schema) # instead of client.create_schema(schema)

        event = {"acronym": "example"}
        client.data_object.create(event, "Event", "2a8d56b7-2dd5-4e68-aa40-53c9196aecde") # instead of client.create(event, "Event", "2a8d56b7-2dd5-4e68-aa40-53c9196aecde")
        city = {"name": "Amsterdam"}
        client.data_object.create(city, "City", "c60505f9-8271-4eec-b998-81d016648d85")

        time.sleep(2.0)
        client.data_object.reference.add("c60505f9-8271-4eec-b998-81d016648d85", "hasEvent", "2a8d56b7-2dd5-4e68-aa40-53c9196aecde") # instead of client.add_reference("c60505f9-8271-4eec-b998-81d016648d85", "hasEvent", "2a8d56b7-2dd5-4e68-aa40-53c9196aecde"), see https://www.semi.technology/documentation/weaviate/current/restful-api-references/semantic-kind.html#add-a-cross-reference


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()