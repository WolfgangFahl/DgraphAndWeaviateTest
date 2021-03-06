'''
Created on 2020-08-05

@author: wf
'''
import datetime
import json
from storage.dgraph import Dgraph

class Simple(Dgraph):
    '''
    see https://github.com/dgraph-io/pydgraph/blob/master/examples/simple/simple.py
    '''
            
    # Create a schema
    def set_schema(self):
        schema = """
        name: string @index(exact) .
        friend: [uid] @reverse .
        age: int .
        married: bool .
        loc: geo .
        dob: datetime .
        type Person {
            name
            friend
            age
            married
            loc
            dob
        }
        """
        return self.addSchema(schema)
    
    # Create data using JSON.
    def create_data(self):
        
            # Create data.
            p = {
                'uid': '_:alice',
                'dgraph.type': 'Person',
                'name': 'Alice',
                'age': 26,
                'married': True,
                'loc': {
                    'type': 'Point',
                    'coordinates': [1.1, 2],
                },
                'dob': datetime.datetime(1980, 1, 1, 23, 0, 0, 0).isoformat(),
                'friend': [
                    {
                        'uid': '_:bob',
                        'dgraph.type': 'Person',
                        'name': 'Bob',
                        'age': 24,
                    }
                ],
                'school': [
                    {
                        'name': 'Crown Public School',
                    }
                ]
            }
    
            response=self.addData(p)
    
            # Get uid of the outermost object (person named "Alice").
            # response.uids returns a map from blank node names to uids.
            print('Created person named "Alice" with uid = {}'.format(response.uids['alice']))

    # Query for data.
    def query_name(self,name):
        # Run query.
        query = """query all($a: string) {
            all(func: eq(name, $a)) {
                uid
                name
                age
                married
                loc
                dob
                friend {
                    name
                    age
                }
                school {
                    name
                }
            }
        }"""
    
        variables = {'$a': '%s' % name}
        res = self.client.txn(read_only=True).query(query, variables=variables)
        if self.debug:
            print (res.json.decode())
        ppl = json.loads(res.json)
    
        # Print results.
        result='Number of people named "%s": {}' % name
        print(result.format(len(ppl['all'])))

    
    # Deleting a data
    def delete_data(self,name):
        # Create a new transaction.
        txn = self.client.txn()
        try:
            query1 = """query all($a: string) {
                all(func: eq(name, $a)) {
                   uid
                }
            }"""
            variables1 = {'$a': '%s' % name}
            res1 = self.client.txn(read_only=True).query(query1, variables=variables1)
            ppl1 = json.loads(res1.json)
            for person in ppl1['all']:
                print("%s's UID: %s" % (name,person['uid']))
                txn.mutate(del_obj=person)
                print('%s deleted' % name)
            txn.commit()
    
        finally:
            txn.discard()
            
         
        