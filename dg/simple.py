'''
Created on 05.08.2020

@author: wf
'''
import pydgraph
import datetime
import json

class Simple(object):
    '''
    see https://github.com/dgraph-io/pydgraph/blob/master/examples/simple/simple.py
    '''


    def __init__(self, port=9080):
        '''
        Constructor
        '''
        self.port=port
        self.client_stub = pydgraph.DgraphClientStub('localhost:%d' % (port))
        self.client =pydgraph.DgraphClient(self.client_stub)
        
    # Drop All - discard all data and start from a clean slate.
    def drop_all(self):
        return self.client.alter(pydgraph.Operation(drop_all=True))    
    
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
        return self.client.alter(pydgraph.Operation(schema=schema))
    
    # Create data using JSON.
    def create_data(self):
        # Create a new transaction.
        txn = self.client.txn()
        try:
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
    
            # Run mutation.
            response = txn.mutate(set_obj=p)
    
            # Commit transaction.
            txn.commit()
    
            # Get uid of the outermost object (person named "Alice").
            # response.uids returns a map from blank node names to uids.
            print('Created person named "Alice" with uid = {}'.format(response.uids['alice']))
    
        finally:
            # Clean up. Calling this after txn.commit() is a no-op and hence safe.
            txn.discard()


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
            
    def close(self):
        self.client_stub.close()        
        