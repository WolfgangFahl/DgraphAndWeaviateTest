'''
Created on 2020-08-10

@author: wf
'''
import pydgraph
import json

class Dgraph(object):
    '''
    wrapper for https://dgraph.io/ GraphQL database
    '''

    def __init__(self, host='localhost',port=9080, debug=False):
        '''
        Constructor
        '''
        self.host=host
        self.port=port
        
        self.debug=debug
        self.client_stub = pydgraph.DgraphClientStub('%s:%d' % (host,port))
        self.client =pydgraph.DgraphClient(self.client_stub)
        
    def addSchema(self,schema):
        '''
        add the given schema
        '''
        result=self.client.alter(pydgraph.Operation(schema=schema))
        return result
    
    def addData(self,obj):
        '''
        add the given object
        '''
        response=None
        # Create a new transaction.
        txn = self.client.txn()
        
        try:
            # Run mutation.
            response = txn.mutate(set_obj=obj)
            # Commit transaction.
            txn.commit()
        finally:
            if self.debug:
                print(obj)
            # Clean up. Calling this after txn.commit() is a no-op and hence safe.
            txn.discard()
        return response
    
    def query(self,graphQuery):
        '''
        do a query 
        '''
        response = self.client.txn(read_only=True).query(graphQuery)
        if self.debug:
            print (response.json.decode())
        result = json.loads(response.json)
        return result
        
    def drop_all(self):
        ''' Drop All - discard all data and start from a clean state.'''
        return self.client.alter(pydgraph.Operation(drop_all=True))
    
    def close(self):
        ''' close the client '''
        self.client_stub.close()
        