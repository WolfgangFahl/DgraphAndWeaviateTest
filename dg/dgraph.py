'''
Created on 2020-08-10

@author: wf
'''
import pydgraph

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
        
    def drop_all(self):
        ''' Drop All - discard all data and start from a clean state.'''
        return self.client.alter(pydgraph.Operation(drop_all=True))
    
    def close(self):
        ''' close the client '''
        self.client_stub.close()
        