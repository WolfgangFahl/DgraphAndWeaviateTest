'''
Created on 2020-08-24

@author: wf
'''

class Sample(object):
    '''
    Sample dataset generator
    '''


    def __init__(self):
        '''
        Constructor
        '''
        
    @staticmethod         
    def getSample(size):         
        listOfDicts=[]
        for index in range(size):
            listOfDicts.append({'pkey': "index%d" %index, 'index': index})
        return listOfDicts    
        