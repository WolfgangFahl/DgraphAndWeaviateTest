'''
Created on 2020-08-13

@author: wf
'''
import unittest
import time
from storage.ruruki import Ruruki

class TestRuruki(unittest.TestCase):
    '''
    test the ruruki in memory graph database
    '''


    def setUp(self):
        self.debug=True
        pass


    def tearDown(self):
        pass


    def testRuruki(self):
        r=Ruruki()
        r.graph.add_vertex_constraint("person", "name")
        r.graph.add_vertex_constraint("book", "name")
        r.graph.add_vertex_constraint("author", "name")
        r.graph.add_vertex_constraint("category", "name")
        # add the categories
        programming = r.graph.get_or_create_vertex("category", name="Programming")
        operating_systems = r.graph.get_or_create_vertex("category", name="Operating Systems")
        
        # add some books
        python_crash_course = r.graph.get_or_create_vertex("book", title="Python Crash Course")
        python_pocket_ref = r.graph.get_or_create_vertex("book", title="Python Pocket Reference")
        how_linux_works = r.graph.get_or_create_vertex("book", title="How Linux Works: What Every Superuser Should Know", edition="second")
        linux_command_line = r.graph.get_or_create_vertex("book", title="The Linux Command Line: A Complete Introduction", edition="first")
        
        # add a couple authors of the books above
        eric_matthes = r.graph.get_or_create_vertex("author", fullname="Eric Matthes", name="Eric", surname="Matthes")
        mark_lutz = r.graph.get_or_create_vertex("author", fullname="Mark Lutz", name="Mark", surname="Lutz")
        brian_ward = r.graph.get_or_create_vertex("author", fullname="Brian Ward", name="Brian", surname="Ward")
        william = r.graph.get_or_create_vertex("author", fullname="William E. Shotts Jr.", name="William", surname="Shotts")
        
        # add some random people
        john = r.graph.get_or_create_vertex("person", name="John", surname="Doe")
        jane = r.graph.get_or_create_vertex("person", name="Jane", surname="Doe")
        # link the books to a category
        r.graph.get_or_create_edge(python_crash_course, "CATEGORY", programming)
        r.graph.get_or_create_edge(python_pocket_ref, "CATEGORY", programming)
        r.graph.get_or_create_edge(linux_command_line, "CATEGORY", operating_systems)
        r.graph.get_or_create_edge(how_linux_works, "CATEGORY", operating_systems)
        
        # link the books to their authors
        r.graph.get_or_create_edge(python_crash_course, "BY", eric_matthes)
        r.graph.get_or_create_edge(python_pocket_ref, "BY", mark_lutz)
        r.graph.get_or_create_edge(how_linux_works, "BY", brian_ward)
        r.graph.get_or_create_edge(linux_command_line, "BY", william)
        
        # Create some arbitrary data between John and Jane Doe.
        r.graph.get_or_create_edge(john, "READING", python_crash_course)
        r.graph.get_or_create_edge(john, "INTEREST", programming)
        r.graph.get_or_create_edge(jane, "LIKE", operating_systems)
        r.graph.get_or_create_edge(jane, "MARRIED-TO", john)
        r.graph.get_or_create_edge(jane, "READING", linux_command_line)
        r.graph.get_or_create_edge(jane, "READING", python_pocket_ref)
        personVertices=r.graph.get_vertices("person").all()
        self.assertEqual(2,len(personVertices))
        pass
    
    def testSpeed(self):
        '''
        test the speed of vertex creation
        '''
        r=Ruruki()
        r.graph.add_vertex_constraint("node", "id")
        startTime=time.time()
        limit=1000
        for i in range(1000):
            r.graph.get_or_create_vertex("node",id="%d" % i)
        needed=time.time()-startTime
        print ("creating %d vertices took %5.1f s = %5.1f v/s" % (limit,needed,limit/needed))    
         


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()