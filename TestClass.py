import unittest
from atomic_writing import atomic_writing

class TestClass(unittest.TestCase):
    
    def test_atomic_writing(self):
        print("start unit test")
        test = atomic_writing()
        self.assertEqual(test,True)

    
if __name__ == '__main__':
    unittest.main()