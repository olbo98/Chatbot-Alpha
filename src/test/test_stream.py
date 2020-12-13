import unittest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from stream import Stream

class TestStream(unittest.TestCase):

    def test_map(self):
        plus3 = lambda x: x + 3
        times2 = lambda x: x*2
        result = Stream(range(4)).map(plus3).map(times2).to_list()
        expected = [6, 8, 10, 12]
        self.assertEqual(expected, expected)

    def test_flat_map(self):
        result = Stream([2,5]).flat_map(range).to_list()
        expected = [0,1,0,1,2,3,4]
        self.assertEqual(expected, expected)

    def test_filter(self):
        odd = lambda x: x % 2 == 0
        result = Stream(range(10)).filter(odd).to_list()
        expected = [1,3,5,7,9]
        self.assertEqual(expected, expected)

    def test_peek(self):
        items = []
        def f(x):
            items.append(x)
        Stream(range(10)).peek(f).count()
        expected = list(range(10))
        self.assertEqual(expected, items)

    def test_foreach(self):
        items = []
        def f(x):
            items.append(x)
        Stream(range(10)).foreach(f)
        expected = list(range(10))
        self.assertEqual(expected, items)

    def test_concat(self):
        result = Stream(range(2)).concat(Stream(range(3))).to_list()
        expected = [0,1,0,1,2]
        self.assertEqual(expected, expected)

    def test_take(self):
        result = (Stream(range(10))
            .take(3)
            .to_list())
        self.assertEqual([0,1,2], result)

    def test_drop(self):
        result = (Stream(range(10))
            .drop(7)
            .to_list())
        self.assertEqual([7,8,9], result)

    def test_take_while(self):
        result = (Stream(range(10))
            .take_while(lambda x: x < 3)
            .to_list())
        self.assertEqual([0,1,2], result)

    def test_drop_while(self):
        result = (Stream(range(10))
            .drop_while(lambda x: x < 5)
            .to_list())
        self.assertEqual([5,6,7,8,9], result)

    def test_to_string(self):
        result = Stream(range(3)).to_string(sep=':')
        expected = '0:1:2'
        self.assertEqual(expected, result)

    def test_count(self):
        result = Stream([2,3,4]).count()
        self.assertEqual(3, result)

    def test_reduce(self):
        add = lambda a,b: a + b
        result = Stream(range(3)).reduce(add)
        expected = 0 + 1 + 2
        self.assertEqual(expected, result)

        result = Stream(range(3)).reduce(add, 4)
        expected = 4 + 0 + 1 + 2
        self.assertEqual(expected, result)

if __name__ == '__main__':
    unittest.main()
