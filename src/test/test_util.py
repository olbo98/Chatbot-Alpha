import unittest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import util

class TestUtil(unittest.TestCase):

    # implicitly tests read_file and concat as well
    def test_multi_file_streamer(self):
        files = ['testfile1', 'testfile2.bz2']
        lines = list(util.multi_file_streamer(*files))
        expected = [
            'testfile 1, line 1\n', 'testfile 1, line 2\n',
            'testfile 2, line 1\n', 'testfile 2, line 2\n',
        ]
        self.assertEqual(expected, lines)

    def test_compose(self):
        add2 = lambda x: x + 2
        times3 = lambda x: x*3
        composition = util.compose(add2, times3)
        result = list(map(composition, range(4)))
        expected = [6, 9, 12, 15]
        self.assertEqual(expected, result)

    def test_flat_map(self):
        result = list(util.flat_map(range, [1,2,3]))
        expected = [0, 0, 1, 0, 1, 2]
        self.assertEqual(expected, result)

    def test_wrap_body(self):
        f = str.upper
        g = util.wrap(f)
        comment = {'body': 'hello', 'id': 'c123'}
        expected = {'body': 'HELLO', 'id': 'c123'}
        result = g(comment)
        self.assertEqual(expected, result)

    def test_wrap_id(self):
        f = lambda s: int(s[1:])
        g = util.wrap(f, 'id')
        comment = {'body': 'hello', 'id': 'c123'}
        expected = {'body': 'hello', 'id': 123}
        result = g(comment)
        self.assertEqual(expected, result)

    def test_foreach(self):
        items = []
        def f(x):
            items.append(x)
        util.foreach(f, range(3))   # modifies items list
        expected = [0,1,2]
        self.assertEqual(expected, items)

    def test_take(self):
        self.assertEqual([2,3,4], list(util.take(3, range(2,6))))
        self.assertEqual([], list(util.take(0, range(10))))
        self.assertEqual([0,1,2], list(util.take(3, range(3))))
        self.assertEqual([0,1,2], list(util.take(4, range(3))))

    def test_take_while(self):
        lt5 = lambda x: x < 5
        result = list(util.take_while(lt5, range(10)))
        expected = [0,1,2,3,4]
        self.assertEqual(expected, result)

    def test_drop(self):
        self.assertEqual([5,6,7,8,9], list(util.drop(5, range(10))))
        self.assertEqual([0,1,2], list(util.drop(0, range(3))))
        self.assertEqual([], list(util.drop(3, [1,2,3])))
        self.assertEqual([], list(util.drop(4, [1,2,3])))

    def test_drop_while(self):
        lt5 = lambda x: x < 5
        result = list(util.drop_while(lt5, range(10)))
        expected = [5,6,7,8,9]
        self.assertEqual(expected, result)

if __name__ == '__main__':
    unittest.main()
