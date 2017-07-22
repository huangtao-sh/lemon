import unittest
class TestSample(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_sample(self):
        a='this is a test.'
        self.assertEqual(a,a)
