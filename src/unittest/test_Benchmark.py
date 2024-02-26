import unittest
import sys
sys.path.append("../")
from beaker import benchmark

class TestBenchmark(unittest.TestCase):
    def setUp(self):
        self.bm = benchmark.Benchmark()

    def test_get_queries_from_file_format_semi(self):
        # Define a test case
        test_file_path = '../../examples/queries/q1.sql'
        # replace with the expected output
        expected_output = [("select 'q1', now()", 'q1')]  

        # Call the function with the test case
        actual_output = self.bm._get_queries_from_file_format_semi(test_file_path)

        # Assert that the actual output matches the expected output
        self.assertEqual(actual_output, expected_output)

    def test_get_queries_from_file_format_orig(self):
        # Define a test case
        test_file_path = '../../examples/queries_orig/q1.sql'
        # replace with the expected output
        expected_output = [("select 'q1', now();", 'Q1')]

        # Call the function with the test case
        actual_output = self.bm._get_queries_from_file_format_orig(test_file_path)

        # Assert that the actual output matches the expected output
        self.assertEqual(actual_output, expected_output)

if __name__ == '__main__':
    unittest.main()