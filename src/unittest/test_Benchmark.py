import unittest
import sys
from dotenv import load_dotenv
import os

sys.path.append("../")
from beaker import benchmark

load_dotenv("../examples/.env")

hostname = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
# Don't put tokens in plaintext in code
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
catalog_name = os.getenv("CATALOG")
schema_name = os.getenv("SCHEMA")

class TestBenchmark(unittest.TestCase):
    def setUp(self):
        self.bm = benchmark.Benchmark()
        self.bm.setName(name="unittest")
        self.bm.setHostname(hostname=hostname)
        self.bm.setWarehouseToken(token=access_token)
        self.bm.setWarehouse(http_path=http_path)

    def test_get_queries_from_file_format_semi(self):
        # Define a test case
        test_file_path = '../../examples/queries/q10.sql'
        # replace with the expected output
        expected_output = [("select 'q10', now()", 'q10')]  

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

    def test_get_queries_from_dir_orig(self):
        # Define a test case
        test_dir_path = '../../examples/queries_orig/'
        # replace with the expected output
        expected_output = [("select 'q1', now();", 'Q1'), ("select 'q2', now();", 'Q2')]

        # Call the function with the test case
        self.bm.query_file_format = "original"
        actual_output = self.bm._get_queries_from_dir(test_dir_path)

        # Assert that the actual output matches the expected output
        self.assertEqual(actual_output, expected_output)
    
    def test_get_queries_from_dir_semi(self):
        # Define a test case
        test_dir_path = '../../examples/queries/'
        # replace with the expected output
        expected_output = [("select 'q1', now()", 'q1'), ("select 'q2', now()", 'q2'), ("select 'q10', now()", 'q10')]

        # Call the function with the test case
        self.bm.query_file_format = "semicolon-delimited"
        actual_output = self.bm._get_queries_from_dir(test_dir_path)
        # Assert that the actual output matches the expected output
        self.assertEqual(actual_output, expected_output)

    def test_validate_warehouse(self):
        # Define a test case
        test_http_path = "/sql/1.0/warehouses/632c5da7a7fd6a78"
        # replace with the expected output
        expected_output = True

        # Call the function with the test case
        actual_output = self.bm._validate_warehouse(test_http_path)

        # Assert that the actual output matches the expected output
        self.assertEqual(actual_output, expected_output)

        # Define a test case
        test_http_path2 = "/sql/1.0/warehouses632c5da7a7fd"
        # replace with the expected output
        expected_output2 = False

        # Call the function with the test case
        actual_output2 = self.bm._validate_warehouse(test_http_path2)

        # Assert that the actual output matches the expected output
        self.assertEqual(actual_output2, expected_output2)


if __name__ == '__main__':
    unittest.main()