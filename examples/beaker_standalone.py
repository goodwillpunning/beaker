import os, sys
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from dotenv import load_dotenv
load_dotenv()

sys.path.append("../src")

from beaker import benchmark

hostname = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
# Don't put tokens in plaintext in code
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
catalog_name = os.getenv("CATALOG")
schema_name = os.getenv("SCHEMA")

print(http_path)

bm = benchmark.Benchmark()
bm.setName(name="simple_test")
bm.setHostname(hostname=hostname)
bm.setWarehouseToken(token=access_token)
bm.setWarehouse(http_path=http_path)
bm.setConcurrency(concurrency=1)

print("---- Specify query in code ------")
query_str = """
SELECT count(*)
  FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`
 WHERE passenger_count > 2;
"""
bm.setQuery(query=query_str)
bm.setCatalog(catalog="hive_metastore")
bm.setSchema(schema="default")
metrics_pdf = bm.execute()
print(metrics_pdf)


print("---- Specify a single query file ------")
bm.query_file_format = "semicolon-delimited"
bm.setQueryFile("queries/q1.sql")
metrics_pdf = bm.execute()
print(metrics_pdf)


print("---- Specify a query directory semicolon format------")
bm.query_file_format = "semicolon-delimited"
bm.setQueryFileDir("queries")
metrics_pdf = bm.execute()
print(metrics_pdf)


print("---- Specify a query directory original format------")
bm.query_file_format = "original"
bm.setQueryFileDir("queries_orig")
metrics_pdf = bm.execute()
print(metrics_pdf)


print("---- Test prewarm table ------")
bm.setCatalog(catalog_name)
bm.setSchema(schema_name)
tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
bm.preWarmTables(tables=tables)