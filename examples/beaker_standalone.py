import os, sys
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sys.path.append('../src')

from beaker import benchmark

bm = benchmark.Benchmark()

hostname = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
# Don't put tokens in plaintext in code
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")

bm.setName(name="simple_test")
bm.setHostname(hostname=hostname)
bm.setWarehouse(http_path=http_path)
bm.setConcurrency(concurrency=1)
bm.setWarehouseToken(token=access_token)

print("---- Specify query in code ------")
query_str="""
SELECT count(*)
  FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`
 WHERE passenger_count > 2
"""
bm.setQuery(query=query_str)
bm.setCatalog(catalog="hive_metastore")

metrics = bm.execute()
print(metrics)


print("---- Specify a single query file ------")
bm.query_file_format = "semicolon-delimited"
bm.setQueryFile('queries/q1.sql')
metrics = bm.execute()
print(metrics)


print("---- Specify a query directory ------")
bm.setQueryFileDir('queries')
metrics = bm.execute()
print(metrics)
