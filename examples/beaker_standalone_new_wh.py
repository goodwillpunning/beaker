import os, sys
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from dotenv import load_dotenv
load_dotenv()

sys.path.append("../src")

from beaker import benchmark, sqlwarehouseutils

hostname = os.getenv("DATABRICKS_HOST")
# Don't put tokens in plaintext in code
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
catalog_name = os.getenv("CATALOG")
schema_name = os.getenv("SCHEMA")

print("---- SPECIFY NEW WAREHOUSE ------")

new_warehouse_config = {
    "type": "warehouse",
    "runtime": "latest",
    "size": "2X-Small",
    "warehouse": "serverless",
    "min_num_clusters": 1,
    "max_num_clusters": 3,
    "enable_photon": True,
}

# Create a new Benchmark Test object
bm = benchmark.Benchmark()
bm.setName(name="new_warehouse_test")
bm.setHostname(hostname=hostname)
bm.setWarehouseToken(token=access_token)
bm.setWarehouseConfig(new_warehouse_config)

print("---- Specify query in code ------")
query_str = """
SELECT count(*)
  FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`
 WHERE passenger_count > 2;
"""
bm.setQuery(query=query_str)
bm.setCatalog(catalog="hive_metastore")
bm.setSchema(schema="default")
bm.setQueryRepeatCount(2)
bm.setConcurrency(2)
metrics_pdf = bm.execute()
print(metrics_pdf)

