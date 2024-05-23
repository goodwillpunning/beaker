import os, sys
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from dotenv import load_dotenv
load_dotenv()

sys.path.append("../src")

from beaker import benchmark, sqlwarehouseutils

hostname = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
# Don't put tokens in plaintext in code
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
catalog_name = "samples"
schema_name = "tpch"

bm = benchmark.Benchmark()
bm.setName(name="simple_test")
bm.setHostname(hostname=hostname)
bm.setWarehouseToken(token=access_token)
bm.setWarehouse(http_path=http_path)
bm.setQueryRepeatCount(2)
bm.setConcurrency(concurrency=10)

print("---- Test prewarm table ------")
bm.setCatalog(catalog_name)
bm.setSchema(schema_name)
tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
# bm.preWarmTables(tables=tables)

bm.query_file_format = "original"
bm.setQueryFileDir("tpch")
metrics_pdf = bm.execute()
print(metrics_pdf)