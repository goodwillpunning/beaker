# Test the dist by running this script.
# It tests:
# 1. The import of beaker.benchmark
# 2. That concurrency and the connection per thread work.
#
# You can run it like this:
# Copy this file to the python env where you have pip installed the dist.
# > python standalone_dist_test.py

import os, sys
from beaker import benchmark

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


hostname = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
# Don't put tokens in plaintext in code
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")

bm = benchmark.Benchmark()
bm.setName(name="standalone_dist_test")
bm.setHostname(hostname=hostname)
bm.setWarehouseToken(token=access_token)
bm.setWarehouse(http_path=http_path)
bm.setConcurrency(concurrency=5)

bm.setQuery("select now(), 'foo';")
bm.setCatalog(catalog="hive_metastore")
bm.setQueryRepeatCount(10)

metrics_pdf = bm.execute()
print(metrics_pdf)

