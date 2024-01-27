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

bm = benchmark.Benchmark()

hostname = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
# Don't put tokens in plaintext in code
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")

bm.setHostname(hostname=hostname)
bm.setWarehouse(http_path=http_path)
bm.setConcurrency(concurrency=5)
bm.setWarehouseToken(token=access_token)

bm.setQuery("select now(), 'foo';")
bm.setQueryRepeatCount(100)

beaker_metrics, history_metrics = bm.execute()
#print(history_metrics)

