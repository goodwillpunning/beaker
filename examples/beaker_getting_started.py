# Databricks notebook source
from beaker import Benchmark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a new Benchmark Test

# COMMAND ----------

# Create a new benchmark test
benchmark = Benchmark()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run a Benchmark Test on an Existing SQL Warehouse/Cluster

# COMMAND ----------

import os

# Note: Don't hard-code authentication secrets.
# Instead, consider using environment variables.
host = os.getenv("DATABRICKS_HOST)
http_path = os.getenv("DATABRICKS_HTTP_PATH)
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN)

# Define connection parameters
# Use the builder pattern to add parameters for connecting to the warehouse
benchmark.setHostname(hostname=hostname)
benchmark.setWarehouse(http_path=http_path)
benchmark.setConcurrency(concurrency=1)
benchmark.setWarehouseToken(token=pat)

# Define the query to execute and target Catalog
query_str="""
SELECT count(*)
  FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`
 WHERE passenger_count > 2
"""
benchmark.setQuery(query=query_str)
benchmark.setCatalog(catalog="hive_metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate a query metrics report

# COMMAND ----------

# Run the benchmark!
metrics = benchmark.execute()
metrics.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Benchmark Test on a new SQL Warehouse

# COMMAND ----------

# Launch a new SQL warehouse to execute benchmark queries on
new_warehouse_config = {
  "type": "warehouse",
  "runtime": "latest",
  "size": "Large",
  "min_num_clusters": 1,
  "max_num_clusters": 3,
  "enable_photon": True
}

# Create a new Benchmark Test object
benchmark = Benchmark()
benchmark.setHostname(hostname=hostname)
benchmark.setWarehouseToken(token=pat)
benchmark.setQuery(query_str)
benchmark.setCatalog(catalog="hive_metastore")
benchmark.setWarehouseConfig(new_warehouse_config)

# (Optional) "pre-warm" tables in the Delta Cache (runs a SELECT * to perform a full-scan)
# benchmark.preWarmTables(tables=["table_a", "table_b", "table_c"])

# Run the benchmark!
metrics = benchmark.execute()
metrics.display()

# COMMAND ----------


