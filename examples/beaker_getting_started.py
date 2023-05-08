# Databricks notebook source
# MAGIC %pip install databricks-sql-connector

# COMMAND ----------

import os, sys
import logging

logging.getLogger().setLevel(logging.INFO)

# COMMAND ----------

from databricks import sql

# COMMAND ----------

# change as required to import the beaker module
sys.path.append(f'{os.getcwd()}/../src')

# COMMAND ----------

from beaker import benchmark
from beaker import spark_fixture

# COMMAND ----------

# reload for changes in benchmark
import importlib
importlib.reload(benchmark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a new Benchmark Test

# COMMAND ----------

# Create a new benchmark test
bm = benchmark.Benchmark()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run a Benchmark Test on an Existing SQL Warehouse/Cluster

# COMMAND ----------

# Change hostname and http_path to your dbsql warehouse
hostname = 'e2-demo-field-eng.cloud.databricks.com'
http_path = '/sql/1.0/warehouses/ead10bf07050390f'

# Add the appropriate scope and key for your token
pat = dbutils.secrets.get(scope='db-field-eng', key='e2-demo-db-access-token')

# COMMAND ----------

# Define connection parameters
# Use the builder pattern to add parameters for connecting to the warehouse
bm.setName(name="simple_test")
bm.setHostname(hostname=hostname)
bm.setWarehouse(http_path=http_path)
bm.setConcurrency(concurrency=1)
bm.setWarehouseToken(token=pat)

# Define the query to execute and target Catalog
query_str="""
SELECT count(*)
  FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`
 WHERE passenger_count > 2
"""
bm.setQuery(query=query_str)
bm.setCatalog(catalog="hive_metastore")

# COMMAND ----------

# Run the benchmark!
metrics = bm.execute()

# COMMAND ----------

metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a spark dataframe of the returned metrics and a view

# COMMAND ----------

df_simple_test = spark_fixture.metrics_to_df_view(metrics, 'simple_test_vw')
df_simple_test.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM simple_test_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Benchmark Test on a new SQL Warehouse

# COMMAND ----------

import importlib
importlib.reload(benchmark)

# COMMAND ----------

new_warehouse_config = {
  "type": "warehouse",
  "runtime": "latest",
  "size": "Large",
  "min_num_clusters": 1,
  "max_num_clusters": 3,
  "enable_photon": True
}

# Create a new Benchmark Test object
bm = benchmark.Benchmark()
bm.setHostname(hostname=hostname)
bm.setWarehouseToken(token=pat)
bm.setQuery(query_str)
bm.setCatalog(catalog="hive_metastore")
bm.setWarehouseConfig(new_warehouse_config)

# COMMAND ----------

# (Optional) "pre-warm" tables in the Delta Cache (runs a SELECT * to perform a full-scan)
# benchmark.preWarmTables(tables=["table_a", "table_b", "table_c"])

# Run the benchmark!
metrics = bm.execute()
print(metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Multiple Queries Concurrently
# MAGIC `Beaker` was created with concurrency in mind. For example, it's useful for answering questions like, "How will a SQL warehouse perform under peak, interactive usage?". 
# MAGIC
# MAGIC You can test concurrent query execution by listing the benchmark queries in a **file**.
# MAGIC
# MAGIC The query file format should follow the format:
# MAGIC
# MAGIC ```
# MAGIC -- a unique query identifier (header) followed by a newline
# MAGIC Q1
# MAGIC
# MAGIC -- the query body followed by a new line
# MAGIC SELECT * FROM us_population_2016 WHERE state in ('DE', 'MD', 'VA');
# MAGIC ```

# COMMAND ----------

# First, create a query file using the format above
dbutils.fs.put("file:/tmp/my_query_file.sql", """
Q1

SELECT count(*)
  FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`
 WHERE passenger_count = 1
 
Q2

SELECT count(*)
  FROM delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`
 WHERE passenger_count > 2
""", overwrite=True)

# COMMAND ----------

bm = benchmark.Benchmark()
bm.setName(name="concurrency_test")
bm.setHostname(hostname=hostname)
bm.setWarehouse(http_path=http_path)
bm.setWarehouseToken(token=pat)

# Next, define the query file path and the parallelism
bm.setConcurrency(concurrency=2)  # execute both queries in parallel
bm.setQueryFile(query_file="/tmp/my_query_file.sql")
bm.setCatalog(catalog="hive_metastore")

# COMMAND ----------

metrics = bm.execute()
print(metrics)

# COMMAND ----------


