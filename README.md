# Beaker 🧪
Execute query benchmark tests against Databricks SQL warehouses and clusters.

<img src="./assets/images/beaker.png" width="200">

## Getting Started
You can create a new Benchmark test by passing in the parameters to the constructor or set the parameters later.

```python
# First, create a new Benchmark object
from src.beaker import *

benchmark = Benchmark()
```

The Benchmark class uses a builder pattern to specify the test parameters.
```python
benchmark.setHostname(hostname=hostname)
# HTTP path to an existing warehouse/cluster
benchmark.setWarehouse(http_path=http_path)
benchmark.setConcurrency(concurrency=10)
benchmark.setWarehouseToken(token=pat)
benchmark.setQuery(query=query)
benchmark.setCatalog(catalog="hive_metastore")
benchmark.preWarmTables(tables=["table_1", "table_2", "table_3"])
```

You may even choose to provision a new SQL warehouse.
```python
new_warehouse_config = {
    "type": "warehouse",
    "runtime": "latest",
    "size": "Large",
    "min_num_clusters": 1,
    "max_num_clusters": 3,
    "enable_photon": True
}
benchmark.setWarehouseConfig(new_warehouse_config)
```

Finally, calling the `.execute()` function runs the benchmark test.
```python
# Run the benchmark!
metrics = benchmark.execute()
metrics.display()
```
## Authentication
`beaker` connects to the SQL warehouse or cluster using the Databricks REST API 2.0. As a result, connection information is needed.

It's recommended that you do not hard-code authentication secrets. Instead consider using environment variables.

Example usage:

```shell
export DATABRICKS_HOST=<workspace-hostname>.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/endpoints/<warehouse-id>
export DATABRICKS_TOKEN=dapi01234567890
```

```python
import os
from beaker import Benchmark

hostname = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
token = os.getenv("DATABRICKS_ACCESS_TOKEN")

benchmark = Benchmark(hostname=hostname, http_path=http_path, token=token)
```                
                
## Setting the benchmark queries to execute
Beaker can execute benchmark queries is several formats:
1. Execute a single query
```benchmark.setQuery(query=query)```
2. Execute several queries from a file
```benchmark.setQueryFile(query_file=query_file)```
3. Execute several query files given a local directory
```benchmark.setQueryFileDir(query_file_dir=query_file_dir)```

However, if multiple query formats are provided, the following query format precedence will be followed:
1. **Query File Dir** - if a local directory is provided then Beaker will parse all query files under the directory
2. **Query File** - if no query directory is provided, but a query file is, then Beaker will parse the query file
3. **Single Query** - if no query directory or query file is provided, then Beaker will execute a single query

### Query file format
The query file must contain queries that are separated using the following format:

```sql
-- a unique query identifier (header) followed by a newline
Q1

-- the query body followed by a new line
SELECT * FROM us_population_2016 WHERE state in ('DE', 'MD', 'VA');

```

## Viewing the metrics report
Once all benchmark queries have been executed, Beaker will consolidate metrics of each query execution into a single Spark DataFrame.
A temporary view is also created, to make querying the output and building local visualizations easier. 

The name of the view has the following format: `{name_of_benchmark}_vw`

<img src="./assets/images/metrics_visualization.png" />

## Contributing
Please halp! Drop me a line at: will.girten@databricks.com if you're interested.

## Legal Information
This software is provided as-is and is not officially supported by Databricks through customer technical support channels. Support, questions, and feature requests can be submitted through the Issues page of this repo. Please see the [legal agreement](LICENSE) and understand that issues with the use of this code will not be answered or investigated by Databricks Support.