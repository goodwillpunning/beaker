import time
import re
import requests
from functools import reduce
from pyspark.sql import DataFrame
from concurrent.futures import ThreadPoolExecutor
from beaker.sqlwarehouseutils import SQLWarehouseUtils
from beaker.spark_fixture import get_spark_session


class Benchmark:
    """Encapsulates a query benchmark test."""

    def __init__(self, query=None, query_file=None, query_file_dir=None, concurrency=1, db_hostname=None,
                 warehouse_http_path=None, token=None, catalog='hive_metastore', new_warehouse_config=None,
                 results_cache_enabled=False):
        self.query = query
        self.query_file = query_file
        self.query_file_dir = query_file_dir
        self.concurrency = concurrency
        self.hostname = self.setHostname(db_hostname)
        self.http_path = warehouse_http_path
        self.token = token
        self.catalog = catalog
        self.new_warehouse_config = new_warehouse_config
        self.results_cache_enabled = results_cache_enabled
        # Check if a new SQL warehouse needs to be created
        if new_warehouse_config is not None:
            self.setWarehouseConfig(new_warehouse_config)
        self.spark = get_spark_session()

    def _get_user_id(self):
        """Helper method for filtering query history the current User's Id"""
        response = requests.get(
            f"https://{self.hostname}/api/2.0/preview/scim/v2/Me",
            headers={
                "Authorization": f"Bearer {self.token}"
            }
        )
        return response.json()['id']

    def _validate_warehouse(self, http_path):
        """Validates the SQL warehouse HTTP path."""
        return True

    def _launch_new_warehouse(self):
        """Launches a new SQL Warehouse"""
        warehouse_utils = SQLWarehouseUtils()
        warehouse_utils.setToken(token=self.token)
        warehouse_utils.setHostname(hostname=self.hostname)
        return warehouse_utils.launch_warehouse(self.new_warehouse_config)

    def setWarehouseConfig(self, config):
        """Launches a new cluster/warehouse from a JSON config."""
        self.new_warehouse_config = config
        print(f"Creating new warehouse with config: {config}")
        warehouse_id = self._launch_new_warehouse()
        print(f"The warehouse Id is: {warehouse_id}")
        self.http_path = f"/sql/1.0/warehouses/{warehouse_id}"

    def setWarehouse(self, http_path):
        """Sets the SQL Warehouse http path to use for the benchmark."""
        assert self._validate_warehouse(id), "Invalid HTTP path for SQL Warehouse."
        self.http_path = http_path

    def setConcurrency(self, concurrency):
        """Sets the query execution parallelism."""
        self.concurrency = concurrency

    def setHostname(self, hostname):
        """Sets the Databricks workspace hostname."""
        hostname_clean = hostname.strip().replace("http://", "").replace("https://", "")\
            .replace("/", "") if hostname is not None else hostname
        self.hostname = hostname_clean

    def setWarehouseToken(self, token):
        """Sets the API token for communicating with the SQL warehouse."""
        self.token = token

    def setCatalog(self, catalog):
        """Set the target Catalog to execute queries."""
        self.catalog = catalog

    def setQuery(self, query):
        """Sets a single query to execute."""
        self.query = query

    def _validateQueryFile(self, query_file):
        """Validates the query file."""
        return True

    def setQueryFile(self, query_file):
        """Sets the query file to use."""
        assert self._validateQueryFile(query_file), "Invalid query file."
        self.query_file = query_file

    def _validateQueryFileDir(self, query_file_dir):
        """Validates the query file directory."""
        return True

    def setQueryFileDir(self, query_file_dir):
        """Sets the directory to load query files."""
        assert self._validateQueryFileDir(query_file_dir), "Invalid query file directory."
        self.query_file_dir = query_file_dir

    def _execute_single_query(self, query, id=None):
        query = query.strip()
        print(query)
        start_time = time.perf_counter()
        sql_warehouse = SQLWarehouseUtils(self.hostname, self.http_path, self.token)
        sql_warehouse.execute_query(query)
        end_time = time.perf_counter()
        elapsed_time = f"{end_time - start_time:0.3f}"
        metrics = [(id, self.hostname, self.http_path, self.concurrency, query, elapsed_time)]
        metrics_df = self.spark.createDataFrame(metrics,
                                           "id string, hostname string, warehouse string, concurrency int, query_text string, query_duration_secs string")
        return metrics_df

    def _set_default_catalog(self):
        query = f"USE CATALOG {self.catalog}"
        self._execute_single_query(query)

    def _set_results_caching(self):
        """Enables/disables results caching."""
        if not self.results_cache_enabled:
            query = "SET use_cached_result=false"
        else:
            query = "SET use_cached_result=true"
        self._execute_single_query(query)

    def _parse_queries(self, raw_queries):
        split_raw = re.split(r"(Q\d+\n+)", raw_queries)[1:]
        split_clean = list(map(str.strip, split_raw))
        headers = split_clean[::2]
        queries = split_clean[1::2]
        return headers, queries

    def _get_concurrent_queries(self, headers_list, queries_list, max_concurrency):
        """Slices headers and queries into equal bins"""
        for i in range(0, len(queries_list), max_concurrency):
            headers = headers_list[i:(i + max_concurrency)]
            queries = queries_list[i:(i + max_concurrency)]
            yield list(zip(queries, headers))

    def _execute_queries_from_file(self, query_file):
        """Parses a file containing a list of queries to execute on a SQL warehouse."""
        # Keep a list of unique query Ids/headers and query strings
        headers = []
        queries = []

        # Load queries from SQL file
        print(f"Loading queries from file: '{query_file}'")
        query_file_cleaned = query_file.replace("dbfs:/", "/dbfs/")  # Replace `dbfs:` paths

        # Parse the raw SQL, splitting lines into a query identifier (header) and query string
        with open(query_file_cleaned) as f:
            raw_queries = f.read()
            file_headers, file_queries = self._parse_queries(raw_queries)
            headers = headers + file_headers
            queries = queries + file_queries

        # Split the list of queries into buckets determined by specified concurrency
        bucketed_queries_list = list(self._get_concurrent_queries(headers, queries, self.concurrency))
        print(f"There are {len(queries)} total queries.")
        print(f"The concurrency is {self.concurrency}")
        print(f"There are {len(bucketed_queries_list)} buckets of queries")

        # Take each bucket of queries and execute concurrently
        final_metrics_result = []
        for query_bucket in bucketed_queries_list:
            print(f'Executing {len(query_bucket)} queries concurrently.')
            # Multi-thread query execution
            queries_in_bucket = [query_with_header for query_with_header in query_bucket]
            num_threads = len(queries_in_bucket)
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                # Maps the method '_execute_single_query' with a list of queries.
                metrics_list = list(
                    executor.map(lambda query_with_header: self._execute_single_query(*query_with_header),
                                 query_bucket))
                final_metrics_result = final_metrics_result + metrics_list

                # Union together the metrics DFs
        if len(final_metrics_result) > 0:
            final_metrics_df = reduce(DataFrame.unionAll, final_metrics_result)
        else:
            final_metrics_df = self.spark.sparkContext.emptyRDD()
        return final_metrics_df

    def execute(self):
        """Executes the benchmark test."""
        print("Executing benchmark test.")
        # Set which Catalog to use
        self._set_default_catalog()
        # Enable/disable results caching on the SQL warehouse
        # https://docs.databricks.com/sql/admin/query-caching.html
        self._set_results_caching()
        # Query format precedence:
        # 1. Query File Dir
        # 2. Query File
        # 3. Single Query
        if self.query_file_dir is not None:
            print("Loading query files from directory.")
            # TODO: Implement query directory parsing
            # metrics_df = self._execute_queries_from_dir(self.query_file_dir)
            metrics_df = self.spark.sparkContext.emptyRDD
        elif self.query_file is not None:
            print("Loading query file.")
            metrics_df = self._execute_queries_from_file(self.query_file)
        elif self.query is not None:
            print("Executing single query.")
            metrics_df = self._execute_single_query(self.query)
        else:
            raise ValueError("No query specified.")
        return metrics_df

    def preWarmTables(self, tables):
        """Delta caches the table before running a benchmark test."""
        assert self.http_path is not None, "No running warehouse. You can launch a new ware house by calling `.setWarehouseConfig()`."
        assert self.catalog is not None, "No catalog provided. You can add a catalog by calling `.setCatalog()`."
        self._execute_single_query(f"USE CATALOG {self.catalog}")
        for table in tables:
            print(f"Pre-warming table: {table}")
            query = f"SELECT * FROM {table}"
            self._execute_single_query(query)

    def __str__(self):
        object_str = f"""
    Benchmark Test:
    ------------------------
    catalog={self.catalog}
    query="{self.query}"
    query_file={self.query_file}
    query_file_dir={self.query_file_dir}
    concurrency={self.concurrency}
    hostname={self.hostname}
    warehouse_http_path={self.http_path}
    """
        return object_str
