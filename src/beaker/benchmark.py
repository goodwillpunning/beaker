import os
import time
import re
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import threading
import datetime
import json
import pandas as pd
from pandas import json_normalize

from beaker.sqlwarehouseutils import SQLWarehouseUtils
from beaker.spark_fixture import get_spark_session, metrics_to_df_view

# Create thread-local storage
thread_local = threading.local()

class Benchmark:
    """Encapsulates a query benchmark test."""

    QUERY_FILE_FORMAT_ORIGINAL = "original"
    QUERY_FILE_FORMAT_SEMICOLON_DELIM = "semicolon-delimited"

    def __init__(
        self,
        name="beaker_benchmark",
        query=None,
        query_file=None,
        query_file_dir=None,
        concurrency=1,
        query_repeat_count=1,
        db_hostname=None,
        warehouse_http_path=None,
        token=None,
        catalog="hive_metastore",
        schema="default",
        new_warehouse_config=None,
        results_cache_enabled=False,
        query_file_format=QUERY_FILE_FORMAT_ORIGINAL,
    ):
        self.name = self._clean_name(name)
        self.query = query
        self.query_file = query_file
        self.query_file_dir = query_file_dir
        self.concurrency = concurrency
        self.query_repeat_count = query_repeat_count
        self.setHostname(db_hostname)
        self.http_path = warehouse_http_path
        self.token = token
        self.catalog = catalog
        self.schema = schema
        self.new_warehouse_config = new_warehouse_config
        self.results_cache_enabled = results_cache_enabled
        # Check if a new SQL warehouse needs to be created
        if new_warehouse_config is not None:
            self.setWarehouseConfig(new_warehouse_config)
        self.query_file_format = query_file_format
        self.sql_warehouse = None

    def _create_dbc(self):
        sql_warehouse = SQLWarehouseUtils(
            self.hostname,
            self.http_path,
            self.token,
            self.catalog,
            self.schema,
            self.results_cache_enabled,
        )
        return sql_warehouse

    def _get_thread_local_connection(self):
        if not hasattr(thread_local, "connection"):
            thread_local.connection = self._create_dbc()
        return thread_local.connection

    def _get_user_id(self):
        """Helper method for filtering query history the current User's Id"""
        response = requests.get(
            f"https://{self.hostname}/api/2.0/preview/scim/v2/Me",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        return response.json()["id"]


    def _validate_warehouse(self, http_path):
        """Validates the SQL warehouse HTTP path."""
        pattern = r'^/sql/1\.0/warehouses/[a-f0-9]+$'
        if re.match(pattern, http_path):
            return True
        else:
            return False

    def _launch_new_warehouse(self):
        """Launches a new SQL Warehouse"""
        warehouse_utils = SQLWarehouseUtils()
        warehouse_utils.setToken(token=self.token)
        warehouse_utils.setHostname(hostname=self.hostname)
        warehouse_utils.setCatalog(self.catalog)
        warehouse_utils.setSchema(self.schema)
        warehouse_utils.setEnableResultCaching(self.results_cache_enabled)
        warehouse_id = warehouse_utils.launch_warehouse(self.new_warehouse_config)
        return warehouse_id

    def _clean_name(self, name):
        """Replaces spaces with underscores"""
        name_lowered_stripped = name.lower().strip()
        return re.sub(r"\s+", "_", name_lowered_stripped)

    def setName(self, name):
        """Sets the name of the Benchmark Test."""
        self.name = self._clean_name(name)

    def setWarehouseConfig(self, config):
        """Launches a new cluster/warehouse from a JSON config."""
        self.new_warehouse_config = config
        logging.info(f"Creating new warehouse with config: {config}")
        self.warehouse_id = self._launch_new_warehouse()
        self.warehouse_name = self._get_warehouse_info()
        self.http_path = f"/sql/1.0/warehouses/{self.warehouse_id}"

    def setWarehouse(self, http_path):
        """Sets the SQL Warehouse http path to use for the benchmark."""
        assert self._validate_warehouse(http_path), "Invalid HTTP path for SQL Warehouse."
        self.http_path = http_path
        if self.http_path:
            self.warehouse_id = self.http_path.split("/")[-1]
            self.warehouse_name = self._get_warehouse_info()

    def stop_warehouse(self, warehouse_id):
        """Stops a SQL warehouse."""
        logging.info(f"Stopping warehouse {warehouse_id}")
        response = requests.post(
            f"https://{self.hostname}/api/2.0/sql/warehouses/{warehouse_id}/stop",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        return response.status_code

    def setConcurrency(self, concurrency):
        """Sets the query execution parallelism."""
        self.concurrency = concurrency

    def setQueryRepeatCount(self, query_repeat_count):
        """Sets the number of times a passed query will be repeatedly run."""
        assert int(query_repeat_count) > 0, "Query repeat count must be > 0."
        self.query_repeat_count = int(query_repeat_count)

    def setHostname(self, hostname):
        """Sets the Databricks workspace hostname."""
        hostname_clean = (
            hostname.strip()
            .replace("http://", "")
            .replace("https://", "")
            .replace("/", "")
            if hostname is not None
            else hostname
        )
        self.hostname = hostname_clean
        logging.info(f"self.hostname = {self.hostname}")

    def setWarehouseToken(self, token):
        """Sets the API token for communicating with the SQL warehouse."""
        self.token = token

    def setCatalog(self, catalog):
        """Set the target Catalog to execute queries."""
        self.catalog = catalog

    def setSchema(self, schema):
        """Set the target schema to execute queries."""
        self.schema = schema

    def setQuery(self, query):
        """Sets a single query to execute."""
        self.query = query

    def setQueryFile(self, query_file):
        """Sets the query file to use."""
        self.query_file = query_file

    def _validateQueryFileDir(self, query_file_dir):
        """Validates the query file directory."""
        return os.path.isdir(query_file_dir)

    def setQueryFileDir(self, query_file_dir):
        """Sets the directory to load query files."""
        assert self._validateQueryFileDir(
            query_file_dir
        ), "Invalid query file directory."
        self.query_file_dir = query_file_dir

    def _execute_single_query(self, query, id=None):
        query = query.strip()
        start_time = time.perf_counter()
        self.sql_warehouse.execute_query(query)
        end_time = time.perf_counter()
        elapsed_time = f"{end_time - start_time:0.3f}"
 
        metrics = {
            "id": id,
            "hostname": self.hostname,
            "http_path": self.http_path,
            "warehouse_name": self.warehouse_name,
            "concurrency": self.concurrency,
            "query": query,
            "elapsed_time": elapsed_time,
        }
        return metrics


    def _set_default_catalog(self):
        if self.catalog:
            query = f"USE CATALOG {self.catalog}"
            self._execute_single_query(query)

    def _set_default_schema(self):
        if self.schema:
            query = f"USE SCHEMA {self.schema}"
            self._execute_single_query(query)

    def _parse_queries(self, raw_queries):
        split_raw = re.split(r"(Q\d+\n+)", raw_queries)[1:]
        split_clean = list(map(str.strip, split_raw))
        headers = split_clean[::2]
        queries = split_clean[1::2]
        # add query_id to the query for history metrics extraction
        queries = [f"--{header.strip()}--\n{query}" for header, query in zip(headers, queries)]
        return headers, queries

    def _get_queries_from_file_format_orig(self, f):
        with open(f, "r") as of:
            raw_queries = of.read()
            file_headers, file_queries = self._parse_queries(raw_queries)
        queries = [e for e in zip(file_queries, file_headers)]
        return queries
    
    def _get_queries_from_file_format_semi(self, file_path):
        """
        Parses a SQL file and returns a list of tuples with the query_id and the query text.

        Parameters:
        file_path (str): The path to the SQL file.

        Returns:
        list: A list of tuples, where each tuple contains a query_id and a query text.
        """
        with open(file_path, 'r') as file:
            content = file.read().strip()

        matches = re.findall(r'--(.*?)--\s*(.*?);', content, re.DOTALL)

        queries = [(f"--{query_id}--\n{query_text.strip()};", query_id) for query_id, query_text in matches]
        return queries


    def _get_queries_from_file(self, query_file):
        if self.query_file_format == self.QUERY_FILE_FORMAT_SEMICOLON_DELIM:
            return self._get_queries_from_file_format_semi(query_file)
        elif self.query_file_format == self.QUERY_FILE_FORMAT_ORIGINAL:
            return self._get_queries_from_file_format_orig(query_file)
        else:
            raise Exception("unknown file format")

    def _execute_queries_from_file(self, query_file):
        queries = self._get_queries_from_file(query_file)
        metrics = self._execute_queries(queries, self.concurrency)
        return metrics

    def _get_query_filenames_from_dir(self, query_file_dir):
        return [os.path.join(query_file_dir, f) for f in os.listdir(query_file_dir)]

    def _get_queries_from_dir(self, query_dir):
        query_files = self._get_query_filenames_from_dir(query_dir)
        queries = []
        for qf in query_files:
            qs = self._get_queries_from_file(qf)
            queries += qs
        return queries

    def _execute_queries_from_dir(self, query_dir):
        queries = self._get_queries_from_dir(query_dir)
        metrics = self._execute_queries(queries, self.concurrency)
        return metrics

    def _execute_queries_from_query(self, query):
        metrics = self._execute_queries([(query, "query")], self.concurrency)
        return metrics

    def _execute_queries(self, queries, num_threads):
        # Duplicate queries `query_repeat_count` number of times
        queries = queries * self.query_repeat_count
        # Create bucketed_queries
        bucketed_queries = [queries[i:i + num_threads] for i in range(0, len(queries), num_threads)]

        metrics_list = []
        for query_bucket in bucketed_queries:
            print(f'Executing {len(query_bucket)} queries concurrently on {self.warehouse_name}')
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = [executor.submit(self._execute_single_query, query, id) for query, id in query_bucket]
                    # executor.map(lambda x: self._execute_single_query(*x), queries)
                # wait(futures, return_when=ALL_COMPLETED)
            metrics_list = metrics_list + [future.result() for future in futures]
        return metrics_list

    def get_query_history(self, warehouse_id, start_ts_ms, end_ts_ms):
        """
        Retrieves the Query History for a given workspace and Data Warehouse.

        Parameters:
        -----------
        warehouse_id (str): The ID of the Data Warehouse for which to retrieve the Query History.
        start_ts_ms (int): The Unix timestamp (milliseconds) value representing the start of the query history.
        end_ts_ms (int): The Unix timestamp (milliseconds) value representing the end of the query history.

        Returns:
        --------
        end_res : query history json
        """
        print(f"Extracting query history {self.warehouse_name} from {start_ts_ms} to {end_ts_ms}")
        user_id = self._get_user_id()
        ## Put together request 
        request_string = {
            "filter_by": {
                "query_start_time_range": {
                    "end_time_ms": end_ts_ms,
                    "start_time_ms": start_ts_ms
            },
            "warehouse_ids": warehouse_id,
            "user_ids": [user_id],
            },
            "include_metrics": "true",
            "max_results": "1000"
        }

        # ## Convert dict to json
        v = json.dumps(request_string)

        uri = f"https://{self.hostname}/api/2.0/sql/history/queries"
        headers_auth = {"Authorization":f"Bearer {self.token}"}

        #### Get Query History Results from API
        response = requests.get(uri, data=v, headers=headers_auth)
        while True:
            results = response.json()['res']
            if all([item['is_final'] for item in results]):
                break
            time.sleep(10)
            response = requests.get(uri, data=v, headers=headers_auth)

        if (response.status_code == 200) and ("res" in response.json()):
            end_res = response.json()['res']
            return end_res
        else:
            raise Exception("Failed to retrieve successful query history")
        
    def clean_query_metrics(self, raw_metrics_pdf):
        logging.info(f"Clean Query Metrics {self.warehouse_name}")
        metrics_pdf = json_normalize(raw_metrics_pdf['metrics'].apply(str).apply(eval))
        metrics_pdf["id"] = raw_metrics_pdf["id"]
        metrics_pdf["query_id"] = raw_metrics_pdf["query_id"]
        metrics_pdf["query_text"] = raw_metrics_pdf["query_text"]
        metrics_pdf["status"] = raw_metrics_pdf["status"]
        metrics_pdf["warehouse_name"] = raw_metrics_pdf["warehouse_name"]

        # Reorder the columns
        metrics_pdf = metrics_pdf.reindex(columns=['id', 'warehouse_name', 'query_text', 'query_id'] + [c for c in metrics_pdf.columns if c not in ['id', 'warehouse_name', 'query_text', 'query_id']])      
        return metrics_pdf

    def _get_warehouse_info(self):
        """Gets the warehouse name as it's not available in the config and in http_path."""
        response = requests.get(
            f"https://{self.hostname}/api/2.0/sql/warehouses/{self.warehouse_id}",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        warehouse_name = response.json()["name"]
        return warehouse_name
    

    def execute(self):
        """Executes the benchmark test."""
        logging.info("Executing benchmark")
        if not self.sql_warehouse:
            self.sql_warehouse = self._get_thread_local_connection()

        self._set_default_catalog()
        self._set_default_schema()
        
        print(f"Monitor warehouse `{self.warehouse_name}` at: ", f"https://{self.hostname}/sql/warehouses/{self.warehouse_id}/monitoring")
        
        start_ts_ms = int(time.time() * 1000)
        start_dt = datetime.datetime.fromtimestamp(start_ts_ms/1000).strftime('%Y-%m-%d %H:%M:%S')

        if self.query_file_dir is not None:
            logging.info("Loading query files from directory.")
            metrics = self._execute_queries_from_dir(self.query_file_dir)
        elif self.query_file is not None:
            logging.info("Loading query file.")
            metrics = self._execute_queries_from_file(self.query_file)
        elif self.query is not None:
            logging.info("Executing single query.")
            metrics = self._execute_queries_from_query(self.query)
        else:
            raise ValueError("No query specified.")

        end_ts_ms = int(time.time() * 1000)

        history_metrics = self.get_query_history(self.warehouse_id, start_ts_ms, end_ts_ms)
        history_pdf = pd.DataFrame(history_metrics)
        history_pdf["warehouse_name"] = self.warehouse_name
        ## Extract query ID from query_text, if query ID is not present, use 'query'
        history_pdf['id'] = history_pdf['query_text'].str.extract(r'--(.*?)--', flags=re.IGNORECASE).fillna('query')
        print("Benchmark completed")
        return history_pdf

    def preWarmTables(self, tables):
        """Delta caches the table before running a benchmark test."""
        assert self.http_path is not None, (
            "No running warehouse. "
            "You can launch a new warehouse by calling `.setWarehouseConfig()`."
        )
        assert (
            self.catalog is not None
        ), "No catalog provided. You can add a catalog by calling `.setCatalog()`."
        
        self.sql_warehouse = self._get_thread_local_connection()
        
        self._set_default_catalog()
        self._set_default_schema()
        print(f"Pre-warming tables in {self.catalog}.{self.schema} on {self.warehouse_name}")
        for table in tables:
            query = f"CACHE SELECT * FROM {table}"
            self._execute_single_query(query)


    def __str__(self):
        object_str = f"""
    Benchmark Test:
    ------------------------
    name={self.name}
    catalog={self.catalog}
    query="{self.query}"
    query_file={self.query_file}
    query_file_dir={self.query_file_dir}
    concurrency={self.concurrency}
    query_repeat_count={self.query_repeat_count}
    hostname={self.hostname}
    warehouse_http_path={self.http_path}
    sql_warehouse={self.sql_warehouse}
    """
        return object_str
