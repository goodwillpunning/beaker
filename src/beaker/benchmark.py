import os, sys
import time
import re
import requests
import logging
from concurrent.futures import ThreadPoolExecutor

from functools import reduce
#from pyspark.sql import DataFrame

from beaker.sqlwarehouseutils import SQLWarehouseUtils
#from beaker.spark_fixture import get_spark_session


class Benchmark:
    """Encapsulates a query benchmark test."""
    QUERY_FILE_FORMAT_ORIGINAL = "original"
    QUERY_FILE_FORMAT_SEMICOLON_DELIM = "semicolon-delimited"
    
    def __init__(self, name="Beaker Benchmark Test", query=None, query_file=None, query_file_dir=None, concurrency=1, db_hostname=None,
                 warehouse_http_path=None, token=None, catalog='hive_metastore',
                 schema='default',
                 new_warehouse_config=None,
                 results_cache_enabled=False,
                 query_file_format=QUERY_FILE_FORMAT_ORIGINAL):
        self.name = self._clean_name(name)
        self.query = query
        self.query_file = query_file
        self.query_file_dir = query_file_dir
        self.concurrency = concurrency
        #self.hostname = self.setHostname(db_hostname)
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
        #self.spark = get_spark_session()

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

    def _clean_name(self, name):
        """Replaces spaces with underscores"""
        name_lowered_stripped = name.lower().strip()
        return re.sub(r"\s+", "_", name_lowered_stripped)

    def setName(self, name):
        """Sets the name of the Benchmark Test."""
        self.name=self._clean_name(name)

    def setWarehouseConfig(self, config):
        """Launches a new cluster/warehouse from a JSON config."""
        self.new_warehouse_config = config
        logging.info(f"Creating new warehouse with config: {config}")
        warehouse_id = self._launch_new_warehouse()
        logging.info(f"The warehouse Id is: {warehouse_id}")
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
        hostname_clean = hostname.strip().replace("http://", "").replace("https://", "") \
            .replace("/", "") if hostname is not None else hostname
        self.hostname = hostname_clean
        logging.warn(f'self.hostname = {self.hostname}')
        #print(f'self.hostname = {self.hostname}')

    def setWarehouseToken(self, token):
        """Sets the API token for communicating with the SQL warehouse."""
        self.token = token

    def setCatalog(self, catalog):
        """Set the target Catalog to execute queries."""
        self.catalog = catalog

    def setQuery(self, query):
        """Sets a single query to execute."""
        self.query = query

    def setQueryFile(self, query_file):
        """Sets the query file to use."""
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
        logging.info(query)
        start_time = time.perf_counter()
        sql_warehouse = SQLWarehouseUtils(
            self.hostname, self.http_path, self.token, self.schema,
            self.results_cache_enabled)
        sql_warehouse.execute_query(query)
        end_time = time.perf_counter()
        elapsed_time = f"{end_time - start_time:0.3f}"
        #metrics = (id, self.hostname, self.http_path, self.concurrency, query, elapsed_time)
        metrics = {'id':id, 'hostname':self.hostname, 'http_path':self.http_path,
                   'concurrency':self.concurrency, 'query':query,
                   'elapsed_time':elapsed_time}
        #metrics_df = self.spark.createDataFrame(metrics,
        #                                        "id string, hostname string, warehouse string, concurrency int, query_text string, query_duration_secs string")
        #return metrics_df
        return metrics

    def _set_default_catalog(self):
        query = f"USE CATALOG {self.catalog}"
        self._execute_single_query(query)

    def _parse_queries(self, raw_queries):
        split_raw = re.split(r"(Q\d+\n+)", raw_queries)[1:]
        split_clean = list(map(str.strip, split_raw))
        headers = split_clean[::2]
        queries = split_clean[1::2]
        return headers, queries

    def _get_queries_from_file_format_orig(self, f):
        with open(f, 'r') as of:
            raw_queries = of.read()
            file_headers, file_queries = self._parse_queries(raw_queries)
        queries = [e for e in zip(file_queries, file_headers)]
        return queries

    def _get_queries_from_file_format_semi(self, f, filter_comment_lines=False):
        fc = None
        queries = []
        with open(f, 'r') as of:
            fc = of.read()
        for idx, q in enumerate(fc.split(';')):
            q = q.strip()
            if not q: continue
            rq = []
            for l in q.split('\n'):
                if not l.strip():
                    continue
                if filter_comment_lines and l.startswith('--'):
                    print(f'filtering {l}')
                    continue
                rq.append(l)
            if rq:
                queries.append(('\n'.join(rq), f'query{idx}',))
        return queries

    def _get_queries_from_file(self, query_file):
        if self.query_file_format == self.QUERY_FILE_FORMAT_SEMICOLON_DELIM:
            return self._get_queries_from_file_format_semi(query_file)
        elif self.query_file_format == self.QUERY_FILE_FORMAT_ORIGINAL:
            return self._get_queries_from_file_format_orig(query_file)
        else:
            raise Exception('unknown file format')

    def _execute_queries_from_file(self, query_file):
        queries = self._get_queries_from_file(query_file)
        metrics = self._execute_queries(queries, self.concurrency)
        return metrics


    def _get_query_filenames_from_dir(self, queries_dir):
        return [os.path.join(queries_dir, f) for f in os.listdir(queries_dir)]

    def _get_queries_from_dir(self, query_dir):
        query_files = self._get_query_filenames_from_dir(query_dir)
        queries = []
        for qf in query_files:
            queries.append((open(qf).read(), qf.split('/')[-1]))
        return queries

    def _execute_queries_from_dir(self, query_dir):
        queries = self._get_queries_from_dir(query_dir)
        metrics =  self._execute_queries(queries, self.concurrency)
        return metrics

    def _execute_queries_from_query(self, query):
        metrics = self._execute_queries([(query, 'query')], 1)
        return metrics


    def _execute_queries(self, queries, num_threads):
        metrics_list = None
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            metrics_list = list(
                executor.map(lambda x: self._execute_single_query(*x),
                             queries))
        #final_metrics_df = reduce(DataFrame.unionAll, metrics_list)
        #return final_metrics_df
        return metrics_list


    def execute(self):
        """Executes the benchmark test."""
        logging.info("Executing benchmark test.")
        # Set which Catalog to use
        self._set_default_catalog()
        metrics = None
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
        return metrics

    def preWarmTables(self, tables):
        """Delta caches the table before running a benchmark test."""
        assert self.http_path is not None, "No running warehouse. " \
                                           "You can launch a new ware house by calling `.setWarehouseConfig()`."
        assert self.catalog is not None, "No catalog provided. You can add a catalog by calling `.setCatalog()`."
        self._execute_single_query(f"USE CATALOG {self.catalog}")
        for table in tables:
            logging.info(f"Pre-warming table: {table}")
            query = f"SELECT * FROM {table}"
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
    hostname={self.hostname}
    warehouse_http_path={self.http_path}
    """
        return object_str
