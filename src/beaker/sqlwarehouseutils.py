import requests

from databricks import sql

class SQLWarehouseUtils:

    _LATEST_RUNTIME = '11.3.x-photon-scala2.12'
    _CLUSTER_SIZES = ["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"]

    def __init__(self, hostname=None, warehouse_http_path=None, token=None, schema='default', enable_results_caching=False):
        #print("SQLWarehouseUtils.__init__(%s)", (locals(),))
        #print(f"setting hostname = {hostname}")
        self.hostname=hostname
        self.http_path=warehouse_http_path
        self.access_token=token
        self.schema=schema
        self.enable_results_caching=enable_results_caching

    def _get_connection(self):
        # Enable/disable results caching on the SQL warehouse
        # https://docs.databricks.com/sql/admin/query-caching.html
        if self.enable_results_caching:
            results_caching = "true"
        else:
            results_caching = "false"
        connection = sql.connect(
            server_hostname=self.hostname,
            http_path=self.http_path,
            access_token=self.access_token,
            schema=self.schema,
            session_configuration={"use_cached_result": results_caching})
        return connection

    def execute_query(self, query_str):
        connection = self._get_connection()
        cursor = connection.cursor()
        result = cursor.execute(query_str)
        cursor.close()
        connection.close()

    def get_rows(self, query_str):
        connection = self._get_connection()
        cursor = connection.cursor()
        cursor.execute(query_str)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        return rows

    def setToken(self, token):
        self.access_token = token

    def setHostname(self, hostname):
        self.hostname = hostname

    def _get_spark_runtimes(self):
        """Gets a list of the latest Spark runtimes."""
        response = requests.get(
            f"https://{self.hostname}/api/2.0/clusters/spark-versions",
            headers={
                "Authorization": f"Bearer {self.access_token}"
            }
        )
        result = list(map(lambda v: v['key'], response.json()['versions']))
        return result

    def launch_warehouse(self, config):
        """Creates a new SQL warehouse based upon a config."""
        assert self.access_token is not None, "An API token is needed to launch a compute instance. " \
                                              "Use `.setToken(token)` to add an API token."
        assert self.hostname is not None, "A Databricks hostname is needed to launch a compute instance. " \
                                          "Use `.setHostname(hostname)` to add a Databricks hostname."
        # Determine the type of compute to lauch: warehouse or cluster
        if 'type' not in config:
            type = 'warehouse' # default to a SQL warehouse
        else:
            type = config['type'].strip().lower()
            assert type == "warehouse" or type == "cluster", "Invalid compute 'type' provided. " \
                                                             "Allowed types include: ['warehouse', 'cluster']."

        # Determine the Spark runtime to install
        latest_runtimes = self._get_spark_runtimes()
        if 'runtime' not in config:
            spark_version = self._LATEST_RUNTIME # default to the latest runtime
        elif config['runtime'].strip().lower() == 'latest':
            spark_version = self._LATEST_RUNTIME # default to the latest runtime
        else:
            spark_version = config['runtime'].strip().lower()
            assert spark_version in latest_runtimes, f"Invalid Spark 'runtime'. " \
                                                     f"Valid runtimes include: {latest_runtimes}"

        # Determine the size of the compute
        if 'size' not in config:
            size = 'Small'
        else:
            size = config['size'].strip()
            assert size in self._CLUSTER_SIZES, f"Invalid cluster 'size'. " \
                                                f"Valid cluster 'sizes' include: {self._CLUSTER_SIZES}"

        # Determine if Photon should be enabled or not
        if 'enable_photon' not in config:
            enable_photon = 'true'  # default
        else:
            enable_photon = str(config['enable_photon']).lower()

        # Determine auto-scaling
        if 'max_num_clusters' in config:
            max_num_clusters = config['max_num_clusters']
            min_num_clusters = config['min_num_clusters'] if 'min_num_clusters' in config else 1
        else:
            min_num_clusters = 1
            max_num_clusters = 1

        response = requests.post(
            f"https://{self.hostname}/api/2.0/sql/warehouses/",
            headers={
                "Authorization": f"Bearer {self.access_token}"
            },
            json={
                "name": "🧪 Beaker Benchmark Testing Warehouse",
                "cluster_size": size,
                "min_num_clusters": min_num_clusters,
                "max_num_clusters": max_num_clusters,
                "tags": {
                    "custom_tags": [
                        {
                            "key": "Description",
                            "value": "Beaker Benchmark Testing"
                        }
                    ]
                },
                "enable_photon": enable_photon,
                "channel": {
                    "name": "CHANNEL_NAME_CURRENT"
                }
            }
        )
        warehouse_id = response.json()['id']
        return warehouse_id
