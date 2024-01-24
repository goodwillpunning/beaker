import os
from pyspark.sql import SparkSession
from functools import lru_cache


@lru_cache(maxsize=None)
def get_spark_session():
    if os.getenv("ENV") == "LOCAL":
        return (
            SparkSession.builder.master("local")
            .appName("beaker")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.driver.host", "localhost")
            .getOrCreate()
        )
    else:
        return SparkSession.builder.appName("beaker").getOrCreate()


def get_query_history(hostname, token, warehouse_id, start_ts_ms):
    """
    Retrieves the Query History for a given workspace and Data Warehouse.

    Parameters:
    -----------
    hostname (str): The URL of the Databricks workspace where the Data Warehouse is located.
    token(str): The Personal Access Token (token) to access the Databricks API.
    warehouse_id (str): The ID of the Data Warehouse for which to retrieve the Query History.
    start_ts_ms (int): The Unix timestamp (milliseconds) value representing the start of the query history.
    view_name (str, optional): The name of the view created from the Spark DataFrame containing Query History. Defaults to "demo_query".

    Returns:
    --------
    None
    """
    ## Put together request 
    request_string = {
        "filter_by": {
            "query_start_time_range": {
                # "end_time_ms": end_ts_ms,
                "start_time_ms": start_ts_ms
        },
        "statuses": [
            "FINISHED"
        ],
        "warehouse_ids": warehouse_id
        },
        "include_metrics": "true",
        "max_results": "1000"
    }

    ## Convert dict to json
    v = json.dumps(request_string)

    uri = f"https://{hostname}/api/2.0/sql/history/queries"
    headers_auth = {"Authorization":f"Bearer {token}"}

    #### Get Query History Results from API
    response = requests.get(uri, data=v, headers=headers_auth)
    
    df = None
    if (response.status_code == 200) and ("res" in response.json()):
        end_res = response.json()['res']
        history_df = spark.createDataFrame(end_res)
        # df.createOrReplaceTempView(f"{view_name}_hist_view")
        # print(f"""View Query History at: {view_name}_hist_view""" )
    else:
        print("Failed to retrieve query history")
    return history_df

def metrics_to_df_view(metrics, view_name):
    """Convert a list of dicts to a results dataframe.
    Create a view and return the dataframe.
    """
    spark = get_spark_session()
    # df = spark.createDataFrame(metrics)
    metrics.createOrReplaceTempView(view_name)
    return df
