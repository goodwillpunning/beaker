import os
from pyspark.sql import SparkSession
from functools import lru_cache
from pyspark.sql.functions import col

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

    
def metrics_to_df_view(beaker_metrics, history_metrics, view_name):
    """Convert a list of dicts to a results dataframe.
    Create a view and return the dataframe.
    """
    spark = get_spark_session()
    metrics_df = spark.createDataFrame(beaker_metrics)
    history_df = spark.createDataFrame(history_metrics)
    history_metrics_df = (
        history_df.alias("v1") 
            .join(metrics_df.select("id", "query").distinct().alias("v2"), col("v1.query_text") == col("v2.query"), "inner") 
            .select("v1.warehouse_id", "v2.id", "v1.query_text", 
                (col("v1.duration") / 1000).alias("duration_sec"), 
                (col("metrics.execution_time_ms") / 1000).alias("query_execution_time_sec"), 
                (col("metrics.planning_time_ms") / 1000).alias("planning_time_sec"), 
                (col("metrics.photon_total_time_ms") / 1000).alias("photon_total_time_sec"))
        )
    history_metrics_df.createOrReplaceTempView(view_name)
    return history_metrics_df
