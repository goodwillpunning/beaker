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


def metrics_to_df_view(metrics, view_name):
    """Convert a list of dicts to a results dataframe.
    Create a view and return the dataframe.
    """
    spark = get_spark_session()
    df = spark.createDataFrame(metrics)
    df.createOrReplaceTempView(view_name)
    return df
