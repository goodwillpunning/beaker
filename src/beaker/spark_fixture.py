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

    
def metrics_to_df_view(metrics_pdf, view_name):
    """Convert a pandas dataframe to a spark dataframe.
    Create a view and return the spark dataframe.
    """
    spark = get_spark_session()
    metrics_df = spark.createDataFrame(metrics_pdf)
    metrics_df.createOrReplaceTempView(view_name)
    print(f"Query metrics at: {view_name}")
    return metrics_df
