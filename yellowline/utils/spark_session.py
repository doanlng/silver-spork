"""
utils/spark_session.py — Shared SparkSession factory.

Import get_spark() in every module instead of creating a new
SparkSession each time. Spark will reuse the existing session.
"""

from pyspark.sql import SparkSession
from config import SHUFFLE_PARTITIONS


def get_spark(app_name: str = "YellowLineAnalytics") -> SparkSession:
    """
    Returns a SparkSession configured for the YellowLine pipeline.

    Delta Lake support is enabled via the delta extension.
    Shuffle partitions are kept low for local development —
    raise SHUFFLE_PARTITIONS in config.py when running on a cluster.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        # TODO (stretch): what other configs might you tune here for local dev?
        # Think about: adaptive query execution, broadcast threshold, UI retention
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
