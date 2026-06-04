"""
src/silver/clean.py — Milestone 2: Silver Layer

Goal: read from Bronze as a stream, apply data quality rules,
enrich with taxi zone names, deduplicate, and write clean rows
to the Silver Delta table.

What to implement:
  1. Read Bronze Delta table as a stream
  2. Cast pickup/dropoff strings to TimestampType
  3. Filter out bad rows:
       - fare_amount <= 0
       - trip_distance == 0 and fare_amount > 2  (zero-distance paid trips)
       - passenger_count is null or <= 0
  4. Join with the static zone lookup (read from ZONE_LOOKUP csv)
     to resolve PULocationID → pickup_borough, pickup_zone
     and DOLocationID → dropoff_borough, dropoff_zone
  5. Add derived column: revenue_per_mile = total_amount / trip_distance
     Handle division by zero (trip_distance == 0) gracefully
  6. Deduplicate within a watermark window
  7. Write to SILVER_PATH as Delta append with checkpointing

Questions to answer before moving to Gold:
  - What output mode are you using and why?
        so we're using intermediate tables here to do some cleanings for hte streaming table but overtime we'll want to aggregate and not stress the system by doing it in one pass
        we cant use update mode with duplicates because by dropping any duplicates we essentially tell the engine that this record at this time is the source of truth and drop everything else
  - What happens to the static zone DataFrame if the lookup CSV changes mid-run?
        THe static zone dataframe stays in the same state as it is projected to each node executor while the stream runs.  This means that in order to change the state of the dataframe, we need to restart the stream
  - Why does dropDuplicates on a stream require a watermark?
        The watermark will tell us how long the engine needs to hold onto intermediate state. If a watermark isn't specified, the engine holds on to intermediate state indefinitely effectly making computations complete as opposed to append. Duplicate data coming in outside the window will then be acknowledged as a different event
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col
from utils.spark_session import get_spark
from utils.schema import TRIP_SCHEMA, ZONE_SCHEMA
from config import (
    BRONZE_PATH,
    SILVER_PATH,
    ZONE_LOOKUP,
    CKPT_SILVER,
    SILVER_WATERMARK,
    TRIGGER_INTERVAL,
)


def load_zone_lookup(spark: SparkSession) -> DataFrame:
    """
    Load the taxi zone lookup as a static DataFrame.
    This will be broadcast automatically in the stream-static join.
    """
    return spark.read.format("csv").schema(ZONE_SCHEMA).load(ZONE_LOOKUP)


def apply_quality_filters(df):

    return (
        df.withColumn(
            "tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType())
        )
        .withColumn(
            "tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType())
        )
        .filter(
            (col("passenger_count") > 0)
            & (col("fare_amount") > 0)
            & ~((col("trip_distance") == 0) & (col("fare_amount") > 2))
            & (col("PULocationID").isNotNull())
            & (col("DOLocationID").isNotNull())
        )
    )


def enrich_with_zones(df, zone_df):
    pickup_zones = zone_df.select(
        col("LocationID").alias("pu_location_id"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone"),
    )
    dropoff_zones = zone_df.select(
        col("LocationID").alias("do_location_id"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone"),
    )

    return (
        df.join(pickup_zones, df.PULocationID == pickup_zones.pu_location_id, "left")
        .join(dropoff_zones, df.DOLocationID == dropoff_zones.do_location_id, "left")
        .select(
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
            "pickup_borough",
            "pickup_zone",
            "dropoff_borough",
            "dropoff_zone",
            "fare_amount",
            "tip_amount",
            "total_amount",
        )
    )


def add_derived_columns(df):
    """
    generates revenue per mile and adds that as a column
    """
    return df.withColumn(
        "revenue_per_mile",
        F.when(
            col("trip_distance") != 0, col("total_amount") / col("trip_distance")
        ).otherwise(None),
    )


def run():
    spark = get_spark("YellowLine-Silver")

    zone_df = load_zone_lookup(spark)

    bronze_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", 5)
        .load(BRONZE_PATH)
    )

    # cast timestamps → filter → watermark → deduplicate → enrich → derive
    enriched_cleaned = enrich_with_zones(apply_quality_filters(bronze_stream), zone_df)
    silver_df = enriched_cleaned.withWatermark(
        "tpep_pickup_datetime",
        SILVER_WATERMARK,
    ).dropDuplicatesWithinWatermark()

    query = (
        silver_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CKPT_SILVER)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(SILVER_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
