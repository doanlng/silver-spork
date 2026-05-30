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
  - What happens to the static zone DataFrame if the lookup CSV changes mid-run?
  - Why does dropDuplicates on a stream require a watermark?
"""

from pyspark.sql import functions as F
from utils.spark_session import get_spark
from utils.schema import ZONE_SCHEMA
from config import (
    BRONZE_PATH,
    SILVER_PATH,
    ZONE_LOOKUP,
    CKPT_SILVER,
    SILVER_WATERMARK,
    TRIGGER_INTERVAL,
)


def load_zone_lookup(spark):
    """
    Load the taxi zone lookup as a static DataFrame.
    This will be broadcast automatically in the stream-static join.
    TODO: read ZONE_LOOKUP csv with ZONE_SCHEMA, return DataFrame.
    """
    ...


def apply_quality_filters(df):
    """
    TODO: implement the three quality filters described above.
    Return the filtered DataFrame.
    """
    ...


def enrich_with_zones(df, zone_df):
    """
    TODO: join df with zone_df twice —
      once for pickup (PULocationID) and once for dropoff (DOLocationID).
    Alias carefully to avoid column name collisions.
    Return enriched DataFrame.
    """
    ...


def add_derived_columns(df):
    """
    TODO: add revenue_per_mile.
    Hint: F.when(...).otherwise(...) handles the zero-distance case.
    Return DataFrame with new column.
    """
    ...


def run():
    spark = get_spark("YellowLine-Silver")

    zone_df = load_zone_lookup(spark)

    # TODO: read Bronze Delta as a stream
    bronze_stream = ...

    # TODO: apply transforms in order
    # cast timestamps → filter → watermark → deduplicate → enrich → derive
    silver_df = ...

    # TODO: write to Silver Delta
    query = ...

    query.awaitTermination()


if __name__ == "__main__":
    run()
