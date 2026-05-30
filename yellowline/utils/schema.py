"""
utils/schema.py — All schema definitions for the YellowLine pipeline.

Defining schemas explicitly (rather than inferring) is required for
streaming sources and is a best practice for production pipelines.
Keep all schemas here so they stay consistent across layers.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, TimestampType
)


# ── Raw trip schema (matches Kaggle CSV column names exactly) ─────────────────
# TODO (Milestone 1): Fill in the correct types for each field.
# Reference the dataset description in the project brief.
# Hint: datetime strings come in as StringType — you'll cast them later.

TRIP_SCHEMA = StructType([
    StructField("VendorID",            IntegerType(),  True),
    StructField("tpep_pickup_datetime",  StringType(),  True),  # cast to timestamp in Silver
    StructField("tpep_dropoff_datetime", StringType(),  True),
    StructField("passenger_count",     # TODO: what type?
    StructField("trip_distance",       # TODO: what type?
    StructField("RatecodeID",          IntegerType(),  True),
    StructField("store_and_fwd_flag",  StringType(),   True),
    StructField("PULocationID",        IntegerType(),  True),
    StructField("DOLocationID",        IntegerType(),  True),
    StructField("payment_type",        IntegerType(),  True),
    StructField("fare_amount",         # TODO: what type?
    StructField("extra",               DoubleType(),   True),
    StructField("mta_tax",             DoubleType(),   True),
    StructField("tip_amount",          DoubleType(),   True),
    StructField("tolls_amount",        DoubleType(),   True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount",        # TODO: what type?
])


# ── Taxi zone lookup schema ───────────────────────────────────────────────────
# Used in Silver as a static dimension join.
# Columns: LocationID, Borough, Zone, service_zone

ZONE_SCHEMA = StructType([
    # TODO (Milestone 2): define this schema based on the lookup CSV headers
])


# ── Silver schema (post-clean) ────────────────────────────────────────────────
# You don't read with this schema — Silver is written by your Silver query.
# Define it here for documentation and for use in downstream reads.

SILVER_SCHEMA = StructType([
    # TODO: fill in after you've decided which columns survive cleaning
    # and what new derived columns you add (e.g. revenue_per_mile)
])
