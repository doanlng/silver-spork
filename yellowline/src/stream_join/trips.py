"""
src/stream_join/trips.py — Milestone 5: Stream-to-Stream Trip Completion Join

Goal: simulate two independent streams (pickups, dropoffs) from the same
dataset and join them to compute actual trip duration in real time.

What to implement:
  1. Create a synthetic trip_id by hashing VendorID + pickup_datetime + PULocationID
     Use F.md5(F.concat_ws("|", ...)) or F.hash(...)
  2. Split the simulator feed into two streams:
       - pickups_stream: select trip_id, pickup_ts, PULocationID, pickup_borough
       - dropoffs_stream: select trip_id, dropoff_ts, DOLocationID, dropoff_borough
  3. Apply watermarks to BOTH streams (use COMPLETED_WATERMARK from config)
  4. Join on trip_id with a time-range condition:
       dropoff_ts >= pickup_ts AND dropoff_ts <= pickup_ts + interval 3 hours
  5. Compute actual_duration_minutes
  6. Write to COMPLETED_PATH in Append mode with checkpointing

This is the hardest milestone. Work carefully through the watermark logic
before writing any code — understand why both sides need watermarks and
what happens to unmatched pickups.

Questions to answer before calling this done:
  - Why does Spark require watermarks on both sides of a stream-stream join?
  - What happens to a pickup record if no dropoff arrives before the watermark expires?
  - Why is the time-range condition required in addition to the trip_id key match?
  - What output mode is valid for stream-stream joins and why?

Practical note: you'll need to simulate two streams from the same Bronze
Delta table (or the raw input). Think about how to read the same source
twice and project different columns. Two separate readStream calls work fine.
"""

from pyspark.sql import DataFrame, functions as F
from utils.spark_session import get_spark
from config import (
    BRONZE_PATH,
    COMPLETED_PATH,
    CKPT_COMPLETED,
    COMPLETED_WATERMARK,
    SILVER_PATH,
    TRIGGER_INTERVAL,
)


def make_trip_id(df: DataFrame) -> DataFrame:
    """
    TODO: add a trip_id column by hashing a combination of fields
    that uniquely identifies a trip. Which fields would you choose?
    """

    return df.withColumn(
        "trip_id",
        F.hash(
            F.concat_ws(
                "|",
                F.col("VendorID").cast("string"),
                F.col("tpep_pickup_datetime").cast("string"),
                F.col("tpep_dropoff_datetime").cast("string"),
                F.col("PULocationID").cast("string"),
                F.col("DOLocationID").cast("string"),
            )
        ),
    )


def run():
    spark = get_spark("YellowLine-StreamJoin")

    # TODO: read Bronze Delta as a stream — twice
    # Project only the columns needed for each side
    pickups_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", 5)
        .load(BRONZE_PATH)
        .select(
            "VendorID",
            "tpep_pickup_datetime",
            "PULocationID",
        )
    )

    dropoffs_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", 5)
        .load(BRONZE_PATH)
        .select(
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "DOLocationID",
        )
    )

    # TODO: add trip_id to both sides
    enrich_pickups = make_trip_id(pickups_stream)
    enrich_dropoffs = make_trip_id(dropoffs_stream)

    # TODO: add watermarks to both sides
    wm_pickups = enrich_pickups.withWatermark(
        "tpep_pickup_datetime", COMPLETED_WATERMARK
    )
    wm_dropoffs = enrich_dropoffs.withWatermark(
        "tpep_dropoff_datetime", COMPLETED_WATERMARK
    )

    # TODO: join with time-range condition
    joined = (
        wm_pickups.alias("source")
        .join(
            wm_dropoffs.alias("target"),
            on=(F.col("source.trip_id") == F.col("target.trip_id")),
            how="inner",
        )
        .filter(
            "tpep_dropoff_datetime >= tpep_pickup_datetime AND tpep_pickup_datetime - tpep_dropoff_datetime <= interval `3 Hours`"
        )
    )

    with_actual_duration = joined.withColumn(
        "actual_duration",
        F.unix_timestamp("tpep_pickup_datetime")
        - F.unix_timestamp("tpep_dropoff_datetime") / 60,
    )

    query = (
        with_actual_duration.writeStream.option("checkpointLocation", CKPT_COMPLETED)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(COMPLETED_PATH)
    )
    query.awaitTermination()


if __name__ == "__main__":
    run()
