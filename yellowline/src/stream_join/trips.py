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

from pyspark.sql import functions as F
from utils.spark_session import get_spark
from config import (
    BRONZE_PATH,
    COMPLETED_PATH,
    CKPT_COMPLETED,
    COMPLETED_WATERMARK,
    TRIGGER_INTERVAL,
)


def make_trip_id(df):
    """
    TODO: add a trip_id column by hashing a combination of fields
    that uniquely identifies a trip. Which fields would you choose?
    """
    ...


def run():
    spark = get_spark("YellowLine-StreamJoin")

    # TODO: read Bronze Delta as a stream — twice
    # Project only the columns needed for each side
    pickups_stream  = ...
    dropoffs_stream = ...

    # TODO: add trip_id to both sides
    # TODO: add watermarks to both sides
    # TODO: join with time-range condition
    # TODO: compute actual_duration_minutes
    # TODO: write to COMPLETED_PATH

    query = ...
    query.awaitTermination()


if __name__ == "__main__":
    run()
