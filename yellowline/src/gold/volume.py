"""
src/gold/volume.py — Milestone 3b: 15-Minute Rolling Trip Volume by Zone

Goal: compute trip counts in a sliding 30-minute window (sliding every 15 min)
grouped by pickup zone. Gives dispatch near-real-time density data.

What to implement:
  1. Read Silver Delta as a stream
  2. Apply a watermark on the pickup timestamp
  3. Aggregate using a SLIDING window (duration=30min, slide=15min) + pickup_zone
  4. Compute: trip_count
  5. Write to GOLD_VOLUME in Append mode with checkpointing

Key difference from revenue.py: this uses a sliding window.
A single trip will appear in MULTIPLE output windows — understand why
before your interview.

Questions to answer before moving on:
  - How many windows does each trip appear in given these window settings?
  - Why might this table be larger than the revenue Gold table?
  - How would you use this table to build a real-time heatmap?
"""

from pyspark.sql import functions as F
from utils.spark_session import get_spark
from config import (
    SILVER_PATH,
    GOLD_VOLUME,
    CKPT_GOLD_VOL,
    GOLD_VOLUME_WATERMARK,
    TRIGGER_INTERVAL,
)


def run():
    spark = get_spark("YellowLine-Gold-Volume")

    # TODO: read Silver as a stream
    silver_stream = spark.readStream()

    # TODO: watermark → sliding window aggregation → write
    # Hint: window(col("pickup_ts"), "30 minutes", "15 minutes")
    query = ...

    query.awaitTermination()


if __name__ == "__main__":
    run()
