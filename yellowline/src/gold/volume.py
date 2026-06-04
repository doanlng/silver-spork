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

        each trip occurs in 2 windows
        with a 30 minute window sliding every 15 minutes there are 15 minutes of overlap where events at that timestamp will appear in both windows

  - Why might this table be larger than the revenue Gold table?
        because this window tumbles where the 30 minutes slides every 15, every distinct event contributes to 2 windows. the window that ends at that event, and the window that ends at t + 15 minutes

  - How would you use this table to build a real-time heatmap?
        with color set as the volume by zone, let x axis be zone and y axis be even time window
        this lets us see how traffic across each pickup_zone varies hour by hour
"""

from pyspark.sql import functions as F, Window
from utils.spark_session import get_spark
from config import (
    SILVER_PATH,
    GOLD_VOLUME,
    CKPT_GOLD_VOL,
    GOLD_VOLUME_WATERMARK,
    TRIGGER_INTERVAL,
)
from utils.schema import SILVER_SCHEMA


def run():
    spark = get_spark("YellowLine-Gold-Volume")

    silver_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", 5)
        .load(SILVER_PATH)
    )

    volume_by_zone = (
        silver_stream.withWatermark("tpep_pickup_datetime", GOLD_VOLUME_WATERMARK)
        .groupBy(
            F.window(
                "tpep_pickup_datetime", "30 minutes", "15 minutes"
            ),  # sliding window
            "pickup_zone",
        )
        .agg(F.count("*").alias("trip_volume"))
    )

    query = (
        volume_by_zone.writeStream.trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CKPT_GOLD_VOL)
        .outputMode("append")
        .start(GOLD_VOLUME)
    )
    query.awaitTermination()


if __name__ == "__main__":
    run()
