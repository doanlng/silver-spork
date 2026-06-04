"""
src/gold/revenue.py — Milestone 3a: Hourly Revenue by Borough

Goal: compute total revenue and trip count in tumbling 1-hour windows
grouped by pickup borough. This is the ops team's primary revenue monitor.

What to implement:
  1. Read Silver Delta as a stream
  2. Apply a watermark on the pickup timestamp
  3. Aggregate using a tumbling 1-hour window + pickup_borough groupBy
  4. Compute: trip_count, total_revenue (sum of total_amount), avg_fare
  5. Write to GOLD_REVENUE in Append mode with checkpointing

Design decisions you need to make (and be ready to defend):
  - What watermark delay is appropriate here? Why?
  - Why is Append mode correct and Complete mode dangerous at scale?
  - What does a row in this Gold table represent exactly?

Questions to answer before moving on:
  - How many windows does a single trip appear in for a tumbling window?
        1

  - When does a row appear in the output sink with Append mode?
        a row appears in the sink after spark releases its state,
        the state release occurs when the watermark (which is delayed behind the latest processing time) exceeds the end of the window that contains the event.

  - What would break if you used Complete mode on a 1-year history?
        the watermark would break because complete mode relies on holding onto all the state for its complete computations, meaning it never releases state even behind by one year
        there is a high risk of performance issues with millions of rows being shuffled and rewritten
"""

from pyspark.sql import functions as F
from utils.spark_session import get_spark
from config import (
    SILVER_PATH,
    GOLD_REVENUE,
    CKPT_GOLD_REV,
    GOLD_REVENUE_WATERMARK,
    TRIGGER_INTERVAL,
)


def run():
    spark = get_spark("YellowLine-Gold-Revenue")

    silver_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", 5)
        .load(SILVER_PATH)
    )

    hourly_borough_revenue = (
        silver_stream.withWatermark("tpep_pickup_datetime", GOLD_REVENUE_WATERMARK)
        .groupBy(F.window("tpep_pickup_datetime", "1 hour"), "pickup_borough")
        .agg(F.sum("total_amount").alias("revenue_in_window"))
    )

    query = (
        hourly_borough_revenue.writeStream.outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CKPT_GOLD_REV)
        .outputMode("append")
        .start(GOLD_REVENUE)
    )
    query.awaitTermination()


if __name__ == "__main__":
    run()
