"""
src/anomaly/detect.py — Milestone 4: Fare Anomaly Detection

Goal: flag trips where fare_amount is more than N standard deviations
from the mean fare for that pickup borough. Upsert flagged trips into
a Delta anomalies table using foreachBatch + MERGE.

What to implement:
  1. Read Silver Delta as a stream
  2. Use foreachBatch to process each micro-batch as a plain DataFrame
  3. Inside the function:
     a. Compute per-borough fare mean and stddev for this micro-batch
     b. Flag trips exceeding FARE_ANOMALY_STDDEV_THRESHOLD
     c. MERGE flagged rows into the anomalies Delta table:
          - WHEN MATCHED → update (re-flagged trips may have new metadata)
          - WHEN NOT MATCHED → insert
  4. Write query with foreachBatch (no outputMode needed)

Key questions before moving on:
  - Why must the foreachBatch function be idempotent?
  - What does batch_id give you and how could you use it for exactly-once guarantees?
  - Why can't you use outputMode("update") + Delta write directly here?

Note on statistics: computing stddev over a single micro-batch is a
simplification — in production you'd maintain a rolling statistic.
That's fine for this exercise; the point is the foreachBatch + MERGE pattern.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F, DataFrame
from pyspark.sql import SparkSession
from utils.spark_session import get_spark
from config import (
    SILVER_PATH,
    ANOMALIES_PATH,
    CKPT_ANOMALY,
    TRIGGER_INTERVAL,
    FARE_ANOMALY_STDDEV_THRESHOLD,
)


def flag_anomalies(df: DataFrame) -> DataFrame:
    """
    TODO: given a batch DataFrame, compute per-borough fare stats
    and return only the rows where fare_amount > mean + N*stddev.

    Steps:
      1. groupBy pickup_borough, compute mean(fare_amount) and stddev(fare_amount)
      2. join stats back to df
      3. filter where fare_amount > mean + threshold * stddev
      4. add a flagged_at timestamp column (F.current_timestamp())
    """
    ...


def upsert_anomalies(micro_batch_df: DataFrame, batch_id: int) -> None:
    """
    TODO: foreachBatch handler.
      1. Call flag_anomalies to get flagged rows
      2. If no flagged rows, return early (nothing to upsert)
      3. Load or create the anomalies Delta table
      4. Run MERGE: match on a unique trip identifier
         (think: what combination of columns uniquely identifies a trip?)
      5. whenMatchedUpdateAll, whenNotMatchedInsertAll

    Hint: DeltaTable.forPath(spark, ANOMALIES_PATH) — but what if the
    table doesn't exist yet on the first batch? Handle that case.
    """
    ...


def run():
    spark = get_spark("YellowLine-Anomaly")

    silver_stream = ...  # TODO: read Silver as stream

    query = (
        silver_stream
        .writeStream
        .foreachBatch(upsert_anomalies)
        .option("checkpointLocation", CKPT_ANOMALY)
        # TODO: add trigger
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
