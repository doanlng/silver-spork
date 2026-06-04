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

        Spark guarantees at least once delivery for its execution in general.
        but when it calls on foreachbatch, may re-execute the code within foreachbatch on an already processed batch
        For records that never see completion, foreachbatch will run arbitrarily on the batch again which could be bad in the case of duplicate writes to external locations or storages that don't guarantee idempotency,
        so the foreachbatch must be idempotent to mitigate the risk of duplicate data

  - What does batch_id give you and how could you use it for exactly-once guarantees?

    The batch_id gives a unique monotonically increasing id assignment to the micro-batch processed in the foreachbatch function.
    spark can use this batch_id to see whether this batch has already been processed by the function in foreachbatch

  - Why can't you use outputMode("update") + Delta write directly here?
        Delta has no concept of update
Note on statistics: computing stddev over a single micro-batch is a
simplification — in production you'd maintain a rolling statistic.
That's fine for this exercise; the point is the foreachBatch + MERGE pattern.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F, DataFrame, SparkSession
from pyspark.sql.functions import col
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
    Steps:
      1. groupBy pickup_borough, compute mean(fare_amount) and stddev(fare_amount)
      2. join stats back to df
      3. filter where fare_amount > mean + threshold * stddev
      4. add a flagged_at timestamp column (F.current_timestamp())
    """
    fare_metrics = df.groupBy("pickup_borough").agg(
        F.avg("fare_amount").alias("avg_fare"),
        F.stddev("fare_amount").alias("stdev_fare"),
    )

    return (
        df.withColumn("flagged_at", F.current_timestamp())
        .join(fare_metrics, on="pickup_borough", how="left")
        .filter(
            col("fare_amount")
            > col("avg_fare") + col("stdev_fare") * FARE_ANOMALY_STDDEV_THRESHOLD
        )
        .drop(col("avg_fare"), col("stdev_fare"))
    )


# explicitly declare upsert logic
def upsert_anomalies(micro_batch_df: DataFrame, batch_id: int) -> None:
    """
      1. Call flag_anomalies to get flagged rows
      2. If no flagged rows, return early (nothing to upsert)
      3. Load or create the anomalies Delta table
      4. Run MERGE: match on a unique trip identifier
         (think: what combination of columns uniquely identifies a trip?)
      5. whenMatchedUpdateAll, whenNotMatchedInsertAll

    Hint: DeltaTable.forPath(spark, ANOMALIES_PATH) — but what if the
    table doesn't exist yet on the first batch? Handle that case.
    """
    spark = SparkSession.getActiveSession()
    flagged_anomalies = flag_anomalies(micro_batch_df)
    if spark and not flagged_anomalies.isEmpty():
        if DeltaTable.isDeltaTable(spark, ANOMALIES_PATH):
            DeltaTable.forPath(spark, ANOMALIES_PATH).alias("target").merge(
                flagged_anomalies.alias("source"),
                "target.tpep_pickup_datetime=source.tpep_pickup_datetime and target.tpep_dropoff_datetime=source.tpep_dropoff_datetime and target.vendor_id=source.vendor_id",
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            flagged_anomalies.write.format("delta").mode("append").save(ANOMALIES_PATH)


def run():
    spark = get_spark("YellowLine-Anomaly")

    silver_stream = spark.readStream.format("delta").load(SILVER_PATH)

    query = (
        silver_stream.writeStream.foreachBatch(upsert_anomalies)
        .option("checkpointLocation", CKPT_ANOMALY)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
