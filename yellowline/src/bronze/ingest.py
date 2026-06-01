"""
src/bronze/ingest.py — Milestone 1: Bronze Layer

Goal: read raw trip events from the simulator's output directory and
append them to a Delta table with zero transformation. The Bronze layer
is your source of truth — raw, immutable, complete.

What to implement:
  1. Read a streaming DataFrame from STREAM_INPUT using the TRIP_SCHEMA
  2. Write to BRONZE_PATH as a Delta append stream with checkpointing
  3. Verify the checkpoint directory after a few batches

Questions to answer before moving to Silver:
  - Why define the schema explicitly rather than using inferSchema?
  - What does the checkpoint directory contain after 5 batches?
  - How would you confirm exactly-once delivery after a simulated crash?
"""

from utils.spark_session import get_spark
from utils.schema import TRIP_SCHEMA
from config import (
    STREAM_INPUT,
    BRONZE_PATH,
    CKPT_BRONZE,
    MAX_FILES_PER_TRIGGER,
    TRIGGER_INTERVAL,
)


def run():
    spark = get_spark("YellowLine-Bronze")

    # TODO: read stream from STREAM_INPUT
    # Hint: .readStream.schema(...).option("maxFilesPerTrigger", ...).json(...)
    raw_stream = (
        spark.readStream.schema(TRIP_SCHEMA)
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .json(STREAM_INPUT)
    )

    # TODO: write to Delta at BRONZE_PATH
    # Remember: outputMode, checkpointLocation, trigger
    query = (
        raw_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CKPT_BRONZE)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(BRONZE_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
