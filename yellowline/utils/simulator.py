"""
utils/simulator.py — Stream simulator for replaying historical NYC taxi data.

This is the one fully-implemented utility provided in the scaffold.
Run it in a background thread while your Spark streaming queries are active.

Usage:
    from utils.simulator import simulate_stream
    import threading

    t = threading.Thread(
        target=simulate_stream,
        kwargs={
            'source_csv': 'data/raw/yellow_tripdata.csv',
            'output_dir': '/tmp/yellowline/input/',
        },
        daemon=True,
    )
    t.start()
"""

import os
import time
import random
import argparse
import pandas as pd
from utils.spark_session import get_spark
from utils.schema import TRIP_SCHEMA
from config import (
    RAW_CSV,
    STREAM_INPUT,
    ZONE_LOOKUP,
    SIMULATOR_BATCH_SIZE,
    SIMULATOR_INTERVAL_SECS,
    SIMULATOR_LIMIT_ROWS,
)


def _load_location_pool(df: pd.DataFrame) -> list[int]:
    """Build a pool of valid taxi zone IDs for imputing missing PU/DO IDs."""
    if os.path.exists(ZONE_LOOKUP):
        try:
            zones = pd.read_csv(ZONE_LOOKUP, usecols=["LocationID"])
            location_ids = zones["LocationID"].dropna().astype(int).unique().tolist()
            if location_ids:
                return location_ids
        except Exception:
            # Fall back to observed IDs in the raw trip data.
            pass

    observed_ids = (
        pd.concat([df["PULocationID"], df["DOLocationID"]], ignore_index=True)
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    if observed_ids:
        return observed_ids

    # Final fallback: standard NYC TLC taxi zone ID range.
    return list(range(1, 264))


def simulate_stream(
    source_csv: str = RAW_CSV,
    output_dir: str = STREAM_INPUT,
    batch_size: int = SIMULATOR_BATCH_SIZE,
    interval_seconds: float = SIMULATOR_INTERVAL_SECS,
    limit_rows: int = SIMULATOR_LIMIT_ROWS,
    clear_raw_data: bool = False,
) -> None:
    """
    Reads the NYC taxi CSV, sorts by pickup time, and writes
    `batch_size` rows as a new JSON file to `output_dir` every
    `interval_seconds`. Each file drop mimics a micro-batch of
    trips completing in real time.

    Args:
        source_csv:       Path to the Kaggle CSV file.
        output_dir:       Directory watched by the Bronze streaming query.
        batch_size:       Rows per file. Tune alongside MAX_FILES_PER_TRIGGER.
        interval_seconds: Pause between file writes. Lower = faster stream.
        limit_rows:       Cap total rows to keep local runs manageable.
        clear_raw_data:   If True, delete the source CSV after stream generation.
    """
    os.makedirs(output_dir, exist_ok=True)

    print(f"[simulator] reading {limit_rows:,} rows from {source_csv}...")
    spark = get_spark("YellowLine-Simulator")
    df = (
        spark.read.format("csv")
        .option("header", True)
        .schema(TRIP_SCHEMA)
        .load(source_csv)
        .limit(limit_rows)
        .toPandas()
    )
    df = df.sort_values("tpep_pickup_datetime").reset_index(drop=True)

    location_pool = _load_location_pool(df)
    rng = random.Random(42)
    for col in ("PULocationID", "DOLocationID"):
        missing = df[col].isna()
        missing_count = int(missing.sum())
        if missing_count > 0:
            df.loc[missing, col] = rng.choices(location_pool, k=missing_count)
            print(
                f"[simulator] filled {missing_count:,} nulls in {col} with random zone IDs"
            )
        df[col] = df[col].astype(int)

    total_batches = len(df) // batch_size
    print(
        f"[simulator] will write {total_batches} batches of {batch_size} rows to {output_dir}"
    )

    for i in range(total_batches):
        batch = df.iloc[i * batch_size : (i + 1) * batch_size]
        out_path = os.path.join(output_dir, f"batch_{i:05d}.json")
        batch.to_json(out_path, orient="records", lines=True)
        print(f"[simulator] wrote batch {i + 1}/{total_batches} → {out_path}")
        time.sleep(interval_seconds)

    print("[simulator] all batches written — stream complete.")

    if clear_raw_data:
        if os.path.exists(source_csv):
            os.remove(source_csv)
            print(f"[simulator] deleted raw source file: {source_csv}")
        else:
            print(
                f"[simulator] clear flag enabled, but source file not found: {source_csv}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Replay taxi CSV into JSON micro-batches."
    )
    parser.add_argument(
        "--clear-raw-data",
        action="store_true",
        help="Delete the source CSV after all batches are written.",
    )
    args = parser.parse_args()

    # Run standalone to pre-populate the input directory.
    simulate_stream(clear_raw_data=args.clear_raw_data)
