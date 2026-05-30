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
import pandas as pd
from config import (
    RAW_CSV,
    STREAM_INPUT,
    SIMULATOR_BATCH_SIZE,
    SIMULATOR_INTERVAL_SECS,
    SIMULATOR_LIMIT_ROWS,
)


def simulate_stream(
    source_csv: str = RAW_CSV,
    output_dir: str = STREAM_INPUT,
    batch_size: int = SIMULATOR_BATCH_SIZE,
    interval_seconds: float = SIMULATOR_INTERVAL_SECS,
    limit_rows: int = SIMULATOR_LIMIT_ROWS,
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
    """
    os.makedirs(output_dir, exist_ok=True)

    print(f"[simulator] reading {limit_rows:,} rows from {source_csv}...")
    df = pd.read_csv(source_csv, nrows=limit_rows)
    df = df.sort_values("tpep_pickup_datetime").reset_index(drop=True)

    total_batches = len(df) // batch_size
    print(f"[simulator] will write {total_batches} batches of {batch_size} rows to {output_dir}")

    for i in range(total_batches):
        batch = df.iloc[i * batch_size : (i + 1) * batch_size]
        out_path = os.path.join(output_dir, f"batch_{i:05d}.json")
        batch.to_json(out_path, orient="records", lines=True)
        print(f"[simulator] wrote batch {i + 1}/{total_batches} → {out_path}")
        time.sleep(interval_seconds)

    print("[simulator] all batches written — stream complete.")


if __name__ == "__main__":
    # Run standalone to pre-populate the input directory
    simulate_stream()
