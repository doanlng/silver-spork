# Project Brief: NYC Taxi Real-Time Operations Dashboard
### Databricks SA Interview Prep — Capstone Streaming Project
**Estimated time:** 2–3 hours &nbsp;|&nbsp; **Difficulty:** Intermediate–Advanced &nbsp;|&nbsp; **Dataset:** NYC Yellow Taxi Trip Data (Kaggle)

---

## Background

You have been hired as a Solutions Architect at a fictional NYC taxi analytics company, **YellowLine Analytics**. The operations team currently runs nightly batch jobs to understand fleet performance — but they're losing ground to ride-share competitors who make decisions in real time.

Your job is to replace the batch pipeline with a **streaming Medallion Architecture** on Spark Structured Streaming that gives the ops team live visibility into: trip volumes by zone, revenue trends, and anomalous fares that may indicate meter fraud or data quality issues.

This is a solo SA engagement — you are designing, building, and could be asked to defend every decision.

---

## Dataset

**Source:** [NYC Yellow Taxi Trip Data — Kaggle](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)

The dataset contains one row per completed taxi trip. Key fields you'll work with:

| Field | Type | Notes |
|---|---|---|
| `tpep_pickup_datetime` | timestamp | Trip start — your **event time** |
| `tpep_dropoff_datetime` | timestamp | Trip end — used in stream-stream join milestone |
| `PULocationID` | integer | Pickup zone (1–265) |
| `DOLocationID` | integer | Dropoff zone (1–265) |
| `fare_amount` | double | Base fare in USD |
| `total_amount` | double | Total including tips and surcharges |
| `passenger_count` | integer | Can be null — handle it |
| `trip_distance` | double | Miles |
| `VendorID` | integer | Taxi vendor (1 or 2) |

A **taxi zone lookup table** is available separately (also on Kaggle / NYC TLC site) mapping `LocationID` → `Borough`, `Zone`, `service_zone`. You will use this as your static dimension for enrichment.

---

## Stream Simulator

This is the one piece of scaffolding provided. Since the dataset is historical, you need to replay it as a continuous file stream. The approach: sort trips by `tpep_pickup_datetime`, then write batches of rows to a watched directory at a fixed interval — simulating trips completing in real time.

```python
import pandas as pd
import os
import time
import shutil

def simulate_stream(
    source_csv: str,
    output_dir: str,
    batch_size: int = 500,
    interval_seconds: float = 2.0,
    limit_rows: int = 200_000,       # keep it manageable locally
):
    """
    Reads the NYC taxi CSV, sorts by pickup time, and writes
    `batch_size` rows as a new JSON file to `output_dir` every
    `interval_seconds`. Each file drop mimics a micro-batch of
    trips completing in real time.

    Run this in a separate thread or process from your Spark query.
    """
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(source_csv, nrows=limit_rows)
    df = df.sort_values('tpep_pickup_datetime').reset_index(drop=True)

    total_batches = len(df) // batch_size
    for i in range(total_batches):
        batch = df.iloc[i * batch_size : (i + 1) * batch_size]
        out_path = os.path.join(output_dir, f'batch_{i:05d}.json')
        batch.to_json(out_path, orient='records', lines=True)
        print(f'[simulator] wrote batch {i+1}/{total_batches} → {out_path}')
        time.sleep(interval_seconds)
```

**How to run alongside your Spark query:**

```python
import threading

t = threading.Thread(
    target=simulate_stream,
    kwargs={
        'source_csv': '/path/to/yellow_tripdata.csv',
        'output_dir': '/tmp/taxi/input/',
        'batch_size': 500,
        'interval_seconds': 2.0,
    },
    daemon=True,
)
t.start()

# Then start your streaming query below...
```

> **Note:** Use `daemon=True` so the simulator thread dies automatically when your main process stops. Tune `batch_size` and `interval_seconds` to control how fast data arrives — faster intervals will stress-test your watermark and trigger settings.

---

## Project Milestones

Work through these in order. Each milestone builds on the last and maps directly to a topic you'll be tested on in the Databricks SA interview.

---

### Milestone 1 — Bronze Layer: Raw Ingest
**Concepts:** file source streaming, explicit schema, Delta append, checkpointing

Define the schema explicitly (no `inferSchema`) and read the simulator's output directory as a stream. Write every arriving row to a Delta table at `/tmp/taxi/bronze/` with no transformations — raw fidelity is the goal at this layer.

**You should be able to answer when done:**
- Why is `inferSchema` dangerous on a stream?
- What does the checkpoint directory contain after your first 5 batches?
- How would you verify exactly-once delivery if the query crashed mid-run?

---

### Milestone 2 — Silver Layer: Clean, Enrich, Deduplicate
**Concepts:** stream-static join, data quality filtering, deduplication, watermarking

Read from the Bronze Delta table as a new stream. Apply the following transformations:

1. **Filter out bad rows** — negative fares, zero-distance trips over 1 mile, passenger counts of null or 0
2. **Enrich with zone names** — join the static taxi zone lookup table to resolve `PULocationID` and `DOLocationID` into human-readable borough and zone names
3. **Deduplicate** — the simulator may occasionally produce duplicate rows; deduplicate within a reasonable event-time window using `dropDuplicates` with a watermark
4. **Add a derived column** — compute `revenue_per_mile` = `total_amount / trip_distance`, handling division by zero gracefully

Write cleaned rows to `/tmp/taxi/silver/` as a Delta append stream.

**You should be able to answer when done:**
- What happens to the static zone lookup table if it gets updated mid-query?
- Why does `dropDuplicates` on a stream require a watermark?
- What output mode are you using here and why?

---

### Milestone 3 — Gold Layer: Windowed Aggregations
**Concepts:** tumbling windows, watermarking, output modes, late data trade-offs

Read from Silver as a stream. Build **two separate gold aggregations**, each writing to its own Delta table:

**Gold Table A — Hourly Revenue by Borough**
Aggregate `total_amount` and trip count in tumbling 1-hour windows on `tpep_pickup_datetime`, grouped by pickup borough. This is the ops team's primary revenue monitor.

**Gold Table B — 15-Minute Rolling Trip Volume by Zone**
Aggregate trip count in a sliding 30-minute window, sliding every 15 minutes, grouped by pickup zone. This gives the dispatch team near-real-time density heatmap data.

For both: choose an appropriate watermark delay, justify it, and use Append output mode so results are only emitted once a window is finalized.

**You should be able to answer when done:**
- Why does a longer watermark delay make your results more accurate but less timely?
- Why is Append mode correct here but Complete mode would be dangerous at scale?
- Gold Table B uses a sliding window — how many windows does a single trip appear in?

---

### Milestone 4 — Anomaly Detection with `foreachBatch`
**Concepts:** foreachBatch, conditional Delta MERGE, stateful processing

The fraud team has asked for a live feed of **suspicious trips** — defined as any trip where `fare_amount` is more than 3 standard deviations from the mean fare for that pickup borough in the last 24 hours.

Use `foreachBatch` on the Silver stream to:
1. Compute a rolling per-borough fare mean and stddev (you can use a batch window over the micro-batch itself as a simplification)
2. Flag trips exceeding the threshold
3. **Upsert** flagged trips into a Delta table at `/tmp/taxi/anomalies/` — update the record if it already exists (a re-flagged trip may have updated metadata), insert if new

> **Hint:** This is the pattern where `foreachBatch` shines — you need MERGE semantics, which aren't available natively in streaming write modes.

**You should be able to answer when done:**
- Why must your `foreachBatch` function be idempotent?
- What is the `batch_id` parameter and how would you use it for exactly-once guarantees?
- How would you productionize the stddev threshold — i.e., make it data-driven rather than hardcoded?

---

### Milestone 5 — Stream-to-Stream Join: Trip Completion Matching
**Concepts:** stream-to-stream join, dual watermarks, time-range conditions, state management

This is the most advanced milestone. The operations team wants to track **in-progress trips** — they want to know the moment a trip completes so they can compute actual duration vs. expected duration in real time.

Simulate two independent streams from the same dataset:
- **Pickups stream** — emit a row when a trip starts, using `tpep_pickup_datetime` as event time
- **Dropoffs stream** — emit a row when a trip ends, using `tpep_dropoff_datetime` as event time

Join the two streams on `VendorID` + a synthetic `trip_id` (you can generate one by hashing `VendorID + pickup_datetime + PULocationID`). Add the constraint that a dropoff must occur within 3 hours of the pickup.

Compute `actual_duration_minutes` on the joined result and write to `/tmp/taxi/completed_trips/`.

**You should be able to answer when done:**
- Why do both streams need watermarks for a stream-to-stream join?
- What happens to a pickup record if no matching dropoff arrives before the watermark passes?
- Why is the time-range condition (`dropoff <= pickup + 3 hours`) required in addition to the key match?

---

## Stretch Goals

If you finish early or want to push further:

- **Add a Spark UI investigation** — intentionally introduce skew by running without repartitioning on `PULocationID`, observe the straggler tasks, then fix it and compare
- **Schema evolution** — add a new field to the simulator output mid-run and handle it gracefully using Delta's `mergeSchema` option
- **Monitoring** — log `query.recentProgress` metrics (inputRowsPerSecond, processedRowsPerSecond, triggerExecution duration) to a separate Delta table every batch using `foreachBatch` — this is what a production observability layer looks like
- **Partitioning strategy** — when writing Gold tables, partition by `date` and `borough`; observe the file layout and explain why this benefits downstream batch reads

---

## Deliverable: The SA Walkthrough

When you're done, you should be able to walk an interviewer through the full pipeline in 10 minutes covering:

1. **The business problem** and why batch wasn't sufficient
2. **Architecture decisions** — why Medallion, why Delta, why each output mode
3. **One thing that went wrong** and how you debugged it (use the Spark UI)
4. **How you'd scale it** — what breaks first if trip volume 10x'd, and how you'd fix it

That last point — proactively identifying failure modes — is what separates a good SA answer from a great one.

---

## Quick Reference: Key Configs to Keep in Mind

| Config | Suggested starting value | Why |
|---|---|---|
| `spark.sql.shuffle.partitions` | `8` (local) / `200+` (cluster) | Default 200 is too high for local dev |
| `maxFilesPerTrigger` | `1` | Process one simulator batch at a time |
| `checkpointLocation` | Unique per query | Each streaming query needs its own checkpoint |
| Watermark delay | `10 minutes` for Silver, `30 minutes` for Gold | Balance lateness vs. state size |
| `trigger(processingTime=...)` | `'10 seconds'` | Gives you time to observe Spark UI between batches |

