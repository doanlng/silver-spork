# YellowLine Analytics — Copilot Instructions

## Commands

```bash
# Install dependencies (run from yellowline/)
uv sync

# Run the full pipeline (starts simulator + all unlocked milestones)
cd yellowline && uv run python main.py

# Run a single milestone directly
cd yellowline && uv run python -m src.bronze.ingest
cd yellowline && uv run python -m src.silver.clean

# Run all tests
cd yellowline && uv run pytest tests/

# Run a single test
cd yellowline && uv run pytest tests/test_transforms.py::TestQualityFilters::test_negative_fares_are_dropped
```

## Architecture

Medallion streaming pipeline: CSV replay → Bronze → Silver → Gold/Anomaly/Join

```
Stream Simulator (daemon thread)
    │ JSON files → /tmp/yellowline/input/
    ▼
Bronze (src/bronze/ingest.py)      — raw Delta append, no transforms
    ▼
Silver (src/silver/clean.py)       — cast timestamps, quality filter, zone enrich, dedup
    ├──▶ Gold Revenue (src/gold/revenue.py)    — hourly revenue by borough (tumbling window)
    ├──▶ Gold Volume  (src/gold/volume.py)     — 15-min trip volume by zone (sliding window)
    └──▶ Anomaly      (src/anomaly/detect.py)  — foreachBatch + Delta MERGE for fraud flags
Bronze
    └──▶ Stream Join  (src/stream_join/trips.py) — pickup + dropoff stream-stream join
```

Data flows through Delta tables at `/tmp/yellowline/{bronze,silver,gold/revenue,gold/volume,anomalies,completed_trips}/`. Each streaming query has its own checkpoint under `/tmp/yellowline/checkpoints/`.

## Key Conventions

**Single source of truth for config** — all paths, checkpoint dirs, watermark delays, and tuning parameters live in `yellowline/config.py`. Never hardcode paths or constants in layer modules.

**SparkSession** — always call `get_spark()` from `utils/spark_session.py`; never construct a `SparkSession` directly in layer modules. Spark reuses the existing session.

**Schemas** — all schemas (TRIP_SCHEMA, ZONE_SCHEMA, SILVER_SCHEMA) are defined in `utils/schema.py`. Do not use `inferSchema` — it is unsafe on streaming sources and inconsistent across runs.

**Each module exposes a `run()` function** as its entry point. `main.py` calls these; toggle milestones by commenting/uncommenting imports there.

**Timestamps** — `tpep_pickup_datetime` and `tpep_dropoff_datetime` arrive as `StringType` from the Bronze JSON source. Cast them to `TimestampType` in the Silver layer before applying any watermark or window operations.

**Checkpoint directories must be unique per streaming query** — sharing a checkpoint between two queries corrupts state. All checkpoint paths are in `config.py` with a distinct constant per query.

**Stream simulator** — `utils/simulator.py` reads the Kaggle CSV, sorts by pickup time, and drops batches of JSON files to `STREAM_INPUT` every `SIMULATOR_INTERVAL_SECS`. Run it as a `daemon=True` thread so it exits when the main process exits. The simulator is already fully implemented.

**Unit testing approach** — streaming queries are not unit tested end-to-end. Instead, extract pure transform functions (filters, enrichment, derived columns) and test them against static DataFrames. The `spark` fixture in `tests/test_transforms.py` uses `local[2]` with `shuffle.partitions=2` — keep test sessions consistent with that fixture.

**Delta MERGE pattern** — when upsert semantics are needed (Anomaly layer), use `foreachBatch` to call `DeltaTable.forAlias(...).merge(...)`. Native streaming write modes do not support MERGE.

## Spark UI

While any streaming query is running: http://localhost:4040

- **Stages tab** — shuffle read/write, spill, straggler tasks
- **SQL tab** — physical plan, Exchange nodes, join strategies
- **Storage tab** — cache utilization
