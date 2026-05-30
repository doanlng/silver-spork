# YellowLine Analytics — NYC Taxi Streaming Pipeline

Real-time taxi operations pipeline built on Apache Spark Structured Streaming
and Delta Lake. Databricks SA Interview Prep Capstone Project.

## Architecture

```
Kaggle CSV
    │
    ▼
[Stream Simulator]          <- replays historical data as a live file stream
    │
    ▼
[Bronze Layer]              <- raw ingest, no transforms, Delta append
    │
    ▼
[Silver Layer]              <- clean, enrich (zone lookup), deduplicate
    │
    ├──────────────────────────────────────┐
    ▼                                      ▼
[Gold: Hourly Revenue]       [Gold: 15-min Trip Volume]
[Gold: Anomaly Detection]
    │
    ▼
[Stream-Stream Join]         <- pickup + dropoff matched streams (M5)
```

## Project Structure

```
yellowline/
├── data/
│   ├── raw/                # drop your kaggle CSV here
│   └── lookup/             # taxi_zone_lookup.csv goes here
├── src/
│   ├── bronze/
│   │   └── ingest.py       # Milestone 1
│   ├── silver/
│   │   └── clean.py        # Milestone 2
│   ├── gold/
│   │   ├── revenue.py      # Milestone 3a
│   │   └── volume.py       # Milestone 3b
│   ├── anomaly/
│   │   └── detect.py       # Milestone 4
│   └── stream_join/
│       └── trips.py        # Milestone 5
├── utils/
│   ├── simulator.py        # stream simulator (provided)
│   ├── schema.py           # all schemas defined here
│   └── spark_session.py    # shared SparkSession factory
├── notebooks/              # optional: Databricks notebook versions
├── tests/
│   └── test_transforms.py  # unit test stubs
├── checkpoints/            # auto-created at runtime, gitignored
├── config.py               # paths and tunable constants
├── main.py                 # runs all layers together
└── README.md
```

## Quickstart

```bash
# 1. Install dependencies
pip install pyspark delta-spark pandas

# 2. Download dataset
#    https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data
#    Place CSV in: data/raw/yellow_tripdata.csv
#    Place lookup in: data/lookup/taxi_zone_lookup.csv

# 3. Run a single milestone
python -m src.bronze.ingest

# 4. Run the full pipeline
python main.py
```

## Delta Table Locations (configured in config.py)

| Table        | Path                        |
|--------------|-----------------------------|
| Bronze       | /tmp/yellowline/bronze/     |
| Silver       | /tmp/yellowline/silver/     |
| Gold Revenue | /tmp/yellowline/gold/revenue/ |
| Gold Volume  | /tmp/yellowline/gold/volume/  |
| Anomalies    | /tmp/yellowline/anomalies/  |
| Completed    | /tmp/yellowline/completed_trips/ |

## Spark UI

While any streaming query is running, open: http://localhost:4040

Key things to check:
- Stages tab → shuffle read/write, spill to disk, straggler tasks
- SQL tab → physical plan, Exchange nodes, join strategies
- Storage tab → cache utilization (if you add caching)
