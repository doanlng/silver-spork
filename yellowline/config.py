"""
config.py — Central configuration for the YellowLine pipeline.
Adjust paths and tuning parameters here rather than in individual modules.
"""

import os

# ── Data Paths ────────────────────────────────────────────────────────────────

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
DATA_DIR      = os.path.join(BASE_DIR, "data")

RAW_CSV       = os.path.join(DATA_DIR, "raw", "yellow_tripdata.csv")
ZONE_LOOKUP   = os.path.join(DATA_DIR, "lookup", "taxi_zone_lookup.csv")

# Simulator drops files here; Bronze reads from here
STREAM_INPUT  = "/tmp/yellowline/input/"

# Delta table output locations
BRONZE_PATH     = "/tmp/yellowline/bronze/"
SILVER_PATH     = "/tmp/yellowline/silver/"
GOLD_REVENUE    = "/tmp/yellowline/gold/revenue/"
GOLD_VOLUME     = "/tmp/yellowline/gold/volume/"
ANOMALIES_PATH  = "/tmp/yellowline/anomalies/"
COMPLETED_PATH  = "/tmp/yellowline/completed_trips/"

# Checkpoint locations — one per streaming query (never share these)
CKPT_BRONZE     = "/tmp/yellowline/checkpoints/bronze/"
CKPT_SILVER     = "/tmp/yellowline/checkpoints/silver/"
CKPT_GOLD_REV   = "/tmp/yellowline/checkpoints/gold_revenue/"
CKPT_GOLD_VOL   = "/tmp/yellowline/checkpoints/gold_volume/"
CKPT_ANOMALY    = "/tmp/yellowline/checkpoints/anomaly/"
CKPT_COMPLETED  = "/tmp/yellowline/checkpoints/completed/"

# ── Simulator Settings ────────────────────────────────────────────────────────

SIMULATOR_BATCH_SIZE     = 500       # rows per file drop
SIMULATOR_INTERVAL_SECS  = 2.0       # seconds between file drops
SIMULATOR_LIMIT_ROWS     = 200_000   # cap to keep local runs manageable

# ── Spark Tuning ──────────────────────────────────────────────────────────────

# Shuffle partitions — keep low for local dev, raise for cluster
SHUFFLE_PARTITIONS = 8

# How many simulator files Spark processes per micro-batch
MAX_FILES_PER_TRIGGER = 1

# Trigger interval — slow enough to observe Spark UI between batches
TRIGGER_INTERVAL = "10 seconds"

# ── Watermark Settings ────────────────────────────────────────────────────────
# Think carefully about what each value means before changing it.
# Longer = more correct results for late data. Shorter = lower latency.

SILVER_WATERMARK      = "10 minutes"
GOLD_REVENUE_WATERMARK = "30 minutes"
GOLD_VOLUME_WATERMARK  = "15 minutes"
ANOMALY_WATERMARK     = "10 minutes"
COMPLETED_WATERMARK   = "3 hours"     # max expected trip duration

# ── Anomaly Detection ─────────────────────────────────────────────────────────

FARE_ANOMALY_STDDEV_THRESHOLD = 3.0   # flag fares > mean + N*stddev
