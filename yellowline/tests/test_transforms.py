"""
tests/test_transforms.py — Unit tests for Silver transform functions.

Structured Streaming queries are hard to unit test end-to-end,
but the pure transform functions (filter, enrich, derive) are easy
to test against a static DataFrame.

Run with: python -m pytest tests/
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType,
)

from src.silver.clean import apply_quality_filters, enrich_with_zones, add_derived_columns


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("YellowLine-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# Minimal schema matching what apply_quality_filters expects (pre-cast strings)
TRIP_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
])

ZONE_SCHEMA = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])

# Reusable valid row — override only the field under test in each case
_VALID_ROW = (1, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 2, 3.5, 100, 200, 12.0, 2.0, 15.0)


def _make_df(spark, rows, schema=TRIP_SCHEMA):
    return spark.createDataFrame(rows, schema=schema)


# ── Silver: quality filter tests ─────────────────────────────────────────────

class TestQualityFilters:

    def test_negative_fares_are_dropped(self, spark):
        """Rows with fare_amount <= 0 should be filtered out."""
        rows = [
            (1, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 2, 3.5, 100, 200, -5.0,  2.0,  0.0),  # bad
            (1, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 2, 3.5, 100, 200,  0.0,  2.0,  0.0),  # bad (zero)
            _VALID_ROW,                                                                                # good
        ]
        result = apply_quality_filters(_make_df(spark, rows))
        assert result.count() == 1
        assert result.first()["fare_amount"] == 12.0

    def test_zero_distance_paid_trips_are_dropped(self, spark):
        """trip_distance == 0 with fare_amount > 2 should be filtered."""
        rows = [
            (1, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 2, 0.0, 100, 200, 10.0, 2.0, 12.0),  # bad
            (1, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 2, 0.0, 100, 200,  1.0, 0.0,  1.0),  # ok (fare <= 2)
            _VALID_ROW,                                                                               # good
        ]
        result = apply_quality_filters(_make_df(spark, rows))
        assert result.count() == 2
        vendor_ids = {r["trip_distance"] for r in result.collect()}
        assert 0.0 not in vendor_ids or all(
            r["fare_amount"] <= 2 for r in result.collect() if r["trip_distance"] == 0.0
        )

    def test_null_passenger_count_is_dropped(self, spark):
        """passenger_count of null or 0 should be filtered."""
        rows = [
            (1, "2023-01-01 10:00:00", "2023-01-01 10:30:00", None, 3.5, 100, 200, 12.0, 2.0, 15.0),  # null
            (1, "2023-01-01 10:00:00", "2023-01-01 10:30:00",    0, 3.5, 100, 200, 12.0, 2.0, 15.0),  # zero
            _VALID_ROW,                                                                                  # good
        ]
        result = apply_quality_filters(_make_df(spark, rows))
        assert result.count() == 1
        assert result.first()["passenger_count"] == 2

    def test_valid_rows_pass_through(self, spark):
        """A clean row should survive all filters unchanged."""
        result = apply_quality_filters(_make_df(spark, [_VALID_ROW]))
        assert result.count() == 1
        row = result.first()
        assert row["fare_amount"] == 12.0
        assert row["passenger_count"] == 2
        assert row["trip_distance"] == 3.5


# ── Silver: zone enrichment tests ─────────────────────────────────────────────

class TestZoneEnrichment:

    def test_pickup_and_dropoff_zones_are_resolved(self, spark):
        """PULocationID and DOLocationID should map to borough and zone names."""
        trip_rows = [_VALID_ROW]  # PULocationID=100, DOLocationID=200
        zone_rows = [
            (100, "Manhattan", "Upper East Side N", "Yellow Zone"),
            (200, "Brooklyn",  "Bay Ridge",          "Boro Zone"),
        ]
        trips_df = apply_quality_filters(_make_df(spark, trip_rows))
        zones_df = spark.createDataFrame(zone_rows, schema=ZONE_SCHEMA)

        result = enrich_with_zones(trips_df, zones_df).first()
        assert result["pickup_borough"] == "Manhattan"
        assert result["pickup_zone"]    == "Upper East Side N"
        assert result["dropoff_borough"] == "Brooklyn"
        assert result["dropoff_zone"]    == "Bay Ridge"

    def test_unknown_location_id_produces_null_zone(self, spark):
        """A LocationID not in the zone lookup should produce null borough/zone."""
        trip_rows = [(1, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 2, 3.5, 999, 888, 12.0, 2.0, 15.0)]
        zone_rows = [(100, "Manhattan", "Upper East Side N", "Yellow Zone")]
        trips_df = apply_quality_filters(_make_df(spark, trip_rows))
        zones_df = spark.createDataFrame(zone_rows, schema=ZONE_SCHEMA)

        result = enrich_with_zones(trips_df, zones_df).first()
        assert result["pickup_borough"] is None
        assert result["dropoff_borough"] is None


# ── Silver: derived column tests ──────────────────────────────────────────────

class TestDerivedColumns:

    def test_revenue_per_mile_computed_correctly(self, spark):
        """revenue_per_mile = total_amount / trip_distance."""
        result = apply_quality_filters(_make_df(spark, [_VALID_ROW]))
        result = add_derived_columns(result).first()
        # total_amount=15.0, trip_distance=3.5
        assert abs(result["revenue_per_mile"] - (15.0 / 3.5)) < 1e-6

    def test_revenue_per_mile_zero_distance_is_null(self, spark):
        """Division by zero should produce null, not crash."""
        rows = [(1, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 2, 0.0, 100, 200, 1.0, 0.0, 1.0)]
        result = apply_quality_filters(_make_df(spark, rows))
        result = add_derived_columns(result).first()
        assert result["revenue_per_mile"] is None


# ── Milestone 5: trip_id generation ──────────────────────────────────────────

class TestTripId:

    def test_trip_id_is_deterministic(self, spark):
        """Same input fields should always produce the same trip_id."""
        ...

    def test_different_trips_produce_different_ids(self, spark):
        """Two different trips should not collide on trip_id."""
        ...
