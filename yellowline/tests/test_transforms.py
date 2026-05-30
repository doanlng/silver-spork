"""
tests/test_transforms.py — Unit test stubs for transform functions.

Structured Streaming queries are hard to unit test end-to-end,
but the pure transform functions (filter, enrich, derive) are easy
to test against a static DataFrame. Write these as you go —
they'll also help you debug logic errors faster than running the full pipeline.

Run with: python -m pytest tests/
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("YellowLine-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ── Silver: quality filter tests ─────────────────────────────────────────────

class TestQualityFilters:

    def test_negative_fares_are_dropped(self, spark):
        """Rows with fare_amount <= 0 should be filtered out."""
        # TODO: create a small DataFrame with known good and bad rows,
        # call apply_quality_filters, assert only good rows remain.
        ...

    def test_zero_distance_paid_trips_are_dropped(self, spark):
        """trip_distance == 0 with fare_amount > 2 should be filtered."""
        ...

    def test_null_passenger_count_is_dropped(self, spark):
        """passenger_count of null or 0 should be filtered."""
        ...

    def test_valid_rows_pass_through(self, spark):
        """A clean row should survive all filters unchanged."""
        ...


# ── Silver: derived column tests ──────────────────────────────────────────────

class TestDerivedColumns:

    def test_revenue_per_mile_computed_correctly(self, spark):
        """revenue_per_mile = total_amount / trip_distance."""
        ...

    def test_revenue_per_mile_zero_distance_is_null_or_zero(self, spark):
        """Division by zero should not crash — should produce null or 0."""
        ...


# ── Milestone 5: trip_id generation ──────────────────────────────────────────

class TestTripId:

    def test_trip_id_is_deterministic(self, spark):
        """Same input fields should always produce the same trip_id."""
        ...

    def test_different_trips_produce_different_ids(self, spark):
        """Two different trips should not collide on trip_id."""
        ...
