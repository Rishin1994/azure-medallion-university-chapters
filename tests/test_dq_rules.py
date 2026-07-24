"""Unit tests for the DQ predicates (DQ-Q1 coordinates, DQ-W1 city)."""
from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline import dq_rules


def _coord_flags(spark, rows):
    """Build (lon, lat) as strings, try_cast to double like silver does, and
    return the DQ-Q1 predicate result per row. String source values cover the
    'non-numeric' case (try_cast → null → quarantined)."""
    schema = StructType(
        [StructField("lon", StringType(), True), StructField("lat", StringType(), True)]
    )
    df = spark.createDataFrame(rows, schema=schema)
    df = df.select(
        F.col("lon").try_cast("double").alias("lon"),
        F.col("lat").try_cast("double").alias("lat"),
    )
    return [
        r["bad"]
        for r in df.select(
            dq_rules.invalid_coordinates(F.col("lon"), F.col("lat")).alias("bad")
        ).collect()
    ]


def test_dq_q1_invalid_coordinates(spark):
    rows = [
        ("-120.66", "35.27"),   # valid → OK
        ("-180.0", "-90.0"),    # boundary values are valid → OK
        ("180.0", "90.0"),      # boundary values are valid → OK
        (None, "35.0"),         # missing lon → quarantine
        ("-120.0", None),       # missing lat → quarantine
        ("not-a-number", "35.0"),  # non-numeric → quarantine
        ("-190.5", "45.0"),     # lon out of range → quarantine
        ("120.0", "95.0"),      # lat out of range → quarantine
        ("-120.0", "-95.0"),    # lat out of range → quarantine
    ]
    assert _coord_flags(spark, rows) == [
        False, False, False, True, True, True, True, True, True,
    ]


def test_dq_w1_missing_or_unknown_city(spark):
    rows = [("San Luis Obispo",), ("Chico",), (None,), ("",), ("   ",),
            ("UNKNOWN",), ("unknown",), ("  Unknown  ",)]
    df = spark.createDataFrame(rows, schema=["city"])
    flags = [
        r["warned"]
        for r in df.select(
            dq_rules.missing_or_unknown_city(F.col("city")).alias("warned")
        ).collect()
    ]
    assert flags == [False, False, True, True, True, True, True, True]


def test_with_dq_flags_sets_status_and_warnings(spark):
    df = spark.createDataFrame(
        [
            ("CA-1", "Clean U", "Fresno", "CA", -119.74, 36.82),
            ("CA-2", "Warned U", "UNKNOWN", "CA", -118.24, 34.05),
            ("OR-3", "Quarantined U", "Portland", "OR", -190.5, 45.52),
        ],
        schema=["chapter_id", "chapter_name", "city", "state", "longitude", "latitude"],
    )
    out = {r["chapter_id"]: r for r in dq_rules.with_dq_flags(df).collect()}

    assert out["CA-1"]["is_quarantined"] is False
    assert out["CA-1"]["dq_status"] == "OK"
    assert out["CA-1"]["dq_warnings"] == []

    assert out["CA-2"]["is_quarantined"] is False
    assert out["CA-2"]["dq_status"] == "WARNING"
    assert out["CA-2"]["dq_warnings"] == [dq_rules.REASON_MISSING_OR_UNKNOWN_CITY]

    assert out["OR-3"]["is_quarantined"] is True
    assert out["OR-3"]["quarantine_reason"] == dq_rules.REASON_INVALID_COORDINATES
