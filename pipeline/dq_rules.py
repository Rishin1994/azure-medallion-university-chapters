"""Data-quality rules applied on the Bronze → Silver path.

Two severities, deliberately different behaviours:

  DQ-Q1  QUARANTINE (hard fail)  — invalid/missing coordinates.
         The row never enters Silver/Gold; it is written to the quarantine
         path with reason code INVALID_COORDINATES, the ingest_run_id and
         enough raw payload to debug.

  DQ-W1  WARNING (soft)          — city missing/blank/'UNKNOWN'.
         The row still publishes to Silver → Gold, flagged with
         dq_status='WARNING' and reason MISSING_OR_UNKNOWN_CITY appended
         to dq_warnings. Clean rows carry dq_status='OK' and no warnings.

Rules are expressed as PySpark Column predicates over the flattened silver
frame so they are individually unit-testable (see tests/test_dq_rules.py).
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Reason codes (documented in the Data Product Contract)
REASON_INVALID_COORDINATES = "INVALID_COORDINATES"
REASON_MISSING_OR_UNKNOWN_CITY = "MISSING_OR_UNKNOWN_CITY"

DQ_STATUS_OK = "OK"
DQ_STATUS_WARNING = "WARNING"

LON_MIN, LON_MAX = -180.0, 180.0
LAT_MIN, LAT_MAX = -90.0, 90.0


def invalid_coordinates(lon: Column, lat: Column) -> Column:
    """DQ-Q1 predicate: fail when lon/lat is missing, null, non-numeric
    (null after try_cast to double), NaN, or outside valid WGS84 ranges.

    Callers pass columns already try_cast to double; a non-numeric source
    value becomes null under try_cast (ANSI-safe) and is caught here.
    """
    lon_bad = lon.isNull() | F.isnan(lon) | (lon < F.lit(LON_MIN)) | (lon > F.lit(LON_MAX))
    lat_bad = lat.isNull() | F.isnan(lat) | (lat < F.lit(LAT_MIN)) | (lat > F.lit(LAT_MAX))
    return lon_bad | lat_bad


def missing_or_unknown_city(city: Column) -> Column:
    """DQ-W1 predicate: warn when city is null, blank/whitespace, or the
    literal 'UNKNOWN' (case-insensitive)."""
    trimmed = F.trim(city)
    return city.isNull() | (trimmed == F.lit("")) | (F.upper(trimmed) == F.lit("UNKNOWN"))


def with_dq_flags(df, lon_col: str = "longitude", lat_col: str = "latitude", city_col: str = "city"):
    """Annotate a flattened frame with is_quarantined / dq_status / dq_warnings."""
    quarantined = invalid_coordinates(F.col(lon_col), F.col(lat_col))
    warned = missing_or_unknown_city(F.col(city_col))
    return (
        df.withColumn("is_quarantined", quarantined)
        .withColumn("quarantine_reason", F.when(quarantined, F.lit(REASON_INVALID_COORDINATES)))
        .withColumn(
            "dq_status",
            F.when(warned, F.lit(DQ_STATUS_WARNING)).otherwise(F.lit(DQ_STATUS_OK)),
        )
        .withColumn(
            "dq_warnings",
            F.when(warned, F.array(F.lit(REASON_MISSING_OR_UNKNOWN_CITY))).otherwise(
                F.array().cast("array<string>")
            ),
        )
    )
