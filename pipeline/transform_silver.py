"""Silver transform (PySpark): cleaned, typed, deduped, flattened to chapter grain.

Steps, in order:
  1. Read the bronze run's raw page files (payload as received).
  2. Explode features; flatten attributes + point geometry into columns and
     rename to the published names (ChapterID → chapter_id, geometry.x/y →
     longitude/latitude, OBJECTID → source_object_id [technical]).
  3. Type + trim; defensively re-apply the CA/OR/WA scope filter.
  4. DQ-Q1 first: quarantine rows with invalid coordinates (they must never
     influence later steps), writing reason + raw payload to the quarantine
     path for this run.
  5. Dedupe surviving rows by chapter_id (keep highest source_object_id —
     deterministic "latest wins" for this source).
  6. DQ-W1: flag missing/blank/UNKNOWN city as WARNING (row still publishes).
  7. Write Silver parquet (overwritten each run; ingest_run_id column keeps
     lineage back to the bronze folder).

The regional-director column (MEVR_RD) is intentionally dropped here: it is a
person's name, no consumer use case needs it, and keeping it out of
Silver/Gold keeps the product free of person-data (see contract §6).
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline import config, dq_rules

log = logging.getLogger("pipeline.silver")


class SilverError(RuntimeError):
    pass


def get_spark(app_name: str = "university-chapters-medallion") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def find_bronze_run_dir(run_id: str) -> Path:
    matches = sorted(config.BRONZE_ROOT.glob(f"ingest_date=*/run_id={run_id}"))
    if not matches:
        raise SilverError(f"No bronze folder found for run_id={run_id} under {config.BRONZE_ROOT}")
    return matches[-1]


def read_bronze(spark: SparkSession, bronze_dir: Path) -> tuple[DataFrame, dict]:
    metadata = json.loads((bronze_dir / "_ingest_metadata.json").read_text())
    raw = (
        spark.read.option("multiLine", "true")
        .json(str(bronze_dir / "page_*.json"))
    )
    if "features" not in raw.columns:
        raise SilverError(f"Bronze payload at {bronze_dir} has no 'features' array")
    return raw, metadata


def flatten(raw: DataFrame, metadata: dict) -> DataFrame:
    """Explode the ArcGIS payload to one row per feature with typed columns."""
    features = raw.select(F.explode("features").alias("feature"))
    return features.select(
        F.trim(F.col("feature.attributes.ChapterID").cast("string")).alias("chapter_id"),
        F.trim(F.col("feature.attributes.University_Chapter").cast("string")).alias("chapter_name"),
        F.trim(F.col("feature.attributes.City").cast("string")).alias("city"),
        F.upper(F.trim(F.col("feature.attributes.State").cast("string"))).alias("state"),
        # try_cast: a non-numeric coordinate must become NULL (→ DQ-Q1
        # quarantine), not crash the job under Spark's ANSI-mode cast.
        F.col("feature.geometry.x").try_cast("double").alias("longitude"),
        F.col("feature.geometry.y").try_cast("double").alias("latitude"),
        F.col("feature.attributes.OBJECTID").cast("long").alias("source_object_id"),
        F.to_json(F.col("feature")).alias("raw_payload"),
    ).withColumns(
        {
            "ingest_run_id": F.lit(metadata["run_id"]),
            "ingested_at_utc": F.lit(metadata["ingested_at_utc"]).cast("timestamp"),
        }
    )


def transform(flat: DataFrame) -> tuple[DataFrame, DataFrame, dict]:
    """Apply scope filter, DQ-Q1 quarantine, dedupe, DQ-W1 warnings.

    Returns (silver_df, quarantine_df, counts).
    """
    rows_in = flat.count()

    scoped = flat.filter(F.col("state").isin(*config.IN_SCOPE_STATES))
    rows_out_of_scope = rows_in - scoped.count()

    flagged = dq_rules.with_dq_flags(scoped)

    quarantine_df = flagged.filter(F.col("is_quarantined")).select(
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "source_object_id",
        "quarantine_reason",
        "ingest_run_id",
        "ingested_at_utc",
        "raw_payload",
    )

    survivors = flagged.filter(~F.col("is_quarantined"))

    # Dedupe by business key AFTER quarantine so a bad duplicate can never
    # shadow a good record. Deterministic: keep highest source_object_id.
    w = Window.partitionBy("chapter_id").orderBy(
        F.col("source_object_id").desc_nulls_last(), F.col("chapter_name").asc()
    )
    deduped = (
        survivors.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    silver_df = deduped.select(
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "dq_status",
        "dq_warnings",
        "source_object_id",
        "ingest_run_id",
        "ingested_at_utc",
    )

    rows_quarantined = quarantine_df.count()
    rows_deduped = survivors.count() - deduped.count()
    rows_warned = silver_df.filter(F.col("dq_status") == dq_rules.DQ_STATUS_WARNING).count()
    rows_ok = silver_df.filter(F.col("dq_status") == dq_rules.DQ_STATUS_OK).count()

    counts = {
        "rows_in": rows_in,
        "rows_out_of_scope": rows_out_of_scope,
        "rows_quarantined": rows_quarantined,
        "rows_deduped": rows_deduped,
        "rows_warned": rows_warned,
        "rows_ok": rows_ok,
        "rows_silver": rows_warned + rows_ok,
    }
    return silver_df, quarantine_df, counts


def run(run_id: str, spark: SparkSession | None = None) -> dict:
    spark = spark or get_spark()
    bronze_dir = find_bronze_run_dir(run_id)
    raw, metadata = read_bronze(spark, bronze_dir)
    flat = flatten(raw, metadata)
    silver_df, quarantine_df, counts = transform(flat)

    # Quarantine and Silver/Gold are visibly different paths on disk.
    quarantine_dir = config.quarantine_run_dir(run_id)
    quarantine_df.write.mode("overwrite").parquet(str(quarantine_dir))
    silver_df.write.mode("overwrite").parquet(str(config.SILVER_PATH))

    log.info(
        "Silver written: rows_in=%(rows_in)d rows_quarantined=%(rows_quarantined)d "
        "rows_warned=%(rows_warned)d rows_ok=%(rows_ok)d (deduped=%(rows_deduped)d, "
        "out_of_scope=%(rows_out_of_scope)d)",
        counts,
    )
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Bronze → Silver transform")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run(args.run_id)


if __name__ == "__main__":
    main()
