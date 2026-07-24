"""Gold publish (PySpark): the consumer-facing data product.

Gold contains clean + warned rows only — quarantined rows never reach it
(they were removed on the Bronze → Silver path). Schema and guarantees are
documented in docs/data_product_contract.md; the column list here is the
contract, so it is selected explicitly rather than passed through.

Idempotency choice: full overwrite of the versioned path
(lake/gold/university_chapters/v1/) each run. The dataset is small and
snapshot-shaped, so overwrite-by-run is simpler and safer than MERGE here;
on Databricks/Fabric this would be a Delta MERGE keyed on chapter_id (or an
overwritten partition) — stated in the README trade-offs.

Fail-loudly policy (contract §4):
  * refuse to publish if Silver is empty, or if CA has zero rows —
    CA has a non-zero historical baseline, so CA=0 means upstream breakage;
  * empty OR/WA is a source data fact and does NOT block publishing;
  * defence in depth: refuse to publish if any dq_status other than
    OK/WARNING somehow appears.
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline import config, dq_rules
from pipeline.transform_silver import get_spark

log = logging.getLogger("pipeline.gold")

GOLD_COLUMNS = [
    "chapter_id",
    "chapter_name",
    "city",
    "state",
    "longitude",
    "latitude",
    "dq_status",
    "dq_warnings",
    "ingest_run_id",
    "ingested_at_utc",
]


class GoldPublishError(RuntimeError):
    pass


def run(spark: SparkSession | None = None) -> dict:
    spark = spark or get_spark()
    silver = spark.read.parquet(str(config.SILVER_PATH))

    rows_silver = silver.count()
    if rows_silver == 0:
        raise GoldPublishError("Silver is empty — refusing to publish an empty Gold snapshot.")

    bad_status = silver.filter(
        ~F.col("dq_status").isin(dq_rules.DQ_STATUS_OK, dq_rules.DQ_STATUS_WARNING)
    ).count()
    if bad_status:
        raise GoldPublishError(
            f"{bad_status} row(s) with unexpected dq_status reached Silver — aborting publish."
        )

    ca_rows = silver.filter(F.col("state") == "CA").count()
    if ca_rows == 0:
        raise GoldPublishError(
            "CA row count is 0 but CA has a non-zero baseline — refusing to publish. "
            "(Empty OR/WA alone is an expected source data fact and does not block.)"
        )

    gold = silver.select(*GOLD_COLUMNS)
    gold.write.mode("overwrite").parquet(str(config.GOLD_PATH))

    by_state = {r["state"]: r["n"] for r in silver.groupBy("state").agg(F.count("*").alias("n")).collect()}
    counts = {
        "rows_gold": rows_silver,
        "rows_gold_ca": by_state.get("CA", 0),
        "rows_gold_or": by_state.get("OR", 0),
        "rows_gold_wa": by_state.get("WA", 0),
    }
    log.info("Gold published to %s: %s", config.GOLD_PATH, counts)
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    argparse.ArgumentParser(description="Silver → Gold publish").parse_args()
    run()


if __name__ == "__main__":
    main()
