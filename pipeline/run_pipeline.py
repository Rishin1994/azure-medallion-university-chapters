"""End-to-end orchestrator: Bronze → Silver (+quarantine) → Gold.

Usage:
    python -m pipeline.run_pipeline --source fixture   # offline, seeds DQ rows
    python -m pipeline.run_pipeline --source live      # calls the public API

Any failure (API error, empty batch, CA=0, unexpected dq_status) raises and
exits non-zero — the pipeline never reports success without a publishable
Gold snapshot. Per-run DQ counts are logged and persisted to
lake/_runs/<run_id>.json.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from pipeline import config, ingest_bronze, publish_gold, transform_silver

log = logging.getLogger("pipeline.run")


def run(source: str = "live", run_id: str | None = None) -> dict:
    started = datetime.now(timezone.utc)

    run_id, bronze_dir = ingest_bronze.ingest(source=source, run_id=run_id)

    spark = transform_silver.get_spark()
    try:
        silver_counts = transform_silver.run(run_id, spark=spark)
        gold_counts = publish_gold.run(spark=spark)
    finally:
        spark.stop()

    summary = {
        "run_id": run_id,
        "source": source,
        "started_at_utc": started.isoformat(),
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "bronze_dir": str(bronze_dir),
        **silver_counts,
        **gold_counts,
    }
    config.RUNS_ROOT.mkdir(parents=True, exist_ok=True)
    (config.RUNS_ROOT / f"{run_id}.json").write_text(json.dumps(summary, indent=2))

    log.info(
        "RUN SUMMARY %s | rows_in=%d rows_quarantined=%d rows_warned=%d rows_ok=%d "
        "rows_gold=%d (CA=%d OR=%d WA=%d)",
        run_id,
        summary["rows_in"],
        summary["rows_quarantined"],
        summary["rows_warned"],
        summary["rows_ok"],
        summary["rows_gold"],
        summary["rows_gold_ca"],
        summary["rows_gold_or"],
        summary["rows_gold_wa"],
    )
    return summary


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Run the full medallion pipeline")
    parser.add_argument("--source", choices=["live", "fixture"], default="live")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()
    try:
        run(source=args.source, run_id=args.run_id)
    except Exception:
        log.exception("Pipeline FAILED — nothing was published as success.")
        sys.exit(1)


if __name__ == "__main__":
    main()
