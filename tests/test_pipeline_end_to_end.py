"""End-to-end test: fixture bronze → silver (+quarantine) → gold, fully offline.

Asserts the deliverable DQ behaviours (assignment D6/D7):
  * DQ-Q1 rows are quarantined with reason INVALID_COORDINATES and are
    NEVER present in Gold;
  * the DQ-W1 row IS present in Gold with dq_status='WARNING' and reason
    MISSING_OR_UNKNOWN_CITY;
  * clean rows publish with dq_status='OK' and empty warnings;
  * dedupe keeps one row per chapter_id (latest source_object_id wins);
  * Gold schema matches the Data Product Contract.
"""
from __future__ import annotations

import pytest

from pipeline import config, ingest_bronze, publish_gold, transform_silver
from pipeline.publish_gold import GOLD_COLUMNS

QUARANTINED_IDS = {"OR-9001", "WA-9002"}
WARNED_ID = "CA-9003"


@pytest.fixture(scope="module")
def pipeline_result(tmp_path_factory, monkeypatch_module, spark):
    """Run the whole pipeline once against the fixture, in a temp lake."""
    lake = tmp_path_factory.mktemp("lake")
    monkeypatch_module.setattr(config, "LAKE_ROOT", lake)
    monkeypatch_module.setattr(config, "BRONZE_ROOT", lake / "bronze" / config.DATASET)
    monkeypatch_module.setattr(config, "SILVER_PATH", lake / "silver" / config.DATASET)
    monkeypatch_module.setattr(config, "GOLD_PATH", lake / "gold" / config.DATASET / "v1")
    monkeypatch_module.setattr(config, "QUARANTINE_ROOT", lake / "quarantine" / config.DATASET)
    monkeypatch_module.setattr(config, "RUNS_ROOT", lake / "_runs")

    run_id, _ = ingest_bronze.ingest(source="fixture", run_id="run_test")
    silver_counts = transform_silver.run(run_id, spark=spark)
    gold_counts = publish_gold.run(spark=spark)

    gold = spark.read.parquet(str(config.GOLD_PATH))
    quarantine = spark.read.parquet(str(config.quarantine_run_dir(run_id)))
    return {
        "counts": {**silver_counts, **gold_counts},
        "gold": gold.collect(),
        "gold_columns": gold.columns,
        "quarantine": quarantine.collect(),
    }


@pytest.fixture(scope="module")
def monkeypatch_module():
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    yield mp
    mp.undo()


def test_dq_q1_rows_quarantined_and_never_in_gold(pipeline_result):
    gold_ids = {r["chapter_id"] for r in pipeline_result["gold"]}
    quarantined = {r["chapter_id"]: r for r in pipeline_result["quarantine"]}

    assert set(quarantined) == QUARANTINED_IDS
    assert gold_ids.isdisjoint(QUARANTINED_IDS), "quarantined rows must never reach Gold"
    for row in quarantined.values():
        assert row["quarantine_reason"] == "INVALID_COORDINATES"
        assert row["ingest_run_id"] == "run_test"
        assert row["raw_payload"]  # enough raw payload to debug


def test_dq_w1_row_published_with_warning(pipeline_result):
    gold = {r["chapter_id"]: r for r in pipeline_result["gold"]}
    warned = gold[WARNED_ID]
    assert warned["dq_status"] == "WARNING"
    assert "MISSING_OR_UNKNOWN_CITY" in warned["dq_warnings"]


def test_clean_rows_are_ok_with_empty_warnings(pipeline_result):
    for row in pipeline_result["gold"]:
        if row["chapter_id"] == WARNED_ID:
            continue
        assert row["dq_status"] == "OK"
        assert row["dq_warnings"] == []


def test_gold_only_contains_ok_and_warning(pipeline_result):
    assert {r["dq_status"] for r in pipeline_result["gold"]} <= {"OK", "WARNING"}


def test_dedupe_latest_wins(pipeline_result):
    gold = {r["chapter_id"]: r for r in pipeline_result["gold"]}
    ids = [r["chapter_id"] for r in pipeline_result["gold"]]
    assert len(ids) == len(set(ids)), "gold must be unique per chapter_id"
    # CA-0300 appears twice in the fixture; the higher source ObjectID (9004,
    # name 'Chico State University') must win.
    assert gold["CA-0300"]["chapter_name"] == "Chico State University"


def test_counts_and_contract_schema(pipeline_result):
    c = pipeline_result["counts"]
    assert c["rows_in"] == 7
    assert c["rows_quarantined"] == 2
    assert c["rows_deduped"] == 1
    assert c["rows_warned"] == 1
    assert c["rows_ok"] == 3
    assert c["rows_gold"] == 4
    assert c["rows_gold_ca"] == 4
    # OR/WA empty is a data fact, not a failure — publish still succeeded.
    assert c["rows_gold_or"] == 0
    assert c["rows_gold_wa"] == 0
    assert pipeline_result["gold_columns"] == GOLD_COLUMNS
