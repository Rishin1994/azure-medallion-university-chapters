"""Central configuration for the university chapters medallion pipeline.

Everything is a plain constant so the pipeline stays runnable from a clone
with zero external configuration. In production these would come from
environment/config (Key Vault-backed app settings), never from code.
"""
from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Source API (public ArcGIS FeatureServer — no credentials required)
# ---------------------------------------------------------------------------
FEATURE_SERVER_URL = (
    "https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/"
    "UniversityChapters_Public/FeatureServer/0"
)
QUERY_URL = f"{FEATURE_SERVER_URL}/query"

# Scope of this data product: US west-coast states only.
IN_SCOPE_STATES = ("CA", "OR", "WA")

# Upstream view already filters Status = 'ACTIVE'; we still apply the
# three-state filter server-side so scope is explicit in the request.
WHERE_CLAUSE = "State IN ('CA','OR','WA')"

# Page size for pagination (service maxRecordCount is 1000; west-coast volume
# is tiny, but pagination is implemented so the pipeline survives growth).
PAGE_SIZE = 1000

HTTP_TIMEOUT_SECONDS = 30

# ---------------------------------------------------------------------------
# Lake layout (ADLS-style paths, local filesystem for this take-home)
#
#   lake/bronze/university_chapters/ingest_date=YYYY-MM-DD/run_id=<run_id>/
#   lake/silver/university_chapters/
#   lake/gold/university_chapters/v1/
#   lake/quarantine/university_chapters/run_id=<run_id>/      (DQ-Q1 only)
#   lake/_runs/<run_id>.json                                  (run metrics)
# ---------------------------------------------------------------------------
LAKE_ROOT = Path(os.environ.get("LAKE_ROOT", "lake"))

DATASET = "university_chapters"
GOLD_VERSION = "v1"

BRONZE_ROOT = LAKE_ROOT / "bronze" / DATASET
SILVER_PATH = LAKE_ROOT / "silver" / DATASET
GOLD_PATH = LAKE_ROOT / "gold" / DATASET / GOLD_VERSION
QUARANTINE_ROOT = LAKE_ROOT / "quarantine" / DATASET
RUNS_ROOT = LAKE_ROOT / "_runs"

# Fixture used for offline runs and tests (contains synthetic bad rows that
# exercise DQ-Q1 and DQ-W1 — live API data may be entirely clean).
FIXTURE_PATH = Path(__file__).resolve().parent.parent / "fixtures" / "university_chapters_fixture.json"


def bronze_run_dir(run_id: str, ingest_date: str) -> Path:
    """Bronze is append-by-run: each run lands in its own folder (history kept)."""
    return BRONZE_ROOT / f"ingest_date={ingest_date}" / f"run_id={run_id}"


def quarantine_run_dir(run_id: str) -> Path:
    return QUARANTINE_ROOT / f"run_id={run_id}"
