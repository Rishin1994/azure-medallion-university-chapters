"""Bronze ingest: land the raw ArcGIS FeatureServer payload, as received.

Bronze rules:
  * No business transforms. The API response body is written verbatim
    (one JSON file per page) plus a small ingest-metadata sidecar.
  * Append-by-run: every run gets its own folder under an ingest_date
    partition, so history accumulates and a re-run never destroys evidence.
  * Fail loudly: HTTP errors, ArcGIS "error-in-200" bodies, and an empty
    result set all raise. An empty batch is never silently published —
    the CA baseline is non-zero, so zero rows means something is wrong
    upstream (OR/WA alone being empty is expected and fine).
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

import requests

from pipeline import config

log = logging.getLogger("pipeline.bronze")


class IngestError(RuntimeError):
    """Raised when the source cannot be ingested safely."""


def new_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return now.strftime("run_%Y%m%dT%H%M%SZ")


def _query_page(session: requests.Session, offset: int) -> dict:
    """Fetch one page from the FeatureServer query endpoint."""
    params = {
        "where": config.WHERE_CLAUSE,
        "outFields": "*",
        "returnGeometry": "true",
        "f": "json",
        "resultOffset": offset,
        "resultRecordCount": config.PAGE_SIZE,
    }
    resp = session.get(config.QUERY_URL, params=params, timeout=config.HTTP_TIMEOUT_SECONDS)
    if resp.status_code != 200:
        raise IngestError(f"FeatureServer returned HTTP {resp.status_code}: {resp.text[:500]}")
    payload = resp.json()
    # ArcGIS reports many failures inside a 200 response — treat as fatal.
    if "error" in payload:
        raise IngestError(f"FeatureServer returned an error payload: {payload['error']}")
    if "features" not in payload:
        raise IngestError(f"Unexpected FeatureServer response shape: {list(payload.keys())}")
    return payload


def fetch_live_pages() -> list[dict]:
    """Fetch all pages (handles exceededTransferLimit pagination)."""
    pages: list[dict] = []
    offset = 0
    with requests.Session() as session:
        while True:
            payload = _query_page(session, offset)
            pages.append(payload)
            n = len(payload["features"])
            log.info("Fetched page %d: %d features (offset=%d)", len(pages), n, offset)
            if payload.get("exceededTransferLimit") and n > 0:
                offset += n
            else:
                return pages


def load_fixture_pages(fixture_path: Path) -> list[dict]:
    """Offline mode: read the checked-in fixture instead of the live API.

    The fixture mirrors the real response shape and seeds synthetic bad rows
    so reviewers can see the DQ-Q1 quarantine and DQ-W1 warning paths without
    depending on live data quality.
    """
    if not fixture_path.exists():
        raise IngestError(f"Fixture not found: {fixture_path}")
    with fixture_path.open() as f:
        return [json.load(f)]


def ingest(source: str = "live", run_id: str | None = None) -> tuple[str, Path]:
    """Land bronze for one run. Returns (run_id, bronze_run_dir)."""
    started_at = datetime.now(timezone.utc)
    run_id = run_id or new_run_id(started_at)
    ingest_date = started_at.strftime("%Y-%m-%d")

    if source == "live":
        pages = fetch_live_pages()
        source_detail = config.QUERY_URL
    elif source == "fixture":
        pages = load_fixture_pages(config.FIXTURE_PATH)
        source_detail = str(config.FIXTURE_PATH)
    else:
        raise IngestError(f"Unknown source '{source}' (expected 'live' or 'fixture')")

    rows_in = sum(len(p["features"]) for p in pages)
    if rows_in == 0:
        # Empty-batch policy: hard fail. CA has a non-zero baseline, so an
        # empty result set means a broken filter/upstream — never publish it.
        raise IngestError(
            "FeatureServer returned 0 features for CA/OR/WA — refusing to land an "
            "empty bronze batch (empty OR/WA is a data fact, an empty batch is not)."
        )

    out_dir = config.bronze_run_dir(run_id, ingest_date)
    if out_dir.exists():
        shutil.rmtree(out_dir)  # idempotent re-land for an explicit same run_id
    out_dir.mkdir(parents=True)

    for i, payload in enumerate(pages, start=1):
        (out_dir / f"page_{i:04d}.json").write_text(json.dumps(payload))

    metadata = {
        "run_id": run_id,
        "ingest_date": ingest_date,
        "ingested_at_utc": started_at.isoformat(),
        "source": source,
        "source_detail": source_detail,
        "where_clause": config.WHERE_CLAUSE,
        "pages": len(pages),
        "rows_in": rows_in,
    }
    (out_dir / "_ingest_metadata.json").write_text(json.dumps(metadata, indent=2))
    log.info("Bronze landed: %s (%d rows, %d page(s))", out_dir, rows_in, len(pages))
    return run_id, out_dir


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Land bronze for the university chapters pipeline")
    parser.add_argument("--source", choices=["live", "fixture"], default="live")
    parser.add_argument("--run-id", default=None, help="Optional explicit run id")
    args = parser.parse_args()
    ingest(source=args.source, run_id=args.run_id)


if __name__ == "__main__":
    main()
