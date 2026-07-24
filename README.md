# University Chapters — Azure Medallion Data Product

A thin, Azure-oriented **medallion pipeline** (Bronze → Silver → Gold) that ingests
university chapter data from a public ArcGIS FeatureServer and publishes it as a
governed, consumer-facing **data product** for US west-coast states (CA / OR / WA).

- **Data Product Contract:** [`docs/data_product_contract.md`](docs/data_product_contract.md)
- **Architecture note + diagram:** [`docs/architecture.md`](docs/architecture.md)

| Layer | Path (ADLS-style, local here) | Job |
|---|---|---|
| Bronze | `lake/bronze/university_chapters/ingest_date=…/run_id=…/` | Raw API payload as received + ingest metadata; append-by-run history |
| Silver | `lake/silver/university_chapters/` | Cleaned, typed, deduped, flattened to chapter grain (PySpark); DQ applied |
| Gold | `lake/gold/university_chapters/v1/` | Published product — clean + warned rows only, contract schema |
| Quarantine | `lake/quarantine/university_chapters/run_id=…/` | DQ-Q1 hard-fail rows, never published |

## How to run (from a clone)

Prerequisites: **Python 3.10+** and **Java 17 or 21** (required by Spark 4; local
Spark only, no cluster needed). No credentials — the source API is public.

```bash
python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 1) Offline run from the checked-in fixture — seeds both DQ paths on purpose
python -m pipeline.run_pipeline --source fixture

# 2) Live run against the public FeatureServer
python -m pipeline.run_pipeline --source live

# 3) Tests (unit DQ rules + offline end-to-end; assert quarantine/warning behaviour)
python -m pytest tests/ -v
```

Or with make: `make setup`, `make run-fixture`, `make run-live`, `make test`.

Expected fixture-run summary (also persisted to `lake/_runs/<run_id>.json`):

```
RUN SUMMARY run_… | rows_in=7 rows_quarantined=2 rows_warned=1 rows_ok=3 rows_gold=4 (CA=4 OR=0 WA=0)
```

Inspect the product: `python -c "from pipeline.transform_silver import get_spark; get_spark().read.parquet('lake/gold/university_chapters/v1').show()"`

## Data quality — quarantine vs warning

Both behaviours run on the Bronze → Silver path. Live rows may all be clean, so the
fixture seeds one row for each path (plus a second quarantine variant).

| Rule | Severity | Fails when | What happens |
|---|---|---|---|
| **DQ-Q1** | Quarantine (hard) | `longitude`/`latitude` missing, null, non-numeric, or lon ∉ [−180, 180], lat ∉ [−90, 90] | Row **never** enters Silver/Gold; written to the quarantine path with `quarantine_reason=INVALID_COORDINATES`, `ingest_run_id`, and the raw feature payload for debugging |
| **DQ-W1** | Warning (soft) | `city` null, blank, or literal `UNKNOWN` (case-insensitive) | Row **still publishes** with `dq_status='WARNING'` and `MISSING_OR_UNKNOWN_CITY` appended to `dq_warnings`; clean rows carry `dq_status='OK'` and `[]` |

Order of operations in Silver: scope filter → **DQ-Q1 quarantine** → dedupe by
`chapter_id` (highest `source_object_id` wins — a bad duplicate can never shadow a
good record) → **DQ-W1 flagging**. Per-run counts (`rows_in`, `rows_quarantined`,
`rows_warned`, `rows_ok`) are logged and persisted.

## Failure policy — fail loudly

An empty **OR/WA is a source data fact** (the upstream view filters `Status='ACTIVE'`;
those states currently have zero active chapters) and never blocks publishing. But the
pipeline hard-fails — non-zero exit, nothing published as "success" — when:

- the API returns an HTTP error **or** an ArcGIS error-in-200 payload;
- the batch is entirely empty (CA has a non-zero historical baseline);
- CA row count is 0 after DQ;
- any row with an unexpected `dq_status` reaches the Gold gate.

## Idempotency

- **Bronze**: append-by-run (each run in its own `ingest_date=…/run_id=…` folder) — re-runs add history, never destroy it.
- **Silver/Gold**: full **overwrite of the versioned path** per run, at `chapter_id` grain. Chosen over MERGE because the snapshot is small and the source is a full extract; re-running any number of times converges to the same state. On Databricks/Fabric this becomes a Delta `MERGE` keyed on `chapter_id` (see trade-offs).

## Repo layout

```
pipeline/
  config.py             # URLs, scope, lake layout
  ingest_bronze.py      # API → Bronze (pagination, fail-loud, fixture mode)
  dq_rules.py           # DQ-Q1 / DQ-W1 predicates (unit-testable)
  transform_silver.py   # Bronze → Silver + quarantine (PySpark)
  publish_gold.py       # Silver → Gold gate + publish (PySpark)
  run_pipeline.py       # Orchestrator + run summary logging
fixtures/               # Real-shaped API response with seeded bad rows
tests/                  # Unit DQ tests + offline end-to-end assertions
docs/                   # Architecture note, Data Product Contract
```

## Trade-offs, and what production would add

**Choices made here.** Local PySpark stands in for Azure Databricks / Fabric Spark —
same code shape, no cluster dependency for reviewers. Parquet on a local ADLS-style
folder layout stands in for ADLS Gen2 + Delta. Overwrite-per-run beats MERGE at this
volume; the trade-off is losing Silver/Gold history (Bronze keeps full history by run).
Ingest is plain `requests` (an ArcGIS reader adds nothing for one small endpoint) with
pagination via `exceededTransferLimit`/`resultOffset` even though today's volume fits
one page. The upstream's `MEVR_RD` (regional director — a person's name) is deliberately
dropped at Silver: no consumer use case, and it keeps the product person-data-free.

**Production next steps (ideas, deliberately not built):** Delta Lake tables with
`MERGE` on `chapter_id` + time travel; Azure Data Factory / Fabric pipeline schedule
(daily 06:00 UTC per the contract SLA) with alerting on the fail-loud conditions;
Great Expectations/dbt-style DQ suites replacing hand-rolled predicates; Purview
registration + lineage; Key Vault for any future secrets; CI running the fixture
end-to-end test on PRs; baseline-drift alert (e.g. CA count drops >50% run-over-run).
