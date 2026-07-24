# Data Product Contract — `university_chapters` (v1)

## 1. Name & owner

| | |
|---|---|
| **Product name** | `university_chapters` |
| **Version** | `v1` |
| **Technical owner** | Data Engineering — Rishin (repo owner); on-call: same |
| **Source** | Public ArcGIS FeatureServer `UniversityChapters_Public/0` (upstream view pre-filters `Status='ACTIVE'`) |
| **Consumer use cases** | Chapter analytics and reporting; mapping/geo visualisation of active west-coast chapters; input to growth/coverage dashboards |

## 2. Interface

- **Path:** `lake/gold/university_chapters/v1/` (parquet; Delta table `gold.university_chapters_v1` in production)
- **Grain:** one row per `chapter_id` (deduplicated; latest source record wins)
- **Scope:** states `CA`, `OR`, `WA` only; active chapters only (upstream view)

| Column | Type | Description |
|---|---|---|
| `chapter_id` | string | **Business key** (e.g. `CA-0355`); unique, stable |
| `chapter_name` | string | Display name of the university chapter |
| `city` | string | City; may be present-but-unreliable when warned (see DQ-W1) |
| `state` | string | USPS 2-letter code; one of `CA` / `OR` / `WA` |
| `longitude` | double | WGS84; guaranteed non-null and within [−180, 180] |
| `latitude` | double | WGS84; guaranteed non-null and within [−90, 90] |
| `dq_status` | string | `OK` or `WARNING` — **no other value can appear** (quarantined rows are excluded upstream) |
| `dq_warnings` | array\<string\> | Warning reason codes; empty array when `dq_status='OK'` |
| `ingest_run_id` | string | Technical lineage: bronze run that produced this snapshot |
| `ingested_at_utc` | timestamp | Technical: when the source was ingested (UTC) |

Technical columns (`ingest_run_id`, `ingested_at_utc`) are provided for lineage and
freshness checks; they are not business attributes. The upstream `OBJECTID` is kept
in Silver as `source_object_id` (technical) and not published in Gold.

## 3. Freshness

- **Intended SLA:** refreshed **daily by 06:00 UTC** (production trigger).
- Current mode is manual/on-demand; each snapshot carries `ingested_at_utc` so
  consumers can verify staleness themselves.
- One full snapshot per run — Gold always reflects a single, consistent run.

## 4. Quality

Rules applied on the Bronze → Silver path:

| Rule | Severity | Condition (fail when…) | Effect on this product |
|---|---|---|---|
| **DQ-Q1** | Quarantine (hard) | `longitude`/`latitude` missing, null, non-numeric, or out of WGS84 range | Row excluded from Gold entirely; written to the quarantine path with reason `INVALID_COORDINATES` |
| **DQ-W1** | Warning (soft) | `city` null, blank, or literal `UNKNOWN` (case-insensitive) | Row published with `dq_status='WARNING'` and reason `MISSING_OR_UNKNOWN_CITY` in `dq_warnings` |

Batch-level rules:

- **Empty OR/WA does not block publishing** — zero active OR/WA chapters is a known
  source data fact; consumers must not assume every state has rows and row counts
  for OR/WA > 0 are **not** required.
- The batch **fails (no publish)** when: the API errors, the whole batch is empty,
  or **CA drops to zero** (CA has a non-zero historical baseline — e.g. 3 active
  chapters — so CA=0 signals upstream breakage, not reality).
- Guarantees consumers can rely on: coordinates in Gold are always valid and
  non-null; `chapter_id` is unique; quarantined rows never appear.

## 5. Versioning

- The version lives in the path/table name (`…/v1/`).
- **Non-breaking changes** (adding a nullable column, adding a warning reason code)
  ship in place in `v1` and are announced in the repo changelog.
- **Breaking changes** (rename/retype/removal, grain or semantics change) ship as a
  parallel `…/v2/` publish; `v1` keeps refreshing during a deprecation window agreed
  with consumers, then is frozen and retired.

## 6. Classification

- **Public source data**; license/terms of the upstream ArcGIS Hub dataset apply.
- **No PII expected in this product.** The upstream field `MEVR_RD` (regional
  director — a person's name) is deliberately **not** ingested past Bronze and is
  excluded from Silver and Gold to keep the product free of person data.
- Classification label: **Public / non-sensitive**. No secrets are used or stored
  anywhere in the pipeline (the API is unauthenticated).
