# Azure deployment — Synapse Spark

This folder deploys the university chapters medallion pipeline to **Azure Synapse
Analytics** with **Spark pools** and **Delta Lake on ADLS Gen2** — same DQ rules, same
ordering, same fail-loud policy as the local `pipeline/` package, upgraded with the
cloud pieces the local run only simulates.

Built for the current GA runtime, [Azure Synapse Runtime for Apache Spark 3.5](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-35-runtime)
(Spark 3.5 · **Delta Lake 3.2** · Python 3.11 · Java 17; supported to Oct 2027 — older
runtimes are [deprecated](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-version-support)).

> **Platform context.** Synapse Spark remains fully supported and is a fine home for
> this workload. Two credible alternatives: **Microsoft Fabric** (Microsoft's strategic
> direction — this design ports 1:1: Lakehouse ≈ the lake, Notebook ≈ the notebook,
> Data Pipeline ≈ the pipeline/trigger) and **Azure Databricks** (the canonical
> medallion/Delta platform — the notebook body would run there nearly unchanged as a
> Workflows job). Everything cloud-specific is isolated in this folder, so switching
> later is cheap.

## What gets deployed

| Piece | Resource | Role |
|---|---|---|
| Lake | ADLS Gen2 account, `lake` filesystem | `bronze/`, `silver/`, `gold/`, `quarantine/`, `_runs/`, `fixtures/` paths |
| Compute | Synapse Spark pool `sparkmed` (runtime 3.5, 3 × Small, auto-pause 15 min) | Runs the notebook; bills only while running |
| Code | Notebook `university_chapters_medallion` | The whole medallion run, parameterised (`storage_account`, `lake_container`, `source`, `run_id`) |
| Orchestration | Pipeline `pl_university_chapters_daily` | One `SynapseNotebook` activity, 1 retry, fails loudly |
| Schedule | Trigger `tr_daily_0600_utc` | Daily **06:00 UTC** — the contract's freshness SLA |

## What's upgraded vs the local run

- **Silver, Gold, quarantine and run-summaries are Delta tables.** History and time
  travel come free (`DESCRIBE HISTORY`, `VERSION AS OF`), which retires the local
  trade-off of "overwrite loses history".
- **Gold publish is one atomic `MERGE` on `chapter_id`** — update matched, insert new,
  **delete vanished** (`whenNotMatchedBySourceDelete`). Re-runs converge; a chapter
  that disappears upstream retires from the product with full audit history. This is
  the production idempotency the root README promised.
- **Quarantine is append-by-run, partitioned by `ingest_run_id`** — evidence
  accumulates, and the run summary lands in a queryable `_runs` Delta table instead of
  a JSON file.
- Bronze is unchanged in spirit: the raw payload as received, page files + metadata
  sidecar, under `ingest_date=…/run_id=…`, written via `mssparkutils.fs`.
- A notebook raise fails the Synapse activity, which fails the pipeline run — visible
  in Monitor and alertable via Azure Monitor. Nothing publishes as "success" without a
  publishable Gold.

## Deploy (one script)

Prerequisites: [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
logged in (`az login`), rights to create resources, and three env vars. No secrets are
stored in the repo — the SQL admin password exists only in your shell.

```bash
cd azure-synapse
export STG=stunichap<yourname>01        # globally unique, lowercase
export WS=syn-unichap-<yourname>        # globally unique
export SQL_ADMIN_PASSWORD='<strong password>'
bash deploy.sh
```

The script provisions everything (steps 1–8), starts the daily trigger, and kicks off
one live run (step 9), printing the command to watch it. First pool start adds a few
minutes of warm-up.

Run modes, any time after deploy:

```bash
# Live API run (default)
az synapse pipeline create-run --workspace-name $WS --name pl_university_chapters_daily \
  --parameters '{"storage_account":"'$STG'","lake_container":"lake","source":"live"}'

# Fixture run — exercises DQ-Q1 quarantine + DQ-W1 warning in the cloud
# (deploy.sh uploaded fixtures/university_chapters_fixture.json to the lake)
az synapse pipeline create-run --workspace-name $WS --name pl_university_chapters_daily \
  --parameters '{"storage_account":"'$STG'","lake_container":"lake","source":"fixture"}'
```

## Verify the product

Spark SQL (any notebook attached to the pool):

```sql
SELECT * FROM delta.`abfss://lake@<STG>.dfs.core.windows.net/gold/university_chapters/v1` ORDER BY chapter_id;
DESCRIBE HISTORY delta.`abfss://lake@<STG>.dfs.core.windows.net/gold/university_chapters/v1`;
SELECT * FROM delta.`abfss://lake@<STG>.dfs.core.windows.net/_runs/university_chapters` ORDER BY started_at_utc DESC;
```

Serverless SQL pool (no Spark running, pay-per-query):

```sql
SELECT TOP 100 * FROM OPENROWSET(
    BULK 'https://<STG>.dfs.core.windows.net/lake/gold/university_chapters/v1',
    FORMAT = 'DELTA') AS gold;
```

Expected fixture-run ledger (same as local): `rows_in=7, quarantined=2, deduped=1,
warned=1, ok=3, gold=4 (CA=4, OR=0, WA=0)` — and a live run currently lands 3 clean CA
rows with OR/WA legitimately empty.

## Verification status of this folder

The notebook's cells were executed end-to-end locally on the pool's exact Spark
version (PySpark 3.5, Java 17): fixture ledger, gold contents, quarantine contents,
re-run convergence, delete-vanished semantics and the empty-batch hard-fail all pass.
The Delta-specific calls (`DeltaTable.merge`, `DESCRIBE HISTORY`, time travel) use
standard Delta Lake 3.2 API and run on the pool as-is; they were exercised locally
through a parquet-backed shim because this build environment cannot fetch Delta jars.

## Costs and teardown

The Spark pool bills vCore-hours only while running (3 × Small, auto-pause after 15
idle minutes) — a few-minute daily run costs pennies; the storage footprint is
megabytes. Serverless SQL bills per TB scanned (trivial here). Tear everything down:

```bash
az group delete --name rg-university-chapters --yes
```

## Files

```
azure-synapse/
├── README.md                                   # this file
├── deploy.sh                                   # end-to-end az CLI runbook
├── notebook/university_chapters_medallion.ipynb  # the medallion run (9 cells)
├── pipeline/pl_university_chapters_daily.json  # SynapseNotebook activity + params
└── trigger/tr_daily_0600_utc.json              # daily 06:00 UTC schedule
```
