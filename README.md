# Lakehouse Project (Databricks)

This project rebuilds the [SQL Server data warehouse](https://github.com/nasser-kiti/warehouse-project-local) as a lakehouse on Databricks, using the Medallion Architecture. Airflow lands source files in an immutable volume, and a Lakeflow job loads them into Bronze and runs the dbt transformations for Silver and Gold.

## Overview

- **Data Architecture**: a lakehouse on **Databricks** and Unity Catalog, organised into landing, Bronze, Silver and Gold layers.
- **Ingestion**: write-once, checksum-verified landing of source files, orchestrated by **Airflow**.
- **ELT**: raw data is loaded first, then transformed in the platform with **dbt**.
- **Orchestration**: Airflow handles getting data into the platform; a **Lakeflow** job (defined as code with Databricks Asset Bundles) runs everything inside it.
- **Data Quality**: integrity checks before Bronze, and dbt tests that gate each layer.

## Data Architecture

![Data architecture diagram](assets/images/image.png)

### Specifications

- **Data Sources**: two source systems (CRM and ERP) delivered as CSV files.
- **Raw data is never changed**: landed files are immutable; corrections arrive as new batches.
- **Reruns are safe**: every stage is idempotent, so retries and backfills never duplicate data.
- **Data Quality**: data quality issues are resolved in Silver, and tests stop bad data from reaching Gold.
- **Scope**: latest-state analytics; historisation is not required.

1. **Landing**: an Airflow task uploads the CSV files to a Unity Catalog volume, one write-once folder per source per ingest date, with a checksum manifest written last.
2. **Hand-off**: Airflow triggers the Lakeflow job by name, passing the ingest date, and waits for it to finish.
3. **Bronze**: the job verifies each manifest, then loads the files into Delta tables with `COPY INTO`, unchanged apart from ingestion metadata.
4. **Silver**: dbt deduplicates, trims, standardises and types the data. Tests must pass before Gold builds.
5. **Gold**: dbt models a star schema with business rules applied (in progress).

### Unity Catalog layout

| Object | Purpose |
|--------|---------|
| `catalog.landing.raw` (volume) | Immutable landed files and manifests |
| `catalog.bronze` | Raw Delta tables, one per source file |
| `catalog.silver` | Cleaned, deduplicated, typed tables (dbt) |
| `catalog.gold` | Star schema for reporting (dbt) |

---

## Data Pipeline

### Immutability

Raw data is protected in layers, from who can write to how storage behaves:

| Layer | Control | Status |
| ------- | --------- | -------- |
| 1. Access | Only the ingestion service principal can write to the volume; the volume is owned by a group, not a person | Planned (currently a personal account) |
| 2. Processing | The loader verifies every landed file against its manifest and fails on any discrepancy | ✅ Implemented (`verify_batch`) |
| 3. Bronze tables | `delta.appendOnly = true` on every Bronze table: `INSERT` and `COPY INTO` work; `UPDATE`, `DELETE` and row-changing `MERGE` are refused | ✅ Implemented (set at table creation in `load_bronze`) |
| 4. Storage | A bucket retention policy (S3 Object Lock, GCS retention, Azure immutable blobs) prevents changes or deletion for a set period | Requires an external volume; not available on managed storage |

Idempotency (safe reruns) and immutability (unchangeable raw data) are both separate guarantees for the pipeline; it needs both.

Layers 2 and 3 are only as strong as layer 1: a table owner can turn `delta.appendOnly` off again with `ALTER TABLE`. A legitimate deletion (for example a GDPR erasure request) is therefore a deliberate, reviewed operation: turn the flag off, delete, turn it back on.

### Landing (Ingestion)

- **Purpose**: move source files into the platform without changing them, and record exactly what arrived.
- **Code**: `src/lakehouse/landing/load_volume.py`, run by the `land_files` task in `airflow/dags/lakehouse_ingest.py`.

| Decision | Choice | Why |
|----------|--------|-----|
| Landing target | Unity Catalog volume, `<source>/ingest_date=YYYY-MM-DD/` | Raw files stay replayable and governed by the catalog |
| Source contract | `data_sources.yaml` maps each file to its Bronze table | Adding a source is a config change, shared by landing and Bronze |
| Batch date | Airflow's logical date (`{{ ds }}`), never the clock | Reruns and backfills land in the right folder |
| Write-once | `overwrite=False`, plus a check of every file before uploading | A conflict fails the whole batch before anything is written |
| Retries | Skip files already landed with the same SHA-256 | A retry resumes a half-finished batch instead of duplicating it |
| Completion signal | `_manifest.json` (file, table, SHA-256, size) written last | Downstream never reads a half-landed batch |
| Corrections | A corrected file lands under a new ingest date | History is never rewritten |
| Failure handling | Checksum conflicts raise `AirflowFailException` (no retry) | Retrying can't fix a conflict; it only delays the alert |
| Deployment | Package installed in the Airflow image; DAGs mounted for local editing only | The image that runs is the image that was built, as in production |
| Executor | LocalExecutor | One machine and two tasks; Celery would add infrastructure with no benefit |
| Schedule | Manual trigger while building | Static CSVs would land identical batches every day |

### Bronze (Raw Tables)

- **Purpose**: load landed files into Delta tables exactly as delivered, with enough metadata to trace every row to its file.
- **Code**: `src/lakehouse/bronze/load_bronze.py`, run as the `load_bronze` wheel task in `resources/lakehouse_pipeline.job.yml`.

| Decision | Choice | Why |
|----------|--------|-----|
| Trigger | Airflow passes `ingest_date` as a job parameter | The date flows end to end unchanged |
| Missing manifest | Fail the job | Airflow only triggers after landing succeeds, so a missing manifest is a real error |
| Integrity | Re-check every file's SHA-256 against the manifest before loading | Any change after landing stops the run before Bronze |
| All-or-nothing | All manifests are verified before any `COPY INTO` | Bronze never holds half a day |
| Load method | `COPY INTO` with explicit `FILES = (...)` | SQL-native, tracks loaded files, loads only files listed in the manifest |
| Idempotency | `COPY INTO` skips file paths already loaded into each table | Reruns of the same ingest date insert 0 rows (verified: 18,494 then 0) |
| Column types | Every source column as `STRING` (`inferSchema = false`) | No silent type guessing; typing happens in Silver |
| Schema | Tables created empty; `mergeSchema` builds columns from the header | No DDL to maintain for six tables |
| Append-only | `TBLPROPERTIES ('delta.appendOnly' = 'true')` at creation | Bronze rows can be added but never changed or deleted through SQL |
| Metadata | `_row_hash`, `_ingested_at`, `_source_file`, `_ingest_date` | Dedup, lineage, backfills |
| Partitioning | None | Tables are a few MB; partitioning under ~1 TB adds small-file overhead |
| Compute | Serverless, environment version 6 (Python 3.12) | No clusters to manage; same Python as local and Airflow |
| Deployment | Asset Bundle builds and deploys the wheel | The job is defined in the repo, not in the UI |

**Idempotency scope (tested).** Each layer is idempotent within its own scope:

| Mechanism | Idempotent per | Same date again | Identical file on a new date |
|-----------|----------------|-----------------|------------------------------|
| Manifest + checksums (landing) | Ingest date | Skipped | Landed again (new folder) |
| `COPY INTO` (Bronze) | File path | 0 rows inserted | Loaded again (new path) |
| Key-based dedup (Silver) | Business key | No change | No change |

Verified by landing the same files for 2026-10-21 and 2026-10-22: Bronze doubled (`crm_cust_info`: 36,988 rows, 18,494 distinct `_row_hash`) while Silver stayed at 18,484 customers. Bronze faithfully records that the source sent the same content twice; Silver turns it into one correct current state.

Trade-off: with daily full-snapshot deliveries, Bronze grows by one full copy per day (cents per month at this volume). The planned fix, once the DAG runs on a schedule, is to skip files at landing whose checksum matches the previous batch and record them as `unchanged` in the manifest.

### Silver (Cleansing & Standardisation)

- **Purpose**: one clean, typed, deduplicated row per business entity.
- **Code**: `dbt/models/silver/{crm,erp}/`, run as the `load_silver` dbt task (`dbt build --select source:bronze path:models/silver`) after `load_bronze`. `dbt build` runs each model's tests, so a failing Silver test stops the job before Gold.
- **Ingest date**: not passed to Silver. It builds from whatever Bronze holds; dedup picks the right rows.

| Decision | Choice | Why |
|----------|--------|-----|
| Dedup | Every model keeps one row per business key; the latest delivery (`_ingested_at`) wins, except `crm_cust_info`, where the latest `cst_create_date` wins first; `_row_hash` breaks ties | Bronze is append-only, so the same row can arrive in several deliveries |
| Typing | Cast once in a CTE; `try_to_date` for dates; `cast` (not `try_cast`) for keys | Bad dates become null; a bad key fails the build loudly |
| Money | `decimal(18,2)` | `int` drops cents, `float` rounds |
| Unknown values | `'n/a'` for unmapped codes and blanks | One convention across all models |
| Metadata | `_source_file`, `_ingest_date` kept | Lineage from Silver back to the landed file |
| Materialisation | Tables | Small data rebuilds in seconds; incremental models would add complexity with no benefit |
| Schemas | `generate_schema_name` writes to exactly `silver` | One environment; prefixing dev schemas waits for a second environment |

| Model | Grain | Key rules | Rows |
|-------|-------|-----------|------|
| `crm_cust_info` | One row per customer | Rows without `cst_id` excluded (warn test on source); latest `cst_create_date` wins; names trimmed, codes mapped (`M` → `Married`/`Male`) | 18,484 |
| `crm_prd_info` | One row per product version | `category_id` / `product_key` split from `prd_key`; source end dates ignored (200 of 397 end before they start); `end_date` = next version's start − 1 day, latest version open | 397 |
| `crm_sales_details` | One row per order line | `yyyyMMdd` dates parsed, invalid or pre-1990 dates null (19); price made positive or derived from sales; `order_total` = quantity × price | 60,398 |
| `erp_cust_az12` | One row per customer | `NAS` prefix stripped so `customer_key` matches CRM; future birth dates (16 placeholders like 9999-11-20) set to null | 18,484 |
| `erp_loc_a101` | One row per customer | Hyphens stripped from the key; country codes mapped (`US`/`USA`, `DE`); blanks `'n/a'` | 18,484 |
| `erp_px_cat_g1v2` | One row per category | Trimmed; `maintenance` as a boolean | 37 |

**Tests**: `unique` + `not_null` on every grain key; `accepted_values` on every mapped column (a missing mapping fails the build instead of leaking a raw code); singular tests for business rules (one open product version, valid date ranges, positive amounts, order before ship before due, one row per order line, Silver-to-Bronze reconciliation); `relationships` between models at `warn` severity.

**Known warnings** (reported on every run, by design):
- 4 Bronze `crm_cust_info` rows without `cst_id`.
- 19 sales rows without a valid `order_date`.
- Product category `CO_PE` has no match in `erp_px_cat_g1v2`; Gold keeps those products with a left join and `'n/a'` category.

### Gold (Star Schema): not started

---

## Testing

```bash
uv run python -m unittest discover tests
```

- **Landing** (`tests/test_load_volume.py`): a rerun does nothing, a changed file fails, a retry resumes a partial batch, a conflict uploads nothing, a correction lands under a new date.
- **Bronze** (`tests/test_load_bronze.py`): a changed or deleted file fails verification, and a tampered or missing batch in one source loads nothing for any source.
- **dbt**: source tests, model tests (`unique`, `not_null`, `accepted_values`), and a test that reconciles Silver row counts against Bronze.

Tests use in-memory and temp-folder fakes, so they need no Databricks access.

### Useful Links

[Setup Instructions](assets/docs/setup.md)
[Troubleshooting](assets/docs/debug.md)