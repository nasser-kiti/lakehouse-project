
 # Bronze Architecture Decisions

| Decision | Choice | Why |
| ---------- | -------- | ----- |
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
