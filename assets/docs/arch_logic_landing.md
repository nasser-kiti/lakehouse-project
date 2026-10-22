 # Landing Architecture Decisions

| Decision | Choice | Why |
| ---------- | -------- | ----- |
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
