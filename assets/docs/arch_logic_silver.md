 # Silver Architecture Decisions

| Decision | Choice | Why |
| ---------- | -------- | ----- |
| Dedup | Every model keeps one row per business key; the latest delivery (`_ingested_at`) wins, except `crm_cust_info`, where the latest `cst_create_date` wins first; `_row_hash` breaks ties | Bronze is append-only, so the same row can arrive in several deliveries |
| Typing | Cast once in a CTE; `try_to_date` for dates; `cast` (not `try_cast`) for keys | Bad dates become null; a bad key fails the build loudly |
| Money | `decimal(18,2)` | `int` drops cents, `float` rounds |
| Unknown values | `'n/a'` for unmapped codes and blanks | One convention across all models |
| Metadata | `_source_file`, `_ingest_date` kept | Lineage from Silver back to the landed file |
| Materialisation | Tables | Small data rebuilds in seconds; incremental models would add complexity with no benefit |
| Schemas | `generate_schema_name` writes to exactly `silver` | One environment; prefixing dev schemas waits for a second environment |
