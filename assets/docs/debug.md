# Troubleshooting

| Problem | Cause | Fix |
| --------- | ------- | ----- |
| `ModuleNotFoundError: lakehouse` in `airflow-cli` | Each service had its own image, and the `debug`-profile CLI image was stale | All services share `image: lakehouse-airflow:local`; rebuild after changes under `src/` |
| `chown ... Read-only file system` during `airflow-init` | Data was mounted inside `/opt/airflow` | Mount sources at `/data/source` |
| `Job ID for job name lakehouse_pipeline can not be found` | The Lakeflow job wasn't deployed yet | `databricks bundle deploy` |
| `Library installation failed ... incompatible Python version` | Serverless environment version 2 runs Python 3.11 | `environment_version: "6"` (Python 3.12) |
| `Invalid header value b'Bearer ...\r'` | Env files saved with Windows (CRLF) line endings | `sed -i '' $'s/\r$//' <file>`; `.gitattributes` enforces LF |
| `Env var required but not provided: DATABRICKS_HOST` in VS Code | Extensions don't see variables set in a terminal | `python.envFile` in `.vscode/settings.json` |
| `cannot configure default credentials` from the Databricks CLI | Variables were sourced without being exported | `set -a; source .env.local; set +a`, or `databricks auth login` |
| Airflow can't find the job after a dev deploy | `mode: development` prefixes job names | The bundle doesn't use development mode; the job name is the contract |
