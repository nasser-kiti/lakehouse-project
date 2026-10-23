# Setup Instructions

## Technology Stack

| Area | Tool |
|------|------|
| Platform | Databricks (serverless), Unity Catalog, Delta Lake |
| Ingestion orchestration | Apache Airflow 3.3.2 (Docker Compose, LocalExecutor) |
| In-platform orchestration | Lakeflow Jobs, deployed with Databricks Asset Bundles |
| Transformation | dbt-core + dbt-databricks |
| Language | Python 3.12 everywhere: local, Airflow image and serverless environment version 6 |
| Packaging | uv (`uv_build` backend), one `lakehouse` wheel used by Airflow and Databricks |


### Prerequisites

- Docker Desktop
- [uv](https://docs.astral.sh/uv/)
- Databricks CLI:

  ```bash
  brew tap databricks/tap
  brew install databricks
  ```

- A Databricks workspace with Unity Catalog and serverless jobs

### Setup

1. **Install dependencies**

   ```bash
   uv sync
   ```

2. **Configure environment variables.** Copy `.env.example` to `.env.local` and fill in the values. For dbt Power User, add this to `.vscode/settings.json`:

   ```json
   "python.envFile": "${workspaceFolder}/.env.local"
   ```

3. **Create the landing volume** (Databricks SQL)

   ```sql
   CREATE VOLUME IF NOT EXISTS lakehouse_one.landing.raw;
   ```

4. **Deploy the Lakeflow job**

   ```bash
   databricks auth login --host https://<workspace>.cloud.databricks.com
   databricks bundle validate
   databricks bundle deploy
   ```

5. **Start Airflow.** Create `airflow/.env` with `AIRFLOW_UID`, `FERNET_KEY`, the Databricks variables and `AIRFLOW_CONN_DATABRICKS_DEFAULT`, then:

   ```bash
   cd airflow
   docker compose build
   docker compose up airflow-init
   docker compose up -d
   ```

   Open <http://localhost:8080> and trigger `lakehouse_ingest`.

6. **Run dbt** (from `dbt/`)

   ```bash
   dbt build --select source:bronze+
   ```
