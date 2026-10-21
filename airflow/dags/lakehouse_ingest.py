from __future__ import annotations

import argparse
import pendulum
from airflow.exceptions import AirflowException
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import dag, task


@dag(
    dag_id="lakehouse_ingest",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 20, tz="Africa/Kampala"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2)},
    tags=["lakehouse", "landing"],
)
def lakehouse_ingest():

    @task
    def land_files(ingest_date: str) -> list[dict]:
        from datetime import date
        from lakehouse.landing.load_volume import DataLoader, ImmutableBatchError

        try:
            return DataLoader().upload_to_volume(date.fromisoformat(ingest_date))
        except ImmutableBatchError as exc:
            # A conflict won't fix itself on retry: fail straight away.
            raise AirflowException(str(exc)) from exc


    run_pipeline = DatabricksRunNowOperator(
        task_id="run_lakeflow_pipeline",
        databricks_conn_id="databricks_default",
        job_name="lakehouse_pipeline",
        job_parameters={"ingest_date": "{{ ds }}"},
    )

    land_files(ingest_date="{{ ds }}") >> run_pipeline

lakehouse_ingest()