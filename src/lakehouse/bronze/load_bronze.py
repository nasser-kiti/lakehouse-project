"""
Bronze: load one ingest date's landed batches into Bronze Delta tables.

Runs as a Lakeflow task. Airflow passes the ingest date ({{ ds }}) as a job
parameter only after the upload succeeded, so a missing manifest is an error,
not a skip.
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
import sys

from pyspark.sql import SparkSession

from lakehouse.utils.source_config import ConfigManager
from lakehouse.landing.load_volume import CATALOG, CONFIG_PATH, MANIFEST_NAME, volume_root
from lakehouse.utils.logger import get_logger

logger = get_logger('Bronze Layer')

class BronzeLayer:
    def __init__(self, root: str | None = None,
                 config_path: Path = CONFIG_PATH):
        self.root = root or volume_root()
        self.config_path = config_path
        self.sources = ConfigManager(self.config_path).get_configs()

    def read_manifest(self, batch_dir: str) -> dict:
        manifest_path = Path(batch_dir) / MANIFEST_NAME
        if not manifest_path.is_file():
            raise FileNotFoundError(
                f"No manifest at {manifest_path}: the batch is missing or only partly landed."
            )
        return json.loads(manifest_path.read_text())

    def copy_into_sql(self, table_fqn: str, batch_dir: str, file: str, ingest_date: date) -> str:
        return f"""COPY INTO {table_fqn}
            FROM (
                SELECT
                    *,
                    xxhash64(*)                       AS _row_hash,
                    current_timestamp()               AS _ingested_at,
                    _metadata.file_path               AS _source_file,
                    DATE'{ingest_date.isoformat()}'   AS _ingest_date
                FROM '{batch_dir}'
            )
            FILEFORMAT = CSV
            FILES = ('{file}')
            FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
            COPY_OPTIONS ('mergeSchema' = 'true')
        """

    def load(self, spark: SparkSession, ingest_date: date) -> list[dict]:
        # Read every expected manifest before loading anything: if one source's
        # batch is missing, nothing loads, so Bronze never holds half a day.
        batches = []
        for source in self.sources:
            batch_dir = f"{self.root}/{source.name}/ingest_date={ingest_date.isoformat()}"
            batches.append((batch_dir, self.read_manifest(batch_dir)))

        results = []
        for batch_dir, manifest in batches:
            for f in manifest["files"]:
                table_fqn = f"{CATALOG}.bronze.{f['table']}"
                spark.sql(f"CREATE TABLE IF NOT EXISTS {table_fqn}")

                row = spark.sql(self.copy_into_sql(table_fqn, batch_dir, f["file"], ingest_date)).first()
                if row is None:
                    logger.error(f"COPY INTO returned no result for {f['file']} -> {table_fqn}")
                    raise RuntimeError(f"COPY INTO returned no result for {f['file']} -> {table_fqn}")

                logger.info(f"{f['file']} -> {table_fqn}: {row['num_inserted_rows']} rows inserted")
                results.append({"table": table_fqn, "file": f["file"], "rows_inserted": row["num_inserted_rows"]})
        return results

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Load one ingest date's landed batches into Bronze.")
    parser.add_argument("--ingest-date", required=True, type=date.fromisoformat)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    BronzeLayer().load(spark, args.ingest_date)


if __name__ == "__main__":
    sys.exit(main())
