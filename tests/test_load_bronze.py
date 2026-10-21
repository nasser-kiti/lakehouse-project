"""Immutability and all-or-nothing tests for the Bronze load, against a temp-folder volume.

No Spark or Databricks needed: a fake Spark session records the SQL it would run.
Run: uv run python -m unittest discover tests
"""
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from lakehouse.bronze.load_bronze import BronzeLayer
from lakehouse.landing.load_volume import MANIFEST_NAME, ImmutableBatchError, sha256_of

DAY = date(2026, 10, 21)

CONFIG = """
sources:
  crm:
    local_dir: crm
    files:
      - file: cust_info.csv
        table: crm_cust_info
  erp:
    local_dir: erp
    files:
      - file: LOC_A101.csv
        table: erp_loc_a101
"""


class FakeSpark:
    """Records every statement; COPY INTO returns one result row like Databricks does."""

    def __init__(self):
        self.statements: list[str] = []

    def sql(self, statement):
        self.statements.append(statement)
        return self

    def first(self):
        return {"num_inserted_rows": 1}


class LoadBronzeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "raw"
        self.config = Path(self.tmp.name) / "sources.yaml"
        self.config.write_text(CONFIG)
        self.crm = self.land("crm", {"cust_info.csv": b"cst_id,cst_key\n1,AW1\n"}, {"cust_info.csv": "crm_cust_info"})
        self.erp = self.land("erp", {"LOC_A101.csv": b"CID,CNTRY\nAW-1,Kenya\n"}, {"LOC_A101.csv": "erp_loc_a101"})

    def tearDown(self):
        self.tmp.cleanup()

    def land(self, source: str, files: dict[str, bytes], tables: dict[str, str]) -> Path:
        """Simulate a completed landing: files plus a manifest with their checksums."""
        batch_dir = self.root / source / f"ingest_date={DAY.isoformat()}"
        batch_dir.mkdir(parents=True)
        for name, data in files.items():
            (batch_dir / name).write_bytes(data)
        manifest = {"files": [{"file": n, "table": tables[n], "sha256": sha256_of(d)} for n, d in files.items()]}
        (batch_dir / MANIFEST_NAME).write_text(json.dumps(manifest))
        return batch_dir

    def layer(self) -> BronzeLayer:
        return BronzeLayer(root=str(self.root), config_path=self.config)

    def manifest(self, batch_dir: Path) -> dict:
        return json.loads((batch_dir / MANIFEST_NAME).read_text())

    # --- verify_batch -----------------------------------------------------------

    def test_intact_batch_passes(self):
        self.layer().verify_batch(str(self.crm), self.manifest(self.crm))  # does not raise

    def test_file_changed_after_landing_fails(self):
        (self.crm / "cust_info.csv").write_bytes(b"cst_id,cst_key\n1,AW2\n")  # one byte changed

        with self.assertRaisesRegex(ImmutableBatchError, "changed after landing"):
            self.layer().verify_batch(str(self.crm), self.manifest(self.crm))

    def test_file_deleted_after_landing_fails(self):
        (self.crm / "cust_info.csv").unlink()

        with self.assertRaisesRegex(ImmutableBatchError, "missing from the volume"):
            self.layer().verify_batch(str(self.crm), self.manifest(self.crm))

    # --- load: all-or-nothing ---------------------------------------------------

    def test_intact_day_loads_every_file(self):
        spark = FakeSpark()

        results = self.layer().load(spark, DAY)

        self.assertEqual([r["table"].split(".")[-1] for r in results], ["crm_cust_info", "erp_loc_a101"])
        copies = [s for s in spark.statements if "COPY INTO" in s]
        self.assertEqual(len(copies), 2)
        self.assertIn("FILES = ('cust_info.csv')", copies[0])
        self.assertIn("DATE'2026-10-21'", copies[0])

    def test_tampered_file_in_one_source_loads_nothing(self):
        # ERP is tampered; CRM is fine. Bronze must not get CRM alone for this date.
        (self.erp / "LOC_A101.csv").write_bytes(b"CID,CNTRY\nAW-1,Uganda\n")
        spark = FakeSpark()

        with self.assertRaises(ImmutableBatchError):
            self.layer().load(spark, DAY)
        self.assertEqual(spark.statements, [], "verification must fail before any SQL runs")

    def test_missing_manifest_loads_nothing(self):
        (self.erp / MANIFEST_NAME).unlink()  # ERP only partly landed
        spark = FakeSpark()

        with self.assertRaisesRegex(FileNotFoundError, "partly landed"):
            self.layer().load(spark, DAY)
        self.assertEqual(spark.statements, [])


if __name__ == "__main__":
    unittest.main()
