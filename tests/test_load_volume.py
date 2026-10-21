"""Immutability tests for the landing upload, against an in-memory fake volume.

Run: uv run python -m unittest discover tests
"""
import io
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace

from databricks.sdk.errors import AlreadyExists, NotFound

from lakehouse.ingest.load_volume import MANIFEST_NAME, DataLoader, ImmutableBatchError

ROOT = "/Volumes/test_catalog/landing/raw"
DAY = date(2026, 10, 20)
BATCH = f"{ROOT}/crm/ingest_date=2026-10-20"

CONFIG = """
sources:
  crm:
    local_dir: data/crm
    files:
      - file: cust_info.csv
        table: crm_cust_info
      - file: prd_info.csv
        table: crm_prd_info
"""


class FakeFiles:
    """Mimics the parts of WorkspaceClient().files that the loader uses."""

    def __init__(self):
        self.store: dict[str, bytes] = {}
        self.writes: list[str] = []

    def create_directory(self, path):
        pass

    def upload(self, path, contents, *, overwrite=None):
        if path in self.store and not overwrite:
            raise AlreadyExists(f"{path} already exists")
        self.store[path] = contents.read()
        self.writes.append(path)

    def download(self, path):
        if path not in self.store:
            raise NotFound(f"{path} not found")
        return SimpleNamespace(contents=io.BytesIO(self.store[path]))


class LoadVolumeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self.tmp.name)
        (self.repo / "data/crm").mkdir(parents=True)
        (self.repo / "data/crm/cust_info.csv").write_text("cst_id,cst_key\n1,AW1\n")
        (self.repo / "data/crm/prd_info.csv").write_text("prd_id,prd_key\n10,P10\n")
        (self.repo / "sources.yaml").write_text(CONFIG)
        self.files = FakeFiles()

    def tearDown(self):
        self.tmp.cleanup()

    def loader(self) -> DataLoader:
        return DataLoader(client=SimpleNamespace(files=self.files), root=ROOT,
                          config_path=self.repo / "sources.yaml", repo_root=self.repo)

    def test_first_run_lands_all_files_and_writes_manifest_last(self):
        [result] = self.loader().upload_to_volume(DAY)

        self.assertEqual(result["status"], "landed")
        self.assertEqual(result["uploaded"], ["cust_info.csv", "prd_info.csv"])
        self.assertEqual(self.files.writes[-1], f"{BATCH}/{MANIFEST_NAME}")
        manifest = json.loads(self.files.store[f"{BATCH}/{MANIFEST_NAME}"])
        self.assertEqual([f["table"] for f in manifest["files"]], ["crm_cust_info", "crm_prd_info"])

    def test_rerun_same_date_is_a_no_op(self):
        self.loader().upload_to_volume(DAY)
        writes_after_first_run = list(self.files.writes)

        [result] = self.loader().upload_to_volume(DAY)

        self.assertEqual(result["status"], "already_complete")
        self.assertEqual(self.files.writes, writes_after_first_run)

    def test_changed_file_on_completed_batch_fails(self):
        self.loader().upload_to_volume(DAY)
        (self.repo / "data/crm/cust_info.csv").write_text("cst_id,cst_key\n1,AW2\n")  # one byte changed

        with self.assertRaisesRegex(ImmutableBatchError, r"changed=\['cust_info.csv'\]"):
            self.loader().upload_to_volume(DAY)

    def test_retry_after_partial_upload_resumes(self):
        # Simulate a crash after the first file landed: no second file, no manifest.
        self.files.store[f"{BATCH}/cust_info.csv"] = (self.repo / "data/crm/cust_info.csv").read_bytes()

        [result] = self.loader().upload_to_volume(DAY)

        self.assertEqual(result["skipped"], ["cust_info.csv"])
        self.assertEqual(result["uploaded"], ["prd_info.csv"])
        self.assertIn(f"{BATCH}/{MANIFEST_NAME}", self.files.store)

    def test_conflict_in_partial_batch_uploads_nothing(self):
        # A different version of prd_info already landed for this date.
        self.files.store[f"{BATCH}/prd_info.csv"] = b"prd_id,prd_key\n99,OLD\n"

        with self.assertRaises(ImmutableBatchError):
            self.loader().upload_to_volume(DAY)

        self.assertEqual(self.files.writes, [], "pre-flight must fail before any upload")
        self.assertNotIn(f"{BATCH}/cust_info.csv", self.files.store)

    def test_correction_lands_under_a_new_date(self):
        self.loader().upload_to_volume(DAY)
        (self.repo / "data/crm/cust_info.csv").write_text("cst_id,cst_key\n1,AW2\n")

        [result] = self.loader().upload_to_volume(date(2026, 10, 21))

        self.assertEqual(result["status"], "landed")
        self.assertIn(f"{ROOT}/crm/ingest_date=2026-10-21/cust_info.csv", self.files.store)

    def test_missing_local_file_fails_before_touching_the_volume(self):
        (self.repo / "data/crm/prd_info.csv").unlink()

        with self.assertRaises(FileNotFoundError):
            self.loader().upload_to_volume(DAY)
        self.assertEqual(self.files.writes, [])


if __name__ == "__main__":
    unittest.main()
