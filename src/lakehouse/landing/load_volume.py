"""Landing: upload source files to the immutable landing volume.

Layout, one batch per source per ingest date:
    /Volumes/<catalog>/<schema>/<volume>/<source>/ingest_date=YYYY-MM-DD/<file>
    /Volumes/<catalog>/<schema>/<volume>/<source>/ingest_date=YYYY-MM-DD/_manifest.json  <- written last

Immutability rules:
  * Files are never overwritten (overwrite=False on every upload).
  * Re-running a date is idempotent: files already landed with the same SHA-256 are skipped.
  * A landed file with different contents fails the whole batch *before* anything is
    uploaded. Corrections arrive as a new ingest date, never as a rewrite of history.
  * _manifest.json is written last. Downstream (load_bronze) only reads batches that have
    one, so a half-landed batch is never loaded.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from lakehouse.utils.logger import get_logger
from lakehouse.utils.source_config import ConfigManager, SourceConfig


logger = get_logger('Landing Layer')

CATALOG = os.getenv("LAKEHOUSE_CATALOG", "bara_datacamp")


CONFIG_PATH = Path(__file__).with_name("data_sources.yaml")
# Where the source systems drop their files. Defaults to ./data when run from the repo root;
# the Airflow container sets it to the mounted drop zone.
SOURCE_DIR = Path(os.getenv("LAKEHOUSE_SOURCE_DIR", "data"))
MANIFEST_NAME = "_manifest.json"


class ImmutableBatchError(RuntimeError):
    """Raised when landing would change a file that has already landed."""


@dataclass(frozen=True)
class LocalFile:
    file: str
    table: str
    path: Path
    sha256: str
    bytes: int


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def volume_root() -> str:
    schema = os.getenv("LAKEHOUSE_LANDING_SCHEMA", "landing")
    volume = os.getenv("LAKEHOUSE_LANDING_VOLUME", "raw")
    return f"/Volumes/{CATALOG}/{schema}/{volume}"


class DataLoader:
    def __init__(self, client=None, root: str | None = None,
                 config_path: Path = CONFIG_PATH, source_dir: Path = SOURCE_DIR):
        # WorkspaceClient reads DATABRICKS_HOST / DATABRICKS_TOKEN from the environment.
        # A fake client can be injected for tests.
        self.client = client or WorkspaceClient()
        self.root = root or volume_root()
        self.config_path = config_path
        self.source_dir = Path(source_dir)
        self.sources = self.get_sources()

    def get_sources(self) -> list[SourceConfig]:
        sources = ConfigManager(self.config_path).get_configs()
        if not sources:
            logger.warning("No sources found in configuration.")
            return []

        logger.info(f"Retrieved {len(sources)} sources from configuration: ({', '.join([source.name for source in sources])})")
        return sources

    def upload_to_volume(self, ingest_date: date) -> list[dict]:
        """Lands every configured source for one logical date. Returns a summary per source."""
        results = []
        for source in self.sources:
            if not source.files:
                logger.warning(f"No files to upload for source {source.name}.")
                continue
            results.append(self.land_source(source, ingest_date))
        return results

    def land_source(self, source: SourceConfig, ingest_date: date) -> dict:
        batch_dir = f"{self.root}/{source.name}/ingest_date={ingest_date.isoformat()}"
        local_files = self._describe_local_files(source)

        # A manifest means the batch finished landing earlier: verify, don't rewrite.
        manifest = self._read_remote_manifest(batch_dir)
        if manifest is not None:
            self._assert_manifests_match(manifest, local_files, batch_dir)
            logger.info(f"{source.name}: {batch_dir} is already complete, nothing to upload.")
            return self._summary(source, batch_dir, uploaded=[], skipped=local_files, status="already_complete")

        # No manifest: a new batch, or a retry of one that failed part-way.
        # This avoids partial uploads being left in an inconsistent state.
        to_upload, skipped = [], []
        for f in local_files:
            remote_sha = self._remote_sha256(f"{batch_dir}/{f.file}")
            if remote_sha is None:
                to_upload.append(f)
            elif remote_sha == f.sha256:
                skipped.append(f)
            else:
                raise ImmutableBatchError(
                    f"{batch_dir}/{f.file} already landed with different contents "
                    f"(remote sha256 {remote_sha[:12]}…, local {f.sha256[:12]}…). "
                    "Land the corrected file under a new ingest date instead."
                )

        self.client.files.create_directory(batch_dir)  # no-op if it already exists
        for f in to_upload:
            logger.info(f"Uploading {f.path.relative_to(self.source_dir)} -> {batch_dir}/{f.file} ({f.bytes:,} bytes)")
            with f.path.open("rb") as fh:
                self.client.files.upload(f"{batch_dir}/{f.file}", fh, overwrite=False)
        for f in skipped:
            logger.info(f"Skipping {f.file}: already landed with the same checksum.")

        self._write_manifest(batch_dir, source, ingest_date, local_files)
        return self._summary(source, batch_dir, uploaded=to_upload, skipped=skipped, status="landed")

    # --- helpers -----------------------------------------------------------------

    def _describe_local_files(self, source: SourceConfig) -> list[LocalFile]:
        described = []
        for f in source.files:
            path = self.source_dir / source.local_dir / f.file
            if not path.is_file():
                raise FileNotFoundError(f"Source file not found: {path}")
            data = path.read_bytes()
            described.append(LocalFile(file=f.file, table=f.table, path=path, sha256=sha256_of(data), bytes=len(data)))
        return described

    def _read_remote_manifest(self, batch_dir: str) -> dict | None:
        try:
            return json.loads(self.client.files.download(f"{batch_dir}/{MANIFEST_NAME}").contents.read())
        except NotFound:
            return None

    def _remote_sha256(self, path: str) -> str | None:
        # The Files API exposes no checksum, so download the landed file and hash it.
        try:
            return sha256_of(self.client.files.download(path).contents.read())
        except NotFound:
            return None

    @staticmethod
    def _assert_manifests_match(manifest: dict, local_files: list[LocalFile], batch_dir: str) -> None:
        landed = {f["file"]: f["sha256"] for f in manifest.get("files", [])}
        local = {f.file: f.sha256 for f in local_files}
        if landed != local:
            changed = sorted(k for k in landed.keys() & local.keys() if landed[k] != local[k])
            added = sorted(local.keys() - landed.keys())
            removed = sorted(landed.keys() - local.keys())
            raise ImmutableBatchError(
                f"{batch_dir} is complete and differs from the local files "
                f"(changed={changed}, added={added}, removed={removed}). "
                "Land the corrected files under a new ingest date instead."
            )

    def _write_manifest(self, batch_dir: str, source: SourceConfig, ingest_date: date, files: list[LocalFile]) -> None:
        manifest = {
            "source": source.name,
            "ingest_date": ingest_date.isoformat(),
            "landed_at": datetime.now(timezone.utc).isoformat(),
            "files": [{"file": f.file, "table": f.table, "sha256": f.sha256, "bytes": f.bytes} for f in files],
        }
        payload = io.BytesIO(json.dumps(manifest, indent=2).encode("utf-8"))
        self.client.files.upload(f"{batch_dir}/{MANIFEST_NAME}", payload, overwrite=False)
        logger.info(f"{source.name}: wrote {batch_dir}/{MANIFEST_NAME}, batch complete.")

    @staticmethod
    def _summary(source: SourceConfig, batch_dir: str, uploaded: list[LocalFile], skipped: list[LocalFile], status: str) -> dict:
        # Plain JSON types only, so Airflow can store the return value as an XCom.
        return {
            "source": source.name,
            "batch_dir": batch_dir,
            "status": status,
            "uploaded": [f.file for f in uploaded],
            "skipped": [f.file for f in skipped],
        }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Land source files in the immutable landing volume.")
    parser.add_argument(
        "--ingest-date", required=True, type=date.fromisoformat,
        help="Logical date of the batch, YYYY-MM-DD. Airflow passes {{ ds }}; never derived from the clock.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        results = DataLoader().upload_to_volume(args.ingest_date)
    except ImmutableBatchError as exc:
        logger.error(str(exc))
        return 2
    for r in results:
        logger.info(f"{r['source']}: {r['status']} (uploaded={len(r['uploaded'])}, skipped={len(r['skipped'])})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
