"""Raw ("bronze") layer writer.

Layout (Hive-style partitions, the same convention S3 + Glue/Athena use):

    data/raw/source=<source>/dt=<YYYY-MM-DD>/company=<slug>/response.json
    data/raw/source=<source>/dt=<YYYY-MM-DD>/company=<slug>/_metadata.json

Why `key=value` folder names? Query engines (DuckDB, Athena, Spark) read them
as columns, so `WHERE dt = '2026-11-11'` only scans that folder ("partition
pruning").

Idempotency: the path is fully determined by (source, dt, company). Running
the job twice on the same day overwrites the same file instead of creating
a duplicate. That makes re-runs and Airflow retries safe.
"""
from __future__ import annotations

import json
import os
from pathlib import Path


class LocalRawStore:
    def __init__(self, root: str | Path = "data/raw"):
        self.root = Path(root)

    def partition_dir(self, source: str, dt: str, company: str) -> Path:
        return self.root / f"source={source}" / f"dt={dt}" / f"company={company}"

    def write(self, source: str, dt: str, company: str, body: bytes, metadata: dict) -> Path:
        d = self.partition_dir(source, dt, company)
        d.mkdir(parents=True, exist_ok=True)
        _atomic_write(d / "response.json", body)
        _atomic_write(d / "_metadata.json", json.dumps(metadata, indent=2).encode())
        return d

    def write_run_manifest(self, dt: str, run_id: str, manifest: dict) -> Path:
        d = self.root / "_runs" / f"dt={dt}"
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"run_{run_id}.json"
        _atomic_write(path, json.dumps(manifest, indent=2).encode())
        return path


def _atomic_write(path: Path, data: bytes) -> None:
    """Write to a temp file then rename, so a crash never leaves a half-written
    file that a downstream job could read. (os.replace is atomic on POSIX.)"""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)
