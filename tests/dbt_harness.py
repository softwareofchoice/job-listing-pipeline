"""Build the dbt project against a hand-made raw layer in a temp folder.

Shared by the dbt end-to-end tests: each test module describes the raw
fetches it wants, and gets back a read-only DuckDB connection to the result.
"""
import json
import os
import shutil
import subprocess
from datetime import date
from pathlib import Path

import duckdb
import pytest

from jobpipe.storage import LocalRawStore

REPO = Path(__file__).resolve().parents[1]
DBT = REPO / ".venv" / "bin" / "dbt"

requires_dbt = pytest.mark.skipif(not DBT.exists() and not shutil.which("dbt"), reason="dbt not installed")


def day(n: int) -> date:
    return date(2026, 1, n)


def build_warehouse(tmp: Path, fetches: dict) -> duckdb.DuckDBPyConnection:
    """fetches: {(source, company): {day_number: [posting, ...]}}.
    A day that's missing for a board means that fetch failed."""
    store = LocalRawStore(tmp / "raw")
    for (source, company), days in fetches.items():
        for n, jobs in days.items():
            dt = day(n).isoformat()
            body = jobs if source == "lever" else {"jobs": jobs}
            store.write(source, dt, company, json.dumps(body).encode(),
                        {"source": source, "company_slug": company, "dt": dt,
                         "http_status": 200, "n_postings": len(jobs), "run_id": "test",
                         "fetched_at": f"{dt}T06:00:00+00:00", "sha256": "x"})

    db = tmp / "test.duckdb"
    result = subprocess.run(
        [str(DBT) if DBT.exists() else "dbt", "build",
         "--project-dir", "dbt", "--profiles-dir", "dbt",
         "--target-path", str(tmp / "target"),
         "--vars", json.dumps({"raw_root": str(tmp / "raw")})],
        cwd=REPO, env={**os.environ, "JOBPIPE_DUCKDB_PATH": str(db), "DBT_LOG_PATH": str(tmp / "logs")},
        capture_output=True, text=True)
    assert result.returncode == 0, result.stdout[-4000:]
    return duckdb.connect(str(db), read_only=True)
