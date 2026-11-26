"""dbt reading the raw layer from S3 instead of local files (ADR-031).

Uploads a tiny raw layer to a local moto S3 server with jobpipe.upload, then
builds the project twice: once from the local folder, once from
s3://<bucket>/raw. Both warehouses must be identical. This exercises the
whole path: upload keys, DuckDB's S3 secret (macros/s3_secret.sql) and
globbing Hive partitions over S3.

Needs DuckDB's httpfs and aws extensions, which DuckDB downloads on first
use; skipped when that isn't possible (offline).
"""
import socket

import boto3
import duckdb
import pytest

from dbt_harness import build_warehouse, requires_dbt, write_raw
from jobpipe import upload

pytestmark = requires_dbt

moto_server = pytest.importorskip("moto.server")

BUCKET = "jobpipe-test-bucket"
FAKE_AWS = {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_REGION": "us-east-2", "AWS_DEFAULT_REGION": "us-east-2"}

FETCHES = {
    ("greenhouse", "acme"): {
        1: [{"id": 1, "title": "Data Engineer", "location": {"name": "Remote (US)"},
             "absolute_url": "https://example.test/1", "content": "&lt;p&gt;Python and SQL&lt;/p&gt;"}],
        2: [],
    },
    ("lever", "beta"): {
        1: [{"id": "L1", "text": "Analytics Engineer", "categories": {"location": "Austin, TX"},
             "hostedUrl": "https://example.test/L1", "descriptionPlain": "dbt and Snowflake"}],
        2: [{"id": "L1", "text": "Analytics Engineer", "categories": {"location": "Austin, TX"},
             "hostedUrl": "https://example.test/L1", "descriptionPlain": "dbt and Snowflake"}],
    },
    ("ashby", "gamma"): {
        2: [{"id": "X", "title": "Data Engineer", "location": "Remote", "secondaryLocations": [],
             "jobUrl": "https://example.test/X", "descriptionPlain": "Airflow"}],
    },
}


@pytest.fixture(scope="module", autouse=True)
def s3_extensions():
    try:
        duckdb.connect().execute("install httpfs; install aws")
    except duckdb.Error as exc:
        pytest.skip(f"DuckDB S3 extensions unavailable: {exc}")


@pytest.fixture
def s3_endpoint(monkeypatch):
    for k, v in FAKE_AWS.items():
        monkeypatch.setenv(k, v)
    with socket.socket() as s:   # a free port
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    yield f"127.0.0.1:{port}"
    server.stop()


def rows(con, name):
    return con.execute(f"select * from {name} order by all").fetchall()


def test_building_from_s3_matches_building_from_local_files(tmp_path, s3_endpoint):
    write_raw(tmp_path / "raw", FETCHES)
    s3 = boto3.client("s3", endpoint_url=f"http://{s3_endpoint}")
    s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
    assert upload.run(s3, BUCKET, tmp_path / "raw") == 0

    local = build_warehouse(tmp_path, FETCHES, name="local")
    remote = build_warehouse(tmp_path, FETCHES, name="s3", raw_root=f"s3://{BUCKET}/raw",
                             env={**FAKE_AWS, "JOBPIPE_S3_ENDPOINT": s3_endpoint})

    # Tables only: staging models are views that still point at s3://, and this
    # plain connection has no S3 secret to follow them.
    for name in ["int_postings__scd2", "int_posting_features", "int_posting_scores"]:
        expected = rows(local, name)
        assert expected, f"{name} is empty: the test data didn't exercise anything"
        assert rows(remote, name) == expected, name
