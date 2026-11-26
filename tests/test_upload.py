"""jobpipe.upload: mirror data/raw to S3 (ADR-030).

moto replaces the AWS API in-process, so these tests need no account,
network or credentials, and nothing is ever written to real S3.
"""
import boto3
import pytest
from moto import mock_aws

from jobpipe import upload
from jobpipe.storage import LocalRawStore

BUCKET = "jobpipe-test-bucket"


@pytest.fixture
def s3(monkeypatch):
    # Fake credentials, so a real key in the environment can never be used.
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                 "AWS_SESSION_TOKEN": "testing", "AWS_DEFAULT_REGION": "us-east-2",
                 "AWS_REGION": "us-east-2"}.items():
        monkeypatch.setenv(k, v)
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET,
                             CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        yield client


@pytest.fixture
def raw(tmp_path):
    root = tmp_path / "raw"
    store = LocalRawStore(root)
    store.write("lever", "2026-01-15", "zoox", b'[{"id": 1}]', {"status": 200})
    store.write("greenhouse", "2026-01-15", "discord", b'{"jobs": []}', {"status": 200})
    store.write_run_manifest("2026-01-15", "run1", {"n_ok": 2})
    return root


def keys(s3):
    return sorted(o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", []))


def test_mirrors_the_partition_layout_under_raw(s3, raw):
    assert upload.run(s3, BUCKET, raw) == 0
    assert keys(s3) == [
        "raw/_runs/dt=2026-01-15/run_run1.json",
        "raw/source=greenhouse/dt=2026-01-15/company=discord/_metadata.json",
        "raw/source=greenhouse/dt=2026-01-15/company=discord/response.json.gz",
        "raw/source=lever/dt=2026-01-15/company=zoox/_metadata.json",
        "raw/source=lever/dt=2026-01-15/company=zoox/response.json.gz",
    ]
    obj = s3.get_object(Bucket=BUCKET, Key="raw/source=lever/dt=2026-01-15/company=zoox/response.json.gz")
    assert obj["Body"].read() == (raw / "source=lever/dt=2026-01-15/company=zoox/response.json.gz").read_bytes()
    assert obj["ContentType"] == "application/gzip"
    assert "ContentEncoding" not in obj


def test_second_run_uploads_nothing(s3, raw):
    upload.run(s3, BUCKET, raw)
    p = upload.plan(raw, "raw", upload.remote_etags(s3, BUCKET, "raw"))
    assert p.to_upload == [] and p.unchanged == 5


def test_changed_file_is_uploaded_again(s3, raw):
    upload.run(s3, BUCKET, raw)
    LocalRawStore(raw).write("lever", "2026-01-15", "zoox", b'[{"id": 1}, {"id": 2}]', {"status": 200})
    p = upload.plan(raw, "raw", upload.remote_etags(s3, BUCKET, "raw"))
    assert sorted(k for _, k, _ in p.to_upload) == [
        "raw/source=lever/dt=2026-01-15/company=zoox/_metadata.json",
        "raw/source=lever/dt=2026-01-15/company=zoox/response.json.gz",
    ]


def test_dry_run_uploads_nothing(s3, raw):
    assert upload.run(s3, BUCKET, raw, dry_run=True) == 0
    assert keys(s3) == []


def test_half_written_tmp_files_are_never_shipped(s3, raw):
    (raw / "source=lever/dt=2026-01-15/company=zoox/response.json.gz.tmp").write_bytes(b"partial")
    upload.run(s3, BUCKET, raw)
    assert not [k for k in keys(s3) if k.endswith(".tmp")]


def test_never_deletes_remote_files(s3, raw):
    upload.run(s3, BUCKET, raw)
    (raw / "source=lever/dt=2026-01-15/company=zoox/_metadata.json").unlink()
    upload.run(s3, BUCKET, raw)
    assert "raw/source=lever/dt=2026-01-15/company=zoox/_metadata.json" in keys(s3)


def test_one_failed_upload_does_not_stop_the_rest(s3, raw, monkeypatch):
    real = upload.upload_one

    def flaky(s3_, bucket, path, key, digest):
        if key.endswith("zoox/response.json.gz"):
            raise RuntimeError("network blip")
        real(s3_, bucket, path, key, digest)

    monkeypatch.setattr(upload, "upload_one", flaky)
    assert upload.run(s3, BUCKET, raw) == 1
    assert len(keys(s3)) == 4


def test_not_configured_exits_99_so_airflow_marks_it_skipped(monkeypatch, tmp_path):
    monkeypatch.delenv("JOBPIPE_S3_BUCKET", raising=False)
    monkeypatch.chdir(tmp_path)  # no .env here
    assert upload.main(["--raw-root", str(tmp_path)]) == 99
