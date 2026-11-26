"""Copy the local raw layer to S3 (ADR-030).

    python -m jobpipe.upload             # upload new or changed files
    python -m jobpipe.upload --dry-run   # list what would be uploaded

Every file under data/raw/ is mirrored to s3://<bucket>/raw/ with the same
relative path, so the Hive-style partitions (source=/dt=/company=) carry over
unchanged and Glue/Athena can read them later (Stage 5c):

    data/raw/source=lever/dt=2026-11-26/company=zoox/response.json.gz
    s3://<bucket>/raw/source=lever/dt=2026-11-26/company=zoox/response.json.gz

"New or changed" is decided by MD5: S3's ETag of an object uploaded in ONE
part (put_object) with SSE-S3 encryption is the hex MD5 of its bytes. So one
LIST of the prefix tells us which local files S3 already has, without
downloading anything. Re-running uploads nothing: the job is idempotent.

It never deletes anything in S3 (the IAM policy doesn't even allow it,
ADR-032): a file that disappears locally stays in the lake.

Configuration comes from the environment or .env (ADR-009):
    JOBPIPE_S3_BUCKET                          target bucket (Terraform output)
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  the pipeline user's key
    AWS_REGION                                 e.g. us-east-2

Exit codes: 0 ok, 1 some uploads failed, 99 not configured (no bucket).
99 is what Airflow's BashOperator treats as "skipped", so the task shows as
skipped instead of failed until S3 is set up.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

log = logging.getLogger("jobpipe.upload")

DEFAULT_PREFIX = "raw"
EXIT_NOT_CONFIGURED = 99  # BashOperator's default skip_on_exit_code

# The raw layer only ever holds these; *.tmp files are half-written
# (storage._atomic_write) and must never be shipped.
CONTENT_TYPES = {".gz": "application/gzip", ".json": "application/json"}


@dataclass
class Plan:
    to_upload: list[tuple[Path, str, str]]  # (local path, S3 key, md5 hex)
    unchanged: int


def md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def local_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*") if p.is_file() and p.suffix in CONTENT_TYPES)


def key_for(root: Path, path: Path, prefix: str) -> str:
    # as_posix: S3 keys always use "/", whatever the OS.
    return f"{prefix}/{path.relative_to(root).as_posix()}"


def remote_etags(s3, bucket: str, prefix: str) -> dict[str, str]:
    """Key -> ETag (quotes stripped) for every object under the prefix.
    list_objects_v2 returns at most 1000 keys per call; the paginator follows
    the continuation tokens for us."""
    etags = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for obj in page.get("Contents", []):
            etags[obj["Key"]] = obj["ETag"].strip('"')
    return etags


def plan(root: Path, prefix: str, remote: dict[str, str]) -> Plan:
    to_upload, unchanged = [], 0
    for path in local_files(root):
        key = key_for(root, path, prefix)
        digest = md5_hex(path.read_bytes())
        if remote.get(key) == digest:
            unchanged += 1
        else:
            to_upload.append((path, key, digest))
    return Plan(to_upload, unchanged)


def upload_one(s3, bucket: str, path: Path, key: str, digest: str) -> None:
    data = path.read_bytes()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        # S3 recomputes the MD5 and rejects the PUT if it differs: corruption
        # in transit fails loudly instead of landing in the lake.
        ContentMD5=base64.b64encode(bytes.fromhex(digest)).decode(),
        # application/gzip, deliberately NOT Content-Encoding: gzip. With that
        # header some clients unzip on download, and a reader that then sees
        # ".gz" would try to unzip a second time.
        ContentType=CONTENT_TYPES[path.suffix],
    )


def run(s3, bucket: str, root: Path, prefix: str = DEFAULT_PREFIX, dry_run: bool = False) -> int:
    """Returns the number of failed uploads."""
    p = plan(root, prefix, remote_etags(s3, bucket, prefix))
    log.info("%d files to upload, %d already in s3://%s/%s/",
             len(p.to_upload), p.unchanged, bucket, prefix)
    failed = 0
    for path, key, digest in p.to_upload:
        if dry_run:
            log.info("would upload %s", key)
            continue
        try:
            upload_one(s3, bucket, path, key, digest)
            log.info("uploaded %s", key)
        except Exception as exc:  # keep going: one bad file shouldn't stop the rest
            failed += 1
            log.error("FAILED %s: %s", key, exc)
    return failed


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    p.add_argument("--prefix", default=DEFAULT_PREFIX, help="key prefix in the bucket")
    p.add_argument("--dry-run", action="store_true", help="list what would be uploaded, upload nothing")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Real environment variables win over .env (override=False), as in discover.
    load_dotenv(override=False)
    bucket = os.environ.get("JOBPIPE_S3_BUCKET")
    if not bucket:
        log.warning("JOBPIPE_S3_BUCKET is not set: skipping the upload. "
                    "Create the bucket with Terraform (infra/terraform/README.md), then add it to .env.")
        return EXIT_NOT_CONFIGURED

    import boto3  # imported here so --help and the not-configured path stay fast

    failed = run(boto3.client("s3"), bucket, args.raw_root, args.prefix, args.dry_run)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
