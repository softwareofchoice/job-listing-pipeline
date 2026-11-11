"""Daily ingestion job.

    python -m jobpipe.ingest                    # all active companies, today
    python -m jobpipe.ingest --dt 2026-11-10    # backfill/re-run a specific date
    python -m jobpipe.ingest --source lever     # one provider only

Design choices worth knowing:
  * Raw response bytes are stored exactly as received (no parsing/cleanup),
    gzip-compressed losslessly (ADR-010).
    If our later transforms have a bug, we can re-run them from raw without
    re-fetching, and the source might not have the old data anymore.
  * One company failing never stops the others (failure isolation). Each
    result is recorded in a run manifest so you can see what failed.
  * Sequential requests with a pause in between: we only need ~10 requests a
    day, so there's no reason to hammer anyone's API with concurrency.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

from .http import make_session
from .sources import SOURCES
from .storage import LocalRawStore

log = logging.getLogger("jobpipe.ingest")


@dataclass
class Company:
    slug: str
    name: str
    source: str


def load_companies(path: Path, source_filter: str | None = None) -> list[Company]:
    companies = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if row["active"].strip().lower() != "true":
                continue
            if row["source"] not in SOURCES:
                raise ValueError(f"Unknown source {row['source']!r} for {row['company_slug']}")
            if source_filter and row["source"] != source_filter:
                continue
            companies.append(Company(row["company_slug"], row["company_name"], row["source"]))
    return companies


def fetch_one(session: requests.Session, store: LocalRawStore, company: Company,
              dt: str, run_id: str, timeout: float = 30) -> dict:
    source = SOURCES[company.source]
    url = source.url(company.slug)
    started = time.monotonic()
    result = {"company": company.slug, "source": company.source, "url": url}

    try:
        resp = session.get(url, timeout=timeout)
        result["http_status"] = resp.status_code
        resp.raise_for_status()
        payload = resp.json()  # validate it's JSON before we store it
        n_postings = len(source.extract_postings(payload))
    except (requests.RequestException, ValueError, KeyError, TypeError) as e:
        result.update(status="failed", error=f"{type(e).__name__}: {e}")
        log.error("FAIL  %-10s %-12s %s", company.source, company.slug, result["error"])
        return result

    metadata = {
        "run_id": run_id,
        "source": company.source,
        "company_slug": company.slug,
        "company_name": company.name,
        "dt": dt,
        "url": url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "http_status": resp.status_code,
        "n_postings": n_postings,
        "bytes": len(resp.content),  # uncompressed size
        # Checksum of the UNCOMPRESSED response, so it identifies the content
        # regardless of how it's stored. Lets us detect "nothing changed".
        "sha256": hashlib.sha256(resp.content).hexdigest(),
    }
    path = store.write(company.source, dt, company.slug, resp.content, metadata)

    result.update(status="ok", n_postings=n_postings,
                  seconds=round(time.monotonic() - started, 2), path=str(path))
    log.info("OK    %-10s %-12s %5d postings  %.1fs",
             company.source, company.slug, n_postings, result["seconds"])
    return result


def run(companies_csv: Path, raw_root: Path, dt: str, source_filter: str | None,
        pause_seconds: float) -> dict:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:6]
    companies = load_companies(companies_csv, source_filter)
    store = LocalRawStore(raw_root)
    session = make_session()

    log.info("run %s: %d companies, dt=%s", run_id, len(companies), dt)
    results = []
    for i, company in enumerate(companies):
        if i:
            time.sleep(pause_seconds)
        results.append(fetch_one(session, store, company, dt, run_id))

    failed = [r for r in results if r["status"] != "ok"]
    manifest = {
        "run_id": run_id,
        "dt": dt,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "n_companies": len(results),
        "n_ok": len(results) - len(failed),
        "n_failed": len(failed),
        "total_postings": sum(r.get("n_postings", 0) for r in results),
        "results": results,
    }
    path = store.write_run_manifest(dt, run_id, manifest)
    log.info("done: %d ok, %d failed, %d postings. manifest: %s",
             manifest["n_ok"], manifest["n_failed"], manifest["total_postings"], path)
    return manifest


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--companies", type=Path, default=Path("config/companies.csv"))
    p.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    p.add_argument("--dt", default=datetime.now(timezone.utc).date().isoformat(),
                   help="partition date (YYYY-MM-DD), defaults to today UTC")
    p.add_argument("--source", choices=sorted(SOURCES), help="only ingest one provider")
    p.add_argument("--pause", type=float, default=1.0, help="seconds between requests")
    args = p.parse_args(argv)

    datetime.strptime(args.dt, "%Y-%m-%d")  # fail fast on a bad date
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    manifest = run(args.companies, args.raw_root, args.dt, args.source, args.pause)
    # Non-zero exit if anything failed, so a scheduler (Airflow) can flag it.
    return 1 if manifest["n_failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
