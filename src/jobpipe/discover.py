"""Weekly company discovery: find job boards hiring for our titles.

    # put BRAVE_API_KEY in .env (git-ignored), or export it
    python -m jobpipe.discover --dry-run           # show what would be added
    python -m jobpipe.discover                     # append new companies
    python -m jobpipe.discover --pages 2 --max-queries 40

How it works:
  1. For every (title, job-board host) pair, search Brave for
     `site:<host> "<title>"`, e.g. site:jobs.lever.co "Data Engineer".
  2. Each result URL is handed to the source adapters; the one that owns the
     host extracts the company slug (jobs.lever.co/zoox/123 -> lever/zoox).
  3. Slugs already in companies.csv are skipped (case-insensitive).
  4. Each NEW slug is validated by calling its real job-board API. Only boards
     that respond with at least one posting are added. This keeps typos and
     dead boards out of the daily ingest.
  5. Valid boards are appended to companies.csv with lineage columns
     (discovered_via, discovered_at), and a run manifest is written.

Why weekly and not daily: the set of companies hiring changes slowly, while
postings change daily. Discovery (reference data) and ingestion (fact data)
refresh on different schedules. See docs/decisions.md (ADR-007).

What is stored: only the extracted slugs and per-query counts. Raw search
results are NOT stored, because Brave's terms reserve result storage for
custom plans (ADR-008).
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

from .http import make_session
from .sources import SOURCES

log = logging.getLogger("jobpipe.discover")

BRAVE_URL = "https://api.search.brave.com/res/v1/web/search"
BRAVE_MAX_PAGES = 10  # offset is a page index, 0..9


@dataclass(frozen=True)
class Query:
    source: str
    host: str
    title: str

    @property
    def q(self) -> str:
        return f'site:{self.host} "{self.title}"'


@dataclass
class Candidate:
    source: str
    slug: str
    titles: set[str] = field(default_factory=set)

    @property
    def key(self) -> tuple[str, str]:
        return (self.source, self.slug.lower())


class BudgetExhausted(Exception):
    pass


class FatalSearchError(Exception):
    """Errors that will fail every query (bad key, no credits): stop the run."""


class BraveSearch:
    """Minimal Brave Web Search client with a hard query budget."""

    def __init__(self, session: requests.Session, api_key: str,
                 max_queries: int, pause_seconds: float = 1.0):
        self.session = session
        self.api_key = api_key
        self.max_queries = max_queries
        self.pause_seconds = pause_seconds
        self.used = 0

    def search(self, q: str, page: int) -> tuple[list[str], bool]:
        """Return (result URLs, more_results_available) for one page."""
        if self.used >= self.max_queries:
            raise BudgetExhausted
        if self.used:
            time.sleep(self.pause_seconds)
        self.used += 1  # count before the call: a failed request may still be billed
        resp = self.session.get(
            BRAVE_URL,
            params={"q": q, "count": 20, "offset": page},
            headers={"X-Subscription-Token": self.api_key, "Accept": "application/json"},
            timeout=30,
        )
        if resp.status_code in (401, 402, 403):
            raise FatalSearchError(f"Brave returned {resp.status_code}: check BRAVE_API_KEY and credits")
        resp.raise_for_status()
        data = resp.json()
        urls = [r["url"] for r in data.get("web", {}).get("results", []) if r.get("url")]
        more = bool(data.get("query", {}).get("more_results_available", False))
        return urls, more


def load_titles(path: Path) -> list[str]:
    lines = (line.strip() for line in path.read_text().splitlines())
    return [line for line in lines if line and not line.startswith("#")]


def build_queries(titles: list[str]) -> list[Query]:
    return [Query(src.name, host, title)
            for title in titles
            for src in SOURCES.values()
            for host in src.board_hosts]


def parse_board_url(url: str) -> tuple[str, str] | None:
    """(source, slug) for a recognised job-board URL, else None."""
    for src in SOURCES.values():
        slug = src.slug_from_url(url)
        if slug:
            return src.name, slug
    return None


def existing_keys(companies_csv: Path) -> set[tuple[str, str]]:
    """All companies already listed, active or not (an inactive row means
    we decided against it; discovery must not silently re-add it)."""
    with open(companies_csv, newline="") as f:
        return {(r["source"], r["company_slug"].lower()) for r in csv.DictReader(f)}


def validate_board(session: requests.Session, source: str, slug: str) -> dict:
    """Call the real job-board API. Valid = responds with >= 1 posting.

    Zero postings is rejected too: Lever returns [] for some unknown slugs,
    so an empty board is indistinguishable from a wrong one.
    """
    src = SOURCES[source]
    try:
        resp = session.get(src.url(slug), timeout=30)
        if resp.status_code == 404:
            return {"valid": False, "reason": "404 not found"}
        resp.raise_for_status()
        payload = resp.json()
        n = len(src.extract_postings(payload))
    except (requests.RequestException, ValueError, KeyError, TypeError) as e:
        return {"valid": False, "reason": f"{type(e).__name__}: {e}"}
    if n == 0:
        return {"valid": False, "reason": "0 postings"}
    return {"valid": True, "n_postings": n, "company_name": src.company_name(payload)}


def default_name(slug: str) -> str:
    return slug.replace("-", " ").replace("_", " ").title()


def append_companies(companies_csv: Path, rows: list[dict]) -> None:
    with open(companies_csv, newline="") as f:
        fieldnames = next(csv.reader(f))
    with open(companies_csv, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        w.writerows(rows)


def discover(search: BraveSearch, session: requests.Session, titles: list[str],
             companies_csv: Path, pages: int, dry_run: bool,
             pause_seconds: float = 1.0) -> dict:
    today = datetime.now(timezone.utc).date().isoformat()
    known = existing_keys(companies_csv)
    candidates: dict[tuple[str, str], Candidate] = {}
    query_log, search_errors = [], []
    budget_hit = False

    # 1-2. Search and extract slugs.
    for query in build_queries(titles):
        if budget_hit:
            break
        for page in range(min(pages, BRAVE_MAX_PAGES)):
            try:
                urls, more = search.search(query.q, page)
            except BudgetExhausted:
                budget_hit = True
                log.warning("query budget (%d) reached; stopping search", search.max_queries)
                break
            except requests.RequestException as e:
                search_errors.append({"q": query.q, "page": page, "error": str(e)})
                log.error("search failed: %s (page %d): %s", query.q, page, e)
                break

            found, new = set(), set()
            for url in urls:
                parsed = parse_board_url(url)
                # Only accept slugs from the host we searched, not stray links.
                if not parsed or parsed[0] != query.source:
                    continue
                cand = candidates.setdefault(
                    (parsed[0], parsed[1].lower()), Candidate(*parsed))
                cand.titles.add(query.title)
                found.add(cand.key)
                if cand.key not in known:
                    new.add(cand.key)
            query_log.append({"q": query.q, "page": page, "results": len(urls),
                              "boards": len(found), "not_yet_known": len(new)})
            log.info("%-55s p%d  %2d results  %2d boards  %2d new",
                     query.q, page, len(urls), len(found), len(new))
            if not more:
                break

    # 3-4. Validate only boards we don't already track.
    new_cands = [c for k, c in sorted(candidates.items()) if k not in known]
    added, rejected = [], []
    for i, cand in enumerate(new_cands):
        if i:
            time.sleep(pause_seconds)
        result = validate_board(session, cand.source, cand.slug)
        if not result["valid"]:
            rejected.append({"source": cand.source, "slug": cand.slug, "reason": result["reason"]})
            log.info("REJECT    %-10s %-25s %s", cand.source, cand.slug, result["reason"])
            continue
        added.append({
            "company_slug": cand.slug,
            "company_name": result["company_name"] or default_name(cand.slug),
            "source": cand.source,
            "active": "true",
            "discovered_via": "brave:" + "|".join(sorted(cand.titles)),
            "discovered_at": today,
            "n_postings": result["n_postings"],
        })
        log.info("%s %-10s %-25s %4d postings", "WOULD ADD" if dry_run else "ADD      ",
                 cand.source, cand.slug, result["n_postings"])

    # 5. Persist.
    if added and not dry_run:
        append_companies(companies_csv, added)

    return {
        "dry_run": dry_run,
        "queries_used": search.used,
        "budget_exhausted": budget_hit,
        "est_cost_usd": round(search.used * 5 / 1000, 4),  # $5 per 1,000 requests
        "boards_seen": len(candidates),
        "already_known": len(candidates) - len(new_cands),
        "n_added": len(added),
        "n_rejected": len(rejected),
        "added": added,
        "rejected": rejected,
        "search_errors": search_errors,
        "queries": query_log,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--companies", type=Path, default=Path("config/companies.csv"))
    p.add_argument("--titles", type=Path, default=Path("config/discovery_titles.txt"))
    p.add_argument("--runs-dir", type=Path, default=Path("data/discovery/_runs"))
    p.add_argument("--pages", type=int, default=1, help="result pages per query (20 results each, max 10)")
    p.add_argument("--max-queries", type=int, default=100, help="hard cap on Brave requests this run")
    p.add_argument("--pause", type=float, default=1.0, help="seconds between requests")
    p.add_argument("--dry-run", action="store_true", help="validate but don't modify companies.csv")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Read .env if present. Real environment variables win (override=False),
    # so a scheduler like Airflow can inject the key without a file.
    load_dotenv(override=False)
    api_key = os.environ.get("BRAVE_API_KEY")
    if not api_key:
        log.error("BRAVE_API_KEY is not set. Create a key at https://brave.com/search/api/ "
                  "and put it in .env (cp .env.example .env), or export it.")
        return 2

    session = make_session()
    search = BraveSearch(session, api_key, args.max_queries, args.pause)
    try:
        manifest = discover(search, session, load_titles(args.titles), args.companies,
                            args.pages, args.dry_run, args.pause)
    except FatalSearchError as e:
        log.error("%s", e)
        return 2

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:6]
    args.runs_dir.mkdir(parents=True, exist_ok=True)
    path = args.runs_dir / f"run_{run_id}.json"
    path.write_text(json.dumps({"run_id": run_id, **manifest}, indent=2))
    log.info("done%s: %d queries (~$%.3f), %d boards seen, %d known, %d added, %d rejected. manifest: %s",
             " (dry run)" if args.dry_run else "", manifest["queries_used"], manifest["est_cost_usd"],
             manifest["boards_seen"], manifest["already_known"], manifest["n_added"],
             manifest["n_rejected"], path)
    if args.dry_run and manifest["n_added"]:
        log.warning("DRY RUN: companies.csv was NOT modified. Re-run without --dry-run "
                    "to append these %d companies.", manifest["n_added"])
    return 1 if manifest["search_errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
