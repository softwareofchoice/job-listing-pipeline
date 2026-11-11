# job-listing-pipeline

Pipeline that finds companies hiring for data roles, pulls their job postings
from public job-board APIs (Greenhouse, Lever, Ashby), stores them raw, and
(soon) models and ranks them.

Design decisions and their trade-offs: [docs/decisions.md](docs/decisions.md).

## Setup

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/pytest -q
```

## Discovery (weekly)

Finds job boards hiring for the titles in `config/discovery_titles.txt` and
appends the valid ones to `config/companies.csv`.

```
discovery_titles.txt -> Brave Search (site:<board host> "<title>") -> slugs
                     -> validate against the board's API -> companies.csv
```

```bash
cp .env.example .env            # then set BRAVE_API_KEY (https://brave.com/search/api/)
PYTHONPATH=src .venv/bin/python -m jobpipe.discover --dry-run   # preview
PYTHONPATH=src .venv/bin/python -m jobpipe.discover             # append new boards
```

Default: 16 queries per run (4 titles x 4 hosts, 1 page), capped by
`--max-queries`. Roughly $0.08 a run at $5 per 1,000 requests.

## Ingestion (daily)

```
config/companies.csv  ->  ingest  ->  data/raw/source=*/dt=*/company=*/response.json.gz
                                      data/raw/_runs/dt=*/run_*.json   (run manifest)
```

```bash
PYTHONPATH=src .venv/bin/python -m jobpipe.ingest            # today
PYTHONPATH=src .venv/bin/python -m jobpipe.ingest --dt 2026-11-10 --source lever
```

Key properties: raw bytes stored unchanged (gzip-compressed, ~10x smaller), Hive-style partitions, idempotent
per (source, dt, company), atomic writes, per-company failure isolation,
retries with backoff on 429/5xx, 1 request per board per run.

To add a company by hand, add a row to `config/companies.csv` with
`discovered_via=manual`. The slug is the part of the board URL after the host,
e.g. `jobs.lever.co/<slug>`.

## Roadmap
1. ~~Ingestion~~, ~~title-based discovery~~
2. DuckDB + dbt: staging models that unify the 3 schemas, dedupe, SCD2 history
3. Feature extraction: skills, years of experience, seniority
4. Scoring against profile
5. Airflow + Docker, then S3/Glue/Athena + Terraform

Raw files are gzipped: inspect one with
`zcat data/raw/source=lever/dt=2026-11-11/company=zoox/response.json.gz | head -c 500`.
