# job-listing-pipeline

Daily pipeline that pulls job postings from public job-board APIs
(Greenhouse, Lever, Ashby), stores them raw, and (soon) models and ranks them.

## Stage 1: Ingestion (done)

```
config/companies.csv  ->  ingest.py  ->  data/raw/source=*/dt=*/company=*/response.json
                                          data/raw/_runs/dt=*/run_*.json   (run manifest)
```

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
PYTHONPATH=src .venv/bin/python -m jobpipe.ingest            # today
PYTHONPATH=src .venv/bin/python -m jobpipe.ingest --dt 2026-11-10 --source lever
.venv/bin/pytest -q
```

Key properties: raw bytes stored unchanged, Hive-style partitions, idempotent
per (source, dt, company), atomic writes, per-company failure isolation,
retries with backoff on 429/5xx, 1 request per board per run.

To add a company, add a row to `config/companies.csv`. The slug is the part
of the board URL, e.g. `jobs.lever.co/<slug>`.

## Roadmap
2. DuckDB + dbt: staging models that unify the 3 schemas, dedupe, SCD2 history
3. Feature extraction: skills, years of experience, seniority
4. Scoring against profile
5. Airflow + Docker, then S3/Glue/Athena + Terraform
