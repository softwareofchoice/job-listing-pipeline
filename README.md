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

## Transformation (dbt + DuckDB)

```
data/raw/**/response.json.gz ─┬─ stg_greenhouse__postings ─┐
                              ├─ stg_lever__postings ──────┼─ int_postings__daily ─┐
                              └─ stg_ashby__postings ──────┘                       ├─ int_postings__scd2
data/raw/**/_metadata.json ───── stg_raw__fetches ─────────────────────────────────┘
```

```bash
.venv/bin/dbt build --project-dir dbt --profiles-dir dbt     # run models + data tests
```

Run from the repo root. Output goes to `data/warehouse.duckdb`.

- **Staging** (views): one row per posting per fetch day, with the same columns
  for every provider, read straight from the raw files (ADR-011), and
  `description_text` cleaned from HTML (ADR-012).
- **`int_postings__scd2`** (table): one row per *version* of a posting, with
  `valid_from` / `valid_to` / `end_reason` (`changed` or `closed`), rebuilt
  from raw every run (ADR-013). A posting closes when it's missing from a
  successful fetch of its board; failed fetches are skipped (ADR-014).

Example: what changed or closed recently?

```sql
select posting_key, title, valid_from, valid_to, end_reason
from int_postings__scd2
where end_reason is not null
order by valid_to desc;
```

## Feature extraction (dbt)

```
int_postings__scd2 ── int_posting_lines ─┬─ int_posting_skills ──────┐
seeds/skills.csv ────────────────────────┘                           ├─ int_posting_features
                                         └─ int_posting_experience ──┘
```

One row per posting **version** in `int_posting_features`:

- **Skills** from a regex dictionary, `dbt/seeds/skills.csv` (ADR-015, ADR-020), each
  tagged `required`, `preferred` or `unspecified` by the section it appears
  in (ADR-016). To track a new skill, add a row and run `dbt build`.
- **Years of experience:** every mention is kept in `int_posting_experience`;
  `years_min` / `years_max` summarize them (ADR-017).
- **Seniority** from title keywords, falling back to years (ADR-018), with
  `seniority_source` saying which rule decided.

Which skills do data-engineering postings ask for?

```sql
select skill, requirement_level, count(*) as postings
from int_posting_skills s
join int_posting_features f using (posting_version_key)
where f.is_current and f.title ilike '%data engineer%'
group by all
order by postings desc;
```

## Scoring (Stage 4)

```
int_posting_features ─┐
int_postings__scd2 ───┼─ int_posting_scores ── mart_top_matches   (your ranked list)
profile_* seeds ──────┘
```

Your profile is five CSVs in `dbt/seeds/` (ADR-022): skills and weights,
settings (years of experience, score weights), level fit, title rules, and
location rules. Edit them and re-run `dbt build`.

- **Hard filters** (ADR-023): job function (title) and location (Chicago, or
  remote within the US). Applied only in `mart_top_matches`;
  `int_posting_scores` keeps every current posting with `passes_title` /
  `passes_location` flags so you can see why something was filtered.
- **Score** (ADR-024): 0–100, a weighted sum of skill match (50), experience
  fit (25), level fit (15) and freshness (10). Unknown parts count as 0.5.
- **Freshness** (ADR-025): halves every 14 days since the pipeline first saw
  the posting.

```sql
select rank, score, company_slug, title, location_match, why, posting_url
from mart_top_matches order by rank limit 20;
```

## Roadmap
1. ~~Ingestion~~, ~~title-based discovery~~
2. ~~DuckDB + dbt: staging models that unify the 3 schemas, SCD2 history~~
3. ~~Feature extraction: skills, years of experience, seniority~~ (dictionary audited with an LLM: ADR-019, ADR-020)
4. ~~Scoring against profile~~
5. Airflow + Docker, then S3/Glue/Athena + Terraform

Raw files are gzipped: inspect one with
`zcat data/raw/source=lever/dt=2026-11-11/company=zoox/response.json.gz | head -c 500`.
