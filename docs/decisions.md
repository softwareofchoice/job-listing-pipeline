# Architecture Decision Log

Every significant design decision in this pipeline is recorded here, including
the alternatives considered and why they were rejected. New decisions get the
next number. When a decision changes, don't edit the old one: add a new entry
that **supersedes** it, and set the old one's status to `Superseded by ADR-NNN`.

Each entry follows the same format:

- **Context:** the problem and the constraints.
- **Decision:** what we chose.
- **Alternatives:** what else we considered, and why not.
- **Consequences:** what this makes easier or harder, and when to revisit it.

| # | Decision | Status | Date |
|---|---|---|---|
| [001](#adr-001-use-public-job-board-apis-not-search-scraping) | Use public job-board APIs, not search scraping | Accepted | 2026-11-11 |
| [002](#adr-002-raw-layer-stores-responses-unchanged-in-hive-style-partitions) | Raw layer stores responses unchanged, Hive-style partitions | Accepted | 2026-11-11 |
| [003](#adr-003-develop-locally-with-s3-style-folders-duckdb-and-dbt) | Develop locally: S3-style folders, DuckDB and dbt | Accepted | 2026-11-11 |
| [004](#adr-004-one-adapter-class-per-job-board-provider) | One adapter class per job-board provider | Accepted | 2026-11-11 |
| [005](#adr-005-companiescsv-is-the-control-table-for-ingestion) | `companies.csv` is the control table for ingestion | Accepted | 2026-11-11 |
| [006](#adr-006-filter-and-rank-in-the-final-mart-not-at-ingestion) | Filter and rank in the final mart, not at ingestion | Accepted | 2026-11-11 |
| [007](#adr-007-discover-companies-by-job-title-with-brave-search-weekly) | Discover companies by job title with Brave Search, weekly | Accepted | 2026-11-11 |
| [008](#adr-008-discovery-safeguards-validation-budget-cap-no-result-storage) | Discovery safeguards: validation, budget cap, no result storage | Accepted | 2026-11-11 |
| [009](#adr-009-secrets-in-a-git-ignored-env-file-loaded-with-python-dotenv) | Secrets in a git-ignored `.env` file, loaded with python-dotenv | Accepted | 2026-11-11 |
| [010](#adr-010-gzip-compress-raw-responses) | Gzip-compress raw responses | Accepted | 2026-11-11 |

Open questions are listed [at the end](#open-questions).

---

## ADR-001: Use public job-board APIs, not search scraping

**Status:** Accepted, 2026-11-11

**Context.** The original idea was to run Boolean Google searches
(`site:boards.greenhouse.io "data engineer"`) and scrape the results. Scraping
Google violates its terms and gets blocked. Google's Custom Search JSON API is
closed to new signups and shuts down for existing users on 2027-01-01.

**Decision.** Fetch postings from the public JSON endpoints that Greenhouse,
Lever and Ashby provide for embedding job boards:

- Greenhouse: `boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true`
- Lever: `api.lever.co/v0/postings/{slug}?mode=json`
- Ashby: `api.ashbyhq.com/posting-api/job-board/{slug}`

**Alternatives.**

- *Scrape Google results:* against the terms, brittle, and blocked quickly.
- *Scrape job-board HTML:* breaks whenever the page layout changes, and the
  JSON endpoints already contain the same data in structured form.

**Consequences.** We get clean, structured data with full descriptions and no
HTML parsing. The cost is that each API is **per company**: there is no
cross-company search, so we need a list of companies to poll (ADR-005) and a
way to find new ones (ADR-007). Requests stay polite: one request per board per
day, with an identifying User-Agent, and retries with backoff only on 429 and
5xx errors.

---

## ADR-002: Raw layer stores responses unchanged in Hive-style partitions

**Status:** Accepted, 2026-11-11

**Context.** Later transformations will have bugs and will change. The job
boards only show *current* openings, so a posting that has closed can never be
fetched again.

**Decision.** Store each API response byte-for-byte at
`data/raw/source=<source>/dt=<YYYY-MM-DD>/company=<slug>/response.json`, with a
`_metadata.json` file (fetch time, HTTP status, posting count, sha256) next to
it, and a run manifest per run.

- The path depends only on (source, date, company), so **re-running the same
  day overwrites** the files instead of duplicating them (idempotency).
- Files are written to a temp file and then renamed, so a crash never leaves a
  half-written file behind (atomic writes).
- One company failing doesn't stop the others; failures are recorded in the
  run manifest.

**Alternatives.**

- *Parse and store only the fields we need:* smaller, but any field we didn't
  think of is lost forever, and parser bugs corrupt the history.
- *Append-only files with timestamped names:* keeps every fetch, but re-runs
  create duplicates that every downstream query has to remove.

**Consequences.** Downstream models can always be rebuilt from raw without
re-fetching. Query engines read the `key=value` folder names as columns, so
filtering by date only scans that date's folder (partition pruning). Storage
grows by about 34 MB a day for 10 companies, which will need compression or a
retention policy at larger scale.

---

## ADR-003: Develop locally with S3-style folders, DuckDB and dbt

**Status:** Accepted, 2026-11-11

**Context.** The production target is S3 + Glue + Athena, but AWS adds cost,
credentials and slow feedback while learning.

**Decision.** Write to local folders laid out exactly like S3 partitions.
Query and transform with DuckDB and dbt-duckdb.

**Alternatives.**

- *S3 from the start:* most realistic, but slower to iterate on and needs AWS
  setup before seeing any results.
- *MinIO (local S3) in Docker:* uses the real S3 API, but adds Docker setup.
- *Postgres:* needs a separate load step into tables.
- *Athena from the start:* every query has a cost and some latency.

**Consequences.** Moving to S3 only changes the storage location, not the
partition layout. dbt models carry over to Athena with small SQL dialect changes.

---

## ADR-004: One adapter class per job-board provider

**Status:** Accepted, 2026-11-11

**Context.** The three providers differ in URL format and response shape
(Greenhouse escapes the HTML in descriptions, Lever returns a bare list with
epoch-millisecond timestamps, Ashby uses ISO dates).

**Decision.** Each provider is a small `Source` subclass in
`src/jobpipe/sources/`. It knows its API URL, where the postings are in its
response, its public board hostnames, and how to extract a company slug from a
board URL. HTTP, storage, and search code is shared.

**Alternatives.**

- *`if source == ...` checks throughout the code:* every new provider means
  editing every function.

**Consequences.** Adding a provider (Workable, SmartRecruiters) is one new file.
Adapters deliberately do **not** normalize postings: unifying the three schemas
happens in dbt staging, so raw data stays untouched (ADR-002).

---

## ADR-005: `companies.csv` is the control table for ingestion

**Status:** Accepted, 2026-11-11

**Context.** Because the APIs are per company (ADR-001), ingestion needs a list
of which boards to poll.

**Decision.** `config/companies.csv` with the columns `company_slug` (machine
key used in URLs and paths), `company_name` (display label), `source` (which
adapter handles it), `active` (soft delete), and `discovered_via` /
`discovered_at` (lineage: where the row came from, added in ADR-007).

- An unknown `source` stops the run before anything is fetched: that's a
  config mistake we can fix.
- A bad slug or an API outage only fails that one company: that's outside our
  control.

**Alternatives.**

- *A database table:* worth it at thousands of companies or once rows are
  written automatically at high volume, but adds infrastructure now.
- *Hard-coding the list in Python:* changing the list would mean changing code.

**Consequences.** The list is version-controlled and changes are reviewable.
dbt can load it as a seed for `dim_company`. Revisit once the list reaches
thousands of rows or needs per-company state such as consecutive failures.

---

## ADR-006: Filter and rank in the final mart, not at ingestion

**Status:** Accepted, 2026-11-11

**Context.** Most ingested postings aren't relevant (sales, legal, the wrong
location). It's tempting to drop them early.

**Decision.** Ingest everything. Staging extracts structured features
(location, remote status, skills, years of experience, seniority). Personal
criteria are applied only in a final mart model that reads a profile config.

- **Hard filters** only for true "no" criteria: location or work
  authorization, and job function.
- **Scores**, not filters, for noisy or flexible criteria: skills, years of
  experience, and seniority. A regex mistake should rank a good job lower,
  not hide it without telling you.

**Alternatives.**

- *Filter at ingestion:* can't be undone, since the APIs only show current
  postings. It would also bias the skill-demand analysis and break SCD2's
  ability to tell "closed" apart from "filtered out."

**Consequences.** Changing your preferences means re-running one SQL model.
At very large scale, a coarse filter could run early, but only after raw
storage and as a view, never as a delete.

---

## ADR-007: Discover companies by job title with Brave Search, weekly

**Status:** Accepted, 2026-11-11

**Context.** We want coverage driven by **job title** (any company hiring data
engineers), not a hand-picked company list. The job-board APIs can't search
across companies (ADR-001).

**Decision.** A separate `discover` step (`python -m jobpipe.discover`):

1. Searches Brave for `site:<board-host> "<title>"` for every title in
   `config/discovery_titles.txt` and every board host (4 hosts:
   `boards.greenhouse.io`, `job-boards.greenhouse.io`, `jobs.lever.co`,
   `jobs.ashbyhq.com`).
2. Extracts (source, slug) from each result URL using the adapters (ADR-004).
3. Validates each new slug against its real API (ADR-008).
4. Appends valid boards to `companies.csv` with `discovered_via=brave:<titles>`.

Initial titles: Data Engineer, Data Platform Engineer, Analytics Engineer,
Software Engineer, Data.

**Schedule.** Discovery runs **weekly**; ingestion stays **daily**. The set of
hiring companies changes slowly, while postings change daily, so these two
refresh on different schedules (reference data vs. fact data).

**Cost.** Brave's "Search" plan is $5 per 1,000 requests with a monthly free
credit (the pricing page says both "$5" and "up to $20"; check the plans page).
Each request returns up to 20 results.

| Setting | Queries per run | Weekly cost per month |
|---|---|---|
| 4 titles × 4 hosts × 1 page (default) | 16 | ~$0.35 |
| × 5 pages | 80 | ~$1.73 |

Both fit inside the free credit. Daily ingestion API calls are free.

**Alternatives.**

- *Aggregator APIs with title search (such as Adzuna):* broad coverage, but
  shortened descriptions (which weakens skill extraction), redirect links, and
  duplicates across sources. Could be added later as a 4th source.
- *Bulk lists of known job-board slugs:* most coverage for the least effort,
  but thousands of daily requests to manage.
- *Daily discovery:* 7× the cost for little extra coverage.

**Consequences.** Coverage grows automatically toward companies hiring for our
titles. A company only appears once Brave has indexed one of its postings, so
coverage lags by days and misses companies with no matching titles indexed.
Title filtering still happens in staging (ADR-006): discovery decides which
boards to poll, not which postings to keep. Revisit if few new boards are
found per run (diminishing returns), or to add title variants.

---

## ADR-008: Discovery safeguards: validation, budget cap, no result storage

**Status:** Accepted, 2026-11-11

**Context.** Automated discovery writes to the control table and spends money,
so mistakes there spread into daily ingestion or the bill.

**Decision.**

- **Validate before adding:** a new slug is only added if its API responds with
  at least one posting. Zero postings is rejected too, because Lever returns
  `[]` for some unknown slugs, so an empty board can't be told apart from a
  wrong one. Rejected boards can be found again in a later run once they have
  postings.
- **Never re-add known rows:** comparison is case-insensitive and includes
  `active=false` rows, so a company you turned off stays off.
- **Accept only the searched host:** a Lever query only accepts Lever URLs,
  which ignores stray links in results.
- **Skip EU-hosted boards** (`jobs.eu.lever.co`, `job-boards.eu.greenhouse.io`),
  which use different API hosts. They can be added as separate sources later.
- **Hard budget:** `--max-queries` (default 100) caps Brave requests per run.
  A query counts toward the budget before it's sent. A 401, 402 or 403 from
  Brave stops the run immediately.
- **No raw search results stored:** Brave's FAQ says storing results requires a
  custom plan. We keep only the extracted slugs and per-query counts in the run
  manifest (`data/discovery/_runs/`). This is our reading of the terms; we
  haven't confirmed it with Brave.
- **Secrets:** the key comes from the `BRAVE_API_KEY` environment variable and
  is never committed (`.env` is in `.gitignore`).
- **`--dry-run`** shows what would be added without changing the CSV.

**Consequences.** The control table only ever gains live boards, and spending
has a fixed upper limit. Discovery doesn't automatically deactivate boards that
go quiet; that belongs in an ingestion health check (open question).

---

## ADR-009: Secrets in a git-ignored `.env` file, loaded with python-dotenv

**Status:** Accepted, 2026-11-11

**Context.** Discovery needs `BRAVE_API_KEY`, and later stages will need more
secrets (AWS credentials, warehouse passwords). Typing `export` in every new
shell is easy to forget, and hard-coding a key risks committing it.

**Decision.**

- Local secrets live in `.env` at the repo root. It's in `.gitignore`, and
  its permissions are `600` (only you can read it).
- `.env.example` **is committed**, with every variable name and no values, so
  it documents which settings exist.
- Code calls `load_dotenv(override=False)`: real environment variables win over
  the file.

**Alternatives.**

- *`export` in the shell profile:* works, but the key applies to every program
  you run and isn't tied to this project.
- *A secrets manager (AWS Secrets Manager, Vault):* the right choice in
  production, but too much infrastructure for local development.
- *A config file such as YAML:* mixes secrets with settings that should be
  committed.

**Consequences.** Local runs need no setup beyond `cp .env.example .env`.
Because environment variables take precedence, Airflow, Docker or CI can inject
secrets directly later with no code changes (12-factor style). This adds one
dependency, `python-dotenv`.

---

## ADR-010: Gzip-compress raw responses

**Status:** Accepted, 2026-11-11. Amends ADR-002 (file name and encoding only).

**Context.** Discovery (ADR-007) found 224 new boards, which would take daily
ingestion from about 2,900 to about 28,000 postings. Uncompressed raw JSON was
about 34 MB a day for 10 companies, so it would be roughly 300 MB+ a day
(around 9 GB a month) at 234 companies. Job descriptions are repetitive
HTML/text, which compresses very well. Converting history later is a
backfill, so the format should be settled before data piles up.

**Decision.** Store each response as `response.json.gz` (gzip, level 6) in the
same partition layout. `_metadata.json` stays uncompressed and records
`compression`, `bytes` (uncompressed size), `bytes_compressed`, and `sha256`
of the **uncompressed** response.

- gzip is lossless, so the stored bytes are still exactly what the API
  returned. ADR-002's "raw, unchanged" guarantee still holds.
- `mtime=0` in the gzip header makes compression deterministic: the same
  response always produces identical bytes.
- The checksum covers the uncompressed content, so it identifies the data
  regardless of how it's stored.

**Measured** (2026-11-11, 10 companies, 2,909 postings): 34.9 MB → 3.6 MB,
**9.6× smaller**. DuckDB's `read_json` reads the `.gz` files directly with
Hive partition columns, and its posting counts matched the ingest log exactly.

**Alternatives.**

- *zstd:* compresses better and faster, and DuckDB, Athena and Spark support
  it, but some tools (older Glue crawlers, `zcat`, many viewers) don't.
  gzip is supported everywhere, and at this data size the difference doesn't
  matter.
- *Convert to Parquet at ingestion:* much smaller and faster to query, but
  it's a *transformation*: it requires choosing a schema up front, and the
  three providers' schemas differ and change. Parquet belongs in the
  silver/staging layer, not raw.
- *Skip unchanged boards* (compare against yesterday's sha256): saves even
  more, but means a missing file can mean either "unchanged" or "fetch
  failed," which complicates SCD2 and close detection. Could be revisited
  later as its own decision.
- *Stay uncompressed:* simplest, but about 9 GB a month, and S3 storage and
  Athena scan costs grow with every byte.

**Consequences.** About 10× less storage, and Athena scans compressed bytes,
which lowers query cost too. Files are no longer readable in a text editor:
use `zcat response.json.gz | head` or `LocalRawStore.read()`. gzip can't be
split for parallel reads within one file, which doesn't matter here because
each file is one company's response (a few MB at most). Note for Stage 2:
Lever responses are a bare JSON array while Greenhouse and Ashby are objects
with a `jobs` key, so staging reads them with different `read_json` settings.

---

## Open questions

Decisions still to make. Each becomes an ADR once decided.

1. **How dbt reads raw data:** read the JSON files directly, or load them into
   DuckDB tables first?
2. **SCD Type 2:** use dbt's built-in `snapshot`, or write the logic by hand
   in SQL?
3. **When a posting counts as closed:** the first day it's missing, or after N
   consecutive missing days (so a failed fetch doesn't look like every job
   closed)?
4. **Board health:** automatically set `active=false` after N consecutive
   failed or empty fetches?
5. **Profile:** which criteria are hard filters and which are scores (ADR-006)?
