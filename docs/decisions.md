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
| [011](#adr-011-dbt-reads-raw-files-directly-schema-on-read) | dbt reads raw files directly (schema on read) | Accepted | 2026-11-16 |
| [012](#adr-012-convert-description-html-to-text-in-staging-sql) | Convert description HTML to text in staging SQL | Accepted | 2026-11-16 |
| [013](#adr-013-hand-written-scd-type-2-rebuilt-from-raw-on-every-run) | Hand-written SCD Type 2, rebuilt from raw on every run | Accepted | 2026-11-16 |
| [014](#adr-014-a-posting-closes-when-missing-from-a-successful-fetch) | A posting closes when missing from a successful fetch | Accepted | 2026-11-16 |
| [015](#adr-015-extract-skills-with-a-regex-dictionary-in-dbt) | Extract skills with a regex dictionary in dbt | Partly superseded by ADR-020 | 2026-11-19 |
| [016](#adr-016-tag-mentions-by-section-required-preferred-or-unspecified) | Tag mentions by section: required, preferred, or unspecified | Accepted | 2026-11-19 |
| [017](#adr-017-keep-every-years-of-experience-mention-summarize-as-min-and-max) | Keep every years-of-experience mention; summarize as min and max | Accepted | 2026-11-19 |
| [018](#adr-018-seniority-from-title-keywords-then-years) | Seniority from title keywords, then years | Accepted | 2026-11-19 |
| [019](#adr-019-audit-the-skill-dictionary-with-claude-haiku-on-a-fixed-sample) | Audit the skill dictionary with Claude Haiku on a fixed sample | Accepted | 2026-11-19 |
| [020](#adr-020-dictionary-changes-from-the-first-llm-audit) | Dictionary changes from the first LLM audit | Accepted | 2026-11-19 |
| [021](#adr-021-cap-duckdb-threads-and-memory-for-dbt-builds) | Cap DuckDB threads and memory for dbt builds | Accepted | 2026-11-25 |
| [022](#adr-022-your-profile-lives-in-dbt-seeds) | Your profile lives in dbt seeds | Accepted | 2026-11-25 |
| [023](#adr-023-hard-filters-job-function-and-location-only) | Hard filters: job function and location only | Remote rule amended by ADR-026 | 2026-11-25 |
| [024](#adr-024-score-is-a-weighted-sum-of-four-0-1-parts) | Score is a weighted sum of four 0–1 parts | Accepted | 2026-11-25 |
| [025](#adr-025-freshness-counts-from-the-day-we-first-saw-a-posting) | Freshness counts from the day we first saw a posting | Accepted | 2026-11-25 |
| [026](#adr-026-check-remote-locations-option-by-option) | Check remote locations option by option | Accepted | 2026-11-25 |
| [027](#adr-027-lean-airflow-3-in-docker-pipeline-in-its-own-virtualenv) | Lean Airflow 3 in Docker, pipeline in its own virtualenv | Accepted | 2026-11-25 |
| [028](#adr-028-dag-design-no-catch-up-keep-building-after-partial-failures-one-pool) | DAG design: no catch-up, keep building after partial failures, one pool | Accepted | 2026-11-25 |
| [029](#adr-029-a-dated-report-file-per-run) | A dated report file per run | Accepted | 2026-11-25 |
| [030](#adr-030-copy-the-raw-layer-to-s3-in-a-separate-upload-task) | Copy the raw layer to S3 in a separate upload task | Accepted | 2026-11-25 |
| [031](#adr-031-dbt-reads-local-raw-files-by-default-s3-by-switch) | dbt reads local raw files by default, S3 by switch | Accepted | 2026-11-25 |
| [032](#adr-032-the-pipeline-has-its-own-least-privilege-iam-user) | The pipeline has its own least-privilege IAM user | Scope extended by ADR-037 | 2026-11-25 |
| [033](#adr-033-terraform-state-in-s3-created-by-a-bootstrap-stack) | Terraform state in S3, created by a bootstrap stack | Accepted | 2026-11-25 |
| [034](#adr-034-athena-queries-curated-parquet-exported-from-the-dbt-warehouse) | Athena queries curated Parquet exported from the dbt warehouse | Accepted | 2026-11-27 |
| [035](#adr-035-glue-tables-in-terraform-columns-from-a-generated-schema-file) | Glue tables in Terraform, columns from a generated schema file | Accepted | 2026-11-27 |
| [036](#adr-036-partition-projection-for-the-dt-partitions) | Partition projection for the dt partitions | Accepted | 2026-11-27 |
| [037](#adr-037-athena-workgroup-with-a-scan-cap-pipeline-may-also-write-curated) | Athena workgroup with a scan cap; pipeline may also write curated/ | Accepted | 2026-11-27 |

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

## ADR-011: dbt reads raw files directly (schema on read)

**Status:** Accepted, 2026-11-16. Resolves open question 1.

**Context.** Stage 2 needs the raw `.json.gz` files (ADR-002, ADR-010) inside
dbt. The three providers have different shapes (Lever is a bare array;
Greenhouse and Ashby wrap postings in `{"jobs": [...]}`) and their fields
drift over time. Data is small: about 3.6 MB of gzip a day for 10 boards.

**Decision.** Staging models read the files themselves through the
`read_raw()` macro, which wraps DuckDB's `read_json_objects` with
`hive_partitioning=true`. There is no separate load step and no second copy
of the data.

- **Schema on read:** `read_json_objects` returns each value as untyped JSON
  instead of inferring a schema. Each staging model pulls out exactly the
  fields it needs with explicit casts. A new or missing field in one file
  can't break type inference for the rest.
- `format='array'` for Lever (one row per posting), `'auto'` for the others
  (one row per file, then `unnest` the `jobs` array).
- Staging models are **views**, so they always reflect the files on disk.
  The SCD2 model is a **table**, so the files are scanned once per build.
- Fetch metadata (`_metadata.json`) becomes `stg_raw__fetches`, which tells
  SCD2 which days each board was fetched successfully (ADR-014).
- Run dbt from the repo root (`--project-dir dbt --profiles-dir dbt`) so
  `data/raw` resolves the same way it does for the ingest job. The raw path is
  the dbt var `raw_root`; the DuckDB file is `JOBPIPE_DUCKDB_PATH` (default
  `data/warehouse.duckdb`). Tests override both.

**Alternatives.**

- *Python load step into DuckDB tables, then dbt:* the classic "EL then T"
  pattern and faster queries, but a second copy of the data, plus its own
  idempotency logic (delete and reinsert per day).
- *dbt incremental model as a bronze table:* only reads new partitions, but
  harder to reason about, and needs `--full-refresh` whenever logic changes.
- *`read_json` with schema inference:* less SQL, but inference over hundreds
  of files with drifting fields is fragile and can pick the wrong types.
- *dbt `sources` with dbt-duckdb `external_location`:* gives source lineage in
  dbt docs, but each provider needs different read options. Could be added
  later for documentation.

**Consequences.** `dbt build` alone rebuilds everything from raw. Every build
re-reads every file, which is fine at MBs but grows with history. Revisit
(incremental bronze, or Parquet in staging) when a build takes minutes. A
provider with **no files at all** makes its glob fail ("No files found"), so a
fresh setup needs at least one board per provider.

---

## ADR-012: Convert description HTML to text in staging SQL

**Status:** Accepted, 2026-11-16

**Context.** Skill and experience extraction (Stage 3) needs plain text.
Greenhouse sends HTML that is itself HTML-escaped (`&lt;p&gt;Tom &amp;amp;
Jerry`). Lever's `descriptionPlain` **leaves out** the `lists` array, which
holds requirements and responsibilities, the most important text for skill
matching. Ashby provides a usable `descriptionPlain`.

**Decision.** Staging produces one `description_text` column for every
provider, built with dbt macros in `dbt/macros/html_to_text.sql`:

- `html_to_text(expr, double_escaped)`: undo the outer escaping (Greenhouse
  only), turn block-level closing tags and `<br>` into newlines, strip tags,
  decode common entities (`&amp;` always last), normalize whitespace.
- `normalize_whitespace(expr)`: collapse spaces (including non-breaking),
  collapse blank lines, trim, and turn empty strings into null.
- Greenhouse: `html_to_text(content, double_escaped=true)`.
- Lever: `descriptionPlain` + each list's heading and cleaned HTML content +
  `additionalPlain`.
- Ashby: `descriptionPlain`, falling back to cleaning `descriptionHtml`.

A warn-level test flags descriptions where entities or tags survive cleaning.
A missing description is a warning, not a failure: Lever publishes some
postings with no text at all.

**Alternatives.**

- *Python HTML parser (step or DuckDB UDF):* more accurate, but staging would
  then depend on Python code outside dbt.
- *Keep HTML, clean in Stage 3:* thinner staging, but every consumer would
  have to deal with HTML.

**Consequences.** All text cleaning is in SQL, visible in dbt lineage, and
tested (`tests/test_dbt_scd2.py` checks exact output for each provider).
It's best-effort, not a full parser: rare numeric entities can survive (the
warn test reports them; currently 0). Measured on 2026-11-16: 0 leftover
entities or tags across 5,837 posting-days.

---

## ADR-013: Hand-written SCD Type 2, rebuilt from raw on every run

**Status:** Accepted, 2026-11-16. Resolves open question 2.

**Context.** We want to know when each posting opened, changed and closed.
Each daily raw partition is already a **full snapshot** of every board.

**Decision.** `int_postings__scd2` (a table) is built with window functions
from all raw partitions on every run:

1. Hash the tracked attributes per posting per day (`attributes_hash`).
2. Build a timeline: one row per posting per **successful** fetch of its
   board since the posting was first seen; the hash, or `CLOSED` if absent.
3. `LAG` finds the days where the state changes; each change starts a version.
4. `LEAD` gives each version its `valid_to` (exclusive) and `end_reason`
   (`changed` or `closed`). `CLOSED` stretches are dropped: they're gaps.
   A posting that comes back gets a new version.

Tracked attributes: title, department, team, location_raw, locations_raw,
workplace_type, employment_type, description_text. **Not** tracked:
Greenhouse's `updated_at` (it changes without visible edits) and
`published_at`.

List columns are **sorted** before hashing. `list_distinct` doesn't keep
order, and an order change would show up as a false "change".

Tests: a structural test (no overlaps, at most one current version,
`changed` versions are contiguous, `end_reason` null exactly when current)
and an end-to-end pytest scenario with an edit, a failed fetch, a close, a
reopen and an empty board.

**Alternatives.**

- *dbt `snapshot`:* very little code, but it's **stateful**. It only records
  what it sees at each run, can't be rebuilt or backfilled from past raw
  partitions, and losing the snapshot table loses the history. That breaks
  ADR-002's "rebuild anything from raw" promise.
- *Hand-written incremental (merge new days only):* scales better, but needs
  careful handling of late and re-run days. The natural next step at scale.

**Consequences.** History is reproducible from raw, and fixing a bug means
re-running `dbt build`. Cost grows with postings × fetch days (the timeline
is a cross join): about 10M rows at 28k postings over a year, which is fine
for DuckDB. Revisit with an incremental version when builds slow down. First
real run (Nov 11 → 16): 82 `changed` versions (genuine retitles, location
fixes and description edits) and 109 closures.

---

## ADR-014: A posting closes when missing from a successful fetch

**Status:** Accepted, 2026-11-16. Resolves open question 3.

**Context.** The APIs only list open postings, so "closed" has to be inferred
from absence. But a posting can also be absent because the fetch failed.

**Decision.** The ingest job only writes a partition (and `_metadata.json`)
for a successful fetch. SCD2 only considers days with a successful fetch of
that posting's board (`stg_raw__fetches`, `http_status = 200`). A day with no
successful fetch is **unknown** and skipped. A posting closes on the first
day it's missing from a successful fetch of its board. A board that returns
an empty list (`[]`) successfully closes all of its postings.

**Alternatives.**

- *Closed after N consecutive missing successful fetches:* also guards
  against an API briefly returning a partial list, but reports closures
  N−1 days late and merges real close-and-reopen events.
- *First calendar day missing:* one failed fetch would close and then reopen
  every posting at that board.

**Consequences.** A failed fetch can never look like a mass closure. A
*partial* successful response (an API bug) would still create false
closures; the reconciliation test only checks staging against the fetched
count, not against yesterday. If that happens, add an anomaly check such as
"board lost more than 50% of postings in a day." A board that stops being
fetched (`active=false`) keeps its postings current forever, which ties into
open question 4 (board health).

---

## ADR-015: Extract skills with a regex dictionary in dbt

**Status:** Accepted, 2026-11-19. The "skip the employer's own product" rule
is superseded by ADR-020.

**Context.** Scoring (Stage 4) needs to know which skills each posting asks
for. Descriptions are free text, and the same skill has many spellings
(Postgres / PostgreSQL, k8s / Kubernetes). Some names are ambiguous words
(Go, R, Excel, Spark).

**Decision.** A dbt seed, `dbt/seeds/skills.csv`, lists each skill with a
category and an RE2 regex. `int_posting_skills` matches the patterns against
every lower-cased line of the title and description, producing one row per
(posting version, skill) with an example of the matched text.

- Word boundaries (`\b`) everywhere. Ambiguous names need *list context*:
  Go only in `golang`, `go,`, `(go`, `in go`; R only next to a comma or slash
  or beside Python/SQL/Scala; Excel only with a tool cue
  ("Microsoft Excel", "Excel spreadsheets").
- Mentions in `other` sections (about us, benefits, EEO) are ignored, and so
  is the employer's own product (Databricks at Databricks).
- Features are computed per **posting version** (from SCD2), so each
  description is processed once and skill history comes for free.
- Patterns are read from the seed **at compile time** (`run_query`) and
  inlined as constants. Joining to the seed and calling
  `regexp_matches(line, s.pattern)` recompiled a regex for every
  (line, skill) pair, about 12M times: 51.5s. Constants: 2.7s, with
  identical output (0 differing rows out of 10,111).

**Findings while tuning** (2026-11-19 data, checked by sampling matches):

| Problem | Example | Fix |
|---|---|---|
| Employer boilerplate | "lakehouse" in 898 Databricks postings | removed `lakehouse`; skip employer's own skill |
| Common word | EEO line "everyone can **excel**" in 998 postings | require a tool context |
| Ambiguous word | "production/**go** live" | removed the `/go` form |

**Alternatives.**

- *LLM extraction:* handles context and finds unlisted skills, but costs
  money per posting, isn't deterministic, needs a Python step outside dbt,
  and is hard to test. Will be used to audit the dictionary instead.
- *Join on `s.pattern` at runtime:* simpler SQL, but 20× slower (see above).

**Consequences.** Free, deterministic, explainable (`example_match` shows why
a skill was found), and testable. It only finds skills that are in the
dictionary, so coverage depends on maintaining the seed. 61% of current
postings have at least one skill; most of the rest are non-technical roles
(sales, legal, operations). Adding a skill is one CSV row plus `dbt build`.

---

## ADR-016: Tag mentions by section: required, preferred, or unspecified

**Status:** Accepted, 2026-11-19

**Context.** Postings separate "Minimum requirements" from "Nice to have".
Treating "Kafka is a plus" like "Kafka required" would distort scoring, and
the company's tech blurb ("we use Snowflake") isn't a requirement at all.

**Decision.** `int_posting_lines` splits each version into lines (Stage 2
kept line breaks; line 0 is the title) and detects **headings**: lines of at
most 5 words (or 10 ending in `:`) that match a section pattern. Every line
after a heading belongs to that section until the next one. Sections:
`title`, `intro` (before any heading), `required`, `preferred`,
`responsibilities`, `other`. Patterns are checked in that order of priority,
since "Preferred qualifications" also contains "qualification".

Each line's `requirement_level`:

- `preferred` if the line itself has a cue ("is a plus", "preferred",
  "ideally"), even inside a required section
- `required` for the title and the required section
- `preferred` for the preferred section
- `unspecified` otherwise (intro, responsibilities)

A skill mentioned at several levels takes the strongest one.

**Findings.** Generic wording first covered only 54% of Greenhouse
postings: Databricks ("What we look for", "The impact you will have") and
Airbnb ("Your expertise", "A typical day") use house headings. After adding
them: **99% Greenhouse, 99% Lever, 91% Ashby** have a recognized required
section. The end-to-end test caught a bug: the bullet "Experience with
Airflow" under *Nice to have* was taken for a bare "Experience" heading;
bare-word headings must now be the whole line.

**Alternatives.**

- *Any mention counts:* simpler, but "a plus" weighs the same as "required",
  and company blurbs count as requirements.
- *LLM classification of sections:* more robust wording, but the same cost
  and testing issues as ADR-015.

**Consequences.** Scoring can weigh preferred skills less. It's heuristic: a
company with unusual headings falls back to `intro`/`unspecified`. A
warn-level test flags any provider whose required-section coverage drops
below 80%, which usually means a new heading style to add.

---

## ADR-017: Keep every years-of-experience mention; summarize as min and max

**Status:** Accepted, 2026-11-19

**Context.** Postings often give several figures ("5+ years overall, 3+
with Spark", "3-5 years"), and numbers with "years" also appear in company
trivia ("founded 20 years ago", "for over 15 years").

**Decision.** `int_posting_experience` keeps **every** mention (one row each,
with the matched text), and `int_posting_features` summarizes them as
`years_min` (lowest lower bound) and `years_max` (highest upper bound).

- Matches `3+ years`, `3-5 years`, `three (3) years`, `5 or more years`,
  `4 yrs`. Context before/after excludes digits, so "3+ years of Python and
  5+ years of SQL" yields two mentions.
- Kept if: not in an `other` section; and either in a required/preferred
  section, or the context mentions experience, or the number is followed by
  "of/in/with/working/building…". Rejected: "ago", "founded", "of service",
  "for over N years", "the last N years", "N years in business." Capped at 25.

**Findings.** Sampling rejected lines showed that a narrow context rule
dropped real requirements ("8+ years designing…", "for 10+ years"), so the
section is trusted instead. The reject "in business" matched "degree in
business administration", so it now only fires at the end of a phrase. 77%
of current postings have a years figure; values range from 0 to 18.

**Alternatives.**

- *Largest number:* conservative, but sensitive to the noise above.
- *First match:* depends on how the posting is ordered.

**Consequences.** Scoring can use `years_min` (the most lenient reading), and
any surprising value can be traced to its source text. Skill-specific
years ("3+ years with Spark") count as overall years; tying years
to skills could be a later refinement.

---

## ADR-018: Seniority from title keywords, then years

**Status:** Accepted, 2026-11-19

**Context.** Scoring should favor mid-level roles and penalize staff and
principal ones. Titles are the clearest signal but often have no level
("Data Engineer").

**Decision.** Title keywords are checked from the top of the ladder down,
and the first match wins: intern, executive (VP, head of, chief), director,
manager, principal, staff (and "IV"), lead, senior (and "III"), mid ("II"),
entry (junior, associate, new grad, "I"). If the title has no keyword,
`years_min` decides: 0–2 entry, 3–5 mid, 6+ senior. `seniority_source`
(`title`, `years`, `none`) records which rule decided.

"Manager" only counts as management for people-management titles: titles
that start with (Senior) Manager, or name a team (Engineering Manager).
Product/Program/Account Manager are individual-contributor roles.

**Alternatives.**

- *Title only:* most precise, but many postings would be `unknown`.
- *Also scan the description:* phrases like "mentor senior engineers"
  describe the team as often as the role, so they're noisy.

**Consequences.** 89% of current postings get a level (rest `unknown`).
"Highest keyword wins" means "Senior / Staff Engineer" counts as staff,
which is conservative for a mid-level search. "Lead" is its own level
because it means different things at different companies; scoring decides
how to treat it.

---

## ADR-019: Audit the skill dictionary with Claude Haiku on a fixed sample

**Status:** Accepted, 2026-11-19

**Context.** The dictionary (ADR-015) only finds skills it lists, and its
regexes can match the wrong thing. Sampling matches by hand found real
problems (Excel, lakehouse), but only for skills we already thought to check.
We want a measured view of what it misses, without putting an LLM in the
daily pipeline.

**Decision.** A one-off script, `python -m jobpipe.audit_skills`:

- **Sample:** 100 current postings: 60 data-tech titles, 25 other
  engineering, 15 non-tech (where false matches like Excel show up).
  Ordered by `hash(key || seed)`, so the same seed always gives the same
  sample.
- **Model:** `claude-haiku-5-5`. Listing skills is plain extraction, which a
  small model does well. $0.10 / $0.50 per million input / output tokens:
  about $0.05 for the whole sample.
- **Structured output:** a forced tool call (`tool_choice`) with a JSON
  schema (`name`, `requirement_level`, `evidence`), so every answer parses.
  The prompt lists the seed's skill names as canonical spellings, so the two
  answers can be compared name by name.
- **Comparison:** both found it (and do levels agree?), dictionary only
  (possible false match), LLM only for a seed skill (possible missed match),
  LLM only for an unlisted skill (candidate new row).
- **Output:** a Markdown report in `data/audit/` with the evidence for every
  disagreement. **A person edits the seed**; the LLM never changes the
  pipeline. LLM responses aren't stored, so re-checking means another
  (cheap) run.
- **Spending guard:** `--estimate` counts tokens with the free
  token-counting endpoint and prints the cost without calling the model; a
  real run asks for confirmation unless `--yes`.
- The key is `ANTHROPIC_API_KEY` in `.env` (ADR-009).

**Alternatives.**

- *LLM extraction in the pipeline:* see ADR-015.
- *Larger or random sample:* a random 50 would be mostly sales and ops roles;
  300 costs 3× for slightly tighter estimates.
- *Bigger model (Sonnet, Opus):* more careful on ambiguous cases, at a
  higher price. A second run with another model would show how much the
  audit itself depends on the model.
- *Store the LLM responses:* re-comparing after a seed change would be free,
  but the user preferred keeping only the report.
- *Let the LLM write new regexes:* faster, but the patterns still need
  checking, and reviewing them is part of the learning.

**Consequences.** The precision and recall numbers are measured *against the
LLM*, which is a second opinion, not ground truth; the report says so and
shows evidence for every disagreement. The audit can be re-run after seed
changes, or on a new sample (`--seed`), to see whether things improved.

---

## ADR-020: Dictionary changes from the first LLM audit

**Status:** Accepted, 2026-11-19. Supersedes ADR-015's rule that the
employer's own product is never counted.

**Context.** The first audit (ADR-019; `claude-haiku-5-5`, seed 42, 100
postings, $0.055) measured the dictionary against the LLM:

| Measure | Result |
|---|---|
| Precision vs LLM | 96% (471 agreed, 20 dictionary only) |
| Recall vs LLM | 72% (182 LLM only, for skills in the seed) |
| Requirement level agreement | 89% |

Reading the evidence behind the disagreements:

- **Real misses:** the employer's own product when it's a stated
  requirement (18 cases: "Experience working with Databricks" at
  Databricks), and missing spellings: "data models", "Structured
  Streaming", "version control", "GenAI" / "RAG".
- **LLM over-reach:** many LLM-only hits for concept skills were stretches
  ("Data quality" from "partner with data engineers", "Streaming" from
  "imitation learning", "REST APIs" from any "APIs"). Recall against the
  LLM understates the dictionary for concepts.
- **Missing skills:** a long tail of 1–2 posting tools. Nothing central to
  data engineering was missing.
- **Dictionary-only:** mostly correct (the LLM missed PyTorch in an ML
  training role). Azure specialist postings list AWS and Snowflake as
  *competitors*; left as is.

**Decision.**

1. **Employer's product counts only as a stated skill:** in a required or
   preferred section *and* on a line with a usage cue (experience,
   proficient, understanding of, knowledge of, familiar, expertise,
   hands-on, certif, working with, skilled).
2. **Spellings:** Data modeling + `data models?`; Streaming + `structured
   streaming`, `kafka streams`; Git + `version control`; LLMs + `gen ?ai`,
   `generative ai`, `rag`.
3. **New skills:** MLflow, JAX, Kotlin, and React. React is also a verb
   ("react to incidents"), so it requires a tech or list context.

**Alternatives.**

- *Count every mention of the employer's product:* tried first. Databricks
  went from 40 to 1,055 rows, mostly the company name ("reporting to
  Databricks' senior director", "account executive at Databricks"). It
  would have become the most-demanded skill and marked the user down on
  every Databricks posting.
- *Keep skipping it:* simple, but drops real requirements and certifications.
- *Competitor rule ("competitive platforms"):* considered and not taken;
  it's narrow and only affected solutions-architect postings.
- *Add the long tail of niche tools:* one posting each, not relevant to
  the target roles.

**Consequences.** Skill rows went from 10,111 to 10,855, and postings with
at least one skill from 1,797 to 1,849 (of 2,930 current). Per skill:

| Skill | Before | After |
|---|---|---|
| Databricks | 40 | 374 |
| React | 0 | 127 |
| LLMs | 296 | 399 |
| Data modeling | 44 | 107 |
| MLflow | 0 | 52 |
| Kotlin | 0 | 36 |
| Git | 139 | 153 |
| JAX | 0 | 9 |
| Streaming | 30 | 36 |

Remaining edge case: "prior experience **at** Databricks, Snowflake…"
counts those as skills (employer history, not tools). Re-running the audit
(another ~$0.06) would measure the change; with responses not stored
(ADR-019), that's the only way to re-check.

---

## ADR-021: Cap DuckDB threads and memory for dbt builds

**Status:** Accepted, 2026-11-25. Amends ADR-011 (the `max_json_bytes` value).

**Context.** `dbt build` ran out of memory on the development laptop (WSL2:
16 cores, about 7 GB of RAM) with only one day of raw data for 10 boards.
DuckDB defaults to one thread per core and a memory limit of 80% of RAM. Each
thread reading JSON reserves a buffer of about 2 × `maximum_object_size`, so
ADR-011's 256 MB setting needs about 16 × 2 × 256 MB ≈ 8 GB before any data
is processed. Memory use depends on core count, not data size.

**Decision.**

- `max_json_bytes` lowered from 256 MB to **64 MB**. The largest response
  seen so far is about 14 MB uncompressed (Databricks is 9.7 MB), so this
  leaves about 4.5× headroom.
- DuckDB `threads` defaults to **4** and `memory_limit` to **3GB**, set in
  `dbt/profiles.yml` and overridable with `JOBPIPE_DUCKDB_THREADS` and
  `JOBPIPE_DUCKDB_MEMORY` (documented in `.env.example`; not secrets).
- dbt's own `threads` (models built in parallel) is lowered from 4 to **2**,
  since each concurrent model runs its own DuckDB query.

**Measured** (2026-11-25, same data, same machine):

| Settings | Result |
|---|---|
| ADR-011 defaults (256 MB, no DuckDB limits) | Out of Memory: 7.1 GiB used of 5.8 GiB |
| This ADR | 61 nodes pass (1 expected warning), 21 s, 1.1 GB peak |

**Alternatives.**

- *Only set `memory_limit`:* DuckDB would still reserve buffers per thread,
  so it fails at the limit instead of at the OS.
- *Only lower `max_json_bytes`:* helps, but a laptop with more cores would hit
  the same wall again. Capping threads makes memory use predictable.
- *Give WSL more memory (`.wslconfig`):* treats the symptom, works on one
  machine only, and leaves the pipeline fragile on small CI runners or
  containers.

**Consequences.** Builds use a predictable, bounded amount of memory on any
machine, at the cost of some parallelism (unnoticeable at this data size).
A single board response larger than 64 MB fails with a clear DuckDB error
("maximum_object_size"); raise `max_json_bytes` if that happens. On a larger
machine or in Airflow/Docker (Stage 5), set the two environment variables
instead of changing code.

---

## ADR-022: Your profile lives in dbt seeds

**Status:** Accepted, 2026-11-25

**Context.** Scoring (Stage 4) compares every posting with a personal
profile: skills, years of experience, preferred levels, title and location
rules, and score weights. ADR-006 says ranking happens in the final mart.

**Decision.** The profile is five dbt seeds in `dbt/seeds/`:

| Seed | Contents |
|---|---|
| `profile_skills.csv` | Skills you have, weight 3 (core), 2 (strong) or 1 |
| `profile_settings.csv` | One row: your years, score weights, penalties, half-life |
| `profile_level_fit.csv` | 0–1 fit for every seniority level Stage 3 produces |
| `profile_titles.csv` | Include/exclude regexes for the job-function filter |
| `profile_locations.csv` | Allowed places and remote regions |

Edit a CSV and run `dbt build` to re-rank. Seeds are tested like models:
every profile skill must exist in the skill dictionary, every seniority level
needs a fit (`level_score` is `not_null`), and `profile_settings` must be
exactly one row with valid values. Regex patterns are read at compile time and
written into the SQL as constants (the Stage 3 performance trick, ADR-015).

**Alternatives.**

- *YAML file passed as dbt vars:* easier to read, but SQL only sees it
  through Jinja: you can't join it, and dbt can't test it.
- *A Python scoring step:* any logic is easy, but scoring would leave dbt
  (no lineage, no dbt tests), against ADR-006.

**Consequences.** The profile is version-controlled and reviewable, and a
profile change re-ranks without code changes. CSV quirk: a pattern containing
a comma must be quoted (`"..., il\b"`). The profile is personal data in a
repo; it holds only skills and preferences, never contact details.

---

## ADR-023: Hard filters: job function and location only

**Status:** Accepted, 2026-11-25. Resolves the "Profile" open question
together with ADR-024.

**Context.** ADR-006 allows hard filters only for true "no" criteria. Of 2,923
current postings, about 1.5% are data roles, and most are in places you can't
work.

**Decision.** Two hard filters, computed as flags in `int_posting_scores` and
applied only in `mart_top_matches`:

- **Job function** (`passes_title`): the lower-cased title matches an
  `include` pattern (Data Engineer, Analytics Engineer, Data Platform / Infra,
  Software Engineer with data/pipeline/streaming in the title) and no
  `exclude` pattern (solutions/sales roles, internships, people or product
  management).
- **Location** (`passes_location`): each entry in `locations_raw` is checked
  on its own. A posting passes if any entry matches an allowed **place**
  (Chicago, Illinois, `, IL`), or is remote scoped **only** to an allowed
  region (`Remote (US)`, `US-Remote`, `Remote in the US`, `North America`), or
  is a bare `Remote` / `workplace_type=remote` on a posting otherwise located
  in the US (`Remote | US`, `New York, NY`), or says **only** `Remote`.

"Remote scoped only to the region" means: remove the word "remote" and filler
words; what's left must be fully matched by a region pattern. So
`Remote - California` and `Remote (West Coast)` fail (they usually mean you
must live there), while `Remote (US or Canada)` fails too (`or canada` is
left over; noted as a known gap).

A posting that says only "Remote" passes and is labelled
`Remote (region unknown)`: unclear isn't a "no".

Seniority, skills and years are **not** filters; they're scored (ADR-024).

**Alternatives.**

- *Filter on seniority too (drop staff/director):* staff roles can still be
  worth seeing; a low level score pushes them down instead. Management titles
  are excluded as a *function*, not a level.
- *No filters, score only:* nothing hidden, but 98% of the list is noise.
- *Geocoding locations:* accurate, but an external service and much more
  work for 10–200 boards.

**Consequences.** On 2026-11-17 data: 43 titles pass, 316 locations pass,
2 postings pass both (the 10 starter companies hire few data roles in the
US). Checking real titles found one false exclusion ("Software Engineer, Sales
Data & Agents" matched `sales`); the pattern now only matches sales job
titles, and that title is a test case. Ask "why didn't X show up?" with
`int_posting_scores` (`title_includes`, `title_excludes`, `location_match`).

---

## ADR-024: Score is a weighted sum of four 0–1 parts

**Status:** Accepted, 2026-11-25

**Context.** The ranked list needs one number per posting that you can
explain ("82: strong skills, slightly over on years").

**Decision.** `score = 100 × Σ(weight × part) / Σ(weight)` with weights
50 / 25 / 15 / 10:

| Part | Rule |
|---|---|
| `skill_score` | Each skill the posting mentions has a *mass* = importance (required 1.0, preferred 0.5, unspecified 0.75) × your weight (1 if you don't have it). Score = matched mass / total mass. |
| `experience_score` | 1.0 if `years_min` ≤ your 4 years, minus 0.25 per extra year, floor 0. |
| `level_score` | From `profile_level_fit`: mid 1.0, senior 0.8, entry 0.7, unknown 0.6, lead 0.5, staff 0.3 … |
| `freshness_score` | ADR-025. |

A part that can't be computed (no skills or years found in the text) is null
in its column and counts as **0.5** (`unknown_part_score`) in the total, so
missing data neither helps nor sinks a posting. Each part is stored as its
own column, and `why` summarizes them in one line
(`skills 58% (missing Data modeling, …) · 5+ yrs (over by 1) · senior · first seen 0d ago`).

**Alternatives.**

- *Multiplicative:* one weak part sinks the score, and it's hard to tune.
- *Tiers (A/B/C):* readable, but no ordering inside a tier beyond date.

**Consequences.** Scores are easy to explain and tune by editing
`profile_settings.csv`. A weighted sum lets a great skill match make up for a
poor level fit, which is intended. Tests check two scores by hand-computed
arithmetic (87.9 and 56.5), so a scoring change shows up as a test diff.

---

## ADR-025: Freshness counts from the day we first saw a posting

**Status:** Accepted, 2026-11-25

**Context.** Provider dates mislead: companies repost and backdate, and a
Linear posting dated 2021 was still open in 2026.

**Decision.** `freshness_score = 0.5 ^ (days since first seen / 14)`. "First
seen" is the earliest `valid_from` of the posting in the SCD2 history
(ADR-013). "Today" is the **latest successful fetch date**, not the wall
clock, so re-running dbt on old data reproduces the same scores.

**Alternatives.**

- *Provider's posted date:* meaningful from day one, but easily faked.
- *The later of the two:* best of both, at the cost of another rule.

**Consequences.** Works the same for all three providers and can't be gamed.
Everything in the first ingest looks equally new (`first seen 0d ago`) until
a few weeks of daily history build up, so freshness carries only 10% of the
weight. It also means daily ingestion matters: a gap in fetching delays
"first seen" for postings that opened during the gap.

---

## ADR-026: Check remote locations option by option

**Status:** Accepted, 2026-11-25. Amends ADR-023's remote rule; resolves the
"Location gaps" open question.

**Context.** ADR-023 removed "remote" from a location entry and required
everything left to name an allowed region. But one entry often lists
**alternatives**: `Remote (US or Canada)`, `San Francisco, Seattle,
US-Remote`, `SF, NYC, Seattle, Remote` (with a separate `US` entry). All of
these failed, although each offers US-remote. Separately, `passes_location`
was **NULL** (not false) for postings with no `workplace_type`, because
`false or null` is `null` in SQL.

**Decision.**

1. Split each location entry into **items** on `,` `;` `/` `&` `|` and the
   words "or" / "and", **before** removing "remote".
2. Keep only items that contain "remote"; strip "remote" and filler words
   (in, the, only, based) from each to get its **scope**.
3. The entry passes the remote rule if **any** item's scope is entirely an
   allowed region (`us`, `u.s.`, `usa`, `united states`, `north america`), or
   if any item is a bare "Remote" and the posting is otherwise located in an
   allowed region (as before).
4. `passes_location` is wrapped in `coalesce(..., false)`, and both filter
   flags have `not_null` tests.

Splitting before stripping matters: `Remote (California, Washington, and
Oregon Only)` splits into `remote (california`, `washington`, `oregon only)`.
Only the first says remote, so its scope is `california` and it fails.
Stripping first would leave an empty item between ", and" that looks like a
bare "Remote".

**Alternatives.**

- *Only allow "X or Y":* fixes `Remote (US or Canada)` but not the city
  lists with `US-Remote` among them.
- *Pass any remote entry that mentions the US:* simplest, but
  `US-West Coast (Remote)` and `US - Remote (East Coast)` are
  region-restricted and would wrongly pass.

**Consequences.** On 2026-11-17 data, location passes went from 316 to 348
postings, with 0 NULL flags; every newly passing and still-failing entry was
reviewed by hand. Region-restricted remote (`Remote (West Coast)`,
`US - Remote (East Coast)`) still fails, which is intended. A comma inside a
single place name would split it ("Washington, D.C." becomes two items), but
that only affects items containing "remote", and the stripped scope still
fails. Eight real location strings are now test cases.

---

## ADR-027: Lean Airflow 3 in Docker, pipeline in its own virtualenv

**Status:** Accepted, 2026-11-25. First part of Stage 5 (5a: local
scheduling; S3/Terraform and Glue/Athena follow).

**Context.** The pipeline needs to run daily (ingest → dbt → report) and
weekly (discovery) without someone typing commands. The roadmap names Airflow
(MWAA in AWS). The development machine is WSL2 with about 7 GB of RAM, and
Airflow and dbt-core pin conflicting versions of shared libraries.

**Decision.**

- **Airflow 3.3.2** (the latest patch of the previous minor; 3.4.0 was three
  weeks old) in Docker Compose (`airflow/docker-compose.yaml`), cut down
  from the official file: **LocalExecutor + Postgres**, with services
  postgres, api-server, scheduler, dag-processor and a one-off init. No Redis,
  Celery worker, Flower, triggerer or example DAGs.
- **SimpleAuthManager with everyone as admin** (no login), and the UI port
  bound to `127.0.0.1` only. The JWT secret has a local default, overridable
  with `AIRFLOW_JWT_SECRET`.
- **The pipeline gets its own virtualenv in the image** (`/opt/jobpipe/venv`,
  from `requirements.txt`). Tasks are `BashOperator`s that call that
  interpreter: the same commands as in CLAUDE.md. Airflow's Python never
  imports pipeline code.
- **The repo is bind-mounted** at `/opt/jobpipe/project`, so code, profile
  seeds and dbt models change without rebuilding; only `requirements.txt`
  changes need `--build`. Containers run as the host user
  (`AIRFLOW_UID`, default 1000), so files in `data/` stay owned by you.
- `AIRFLOW__CORE__PARALLELISM=2` and ADR-021's DuckDB limits keep memory
  bounded.

**Alternatives.**

- *Official Compose (Celery, Redis, workers, Flower):* closest to the docs and
  to a multi-machine setup, but heavy for 7 GB and more moving parts than one
  machine needs.
- *Dagster:* lighter, with good dbt integration (models as assets), but the
  roadmap and most job postings name Airflow.
- *cron + script:* works, but no dependencies, retries, UI or run history.
- *A pipeline image with `DockerOperator`:* the cleanest isolation and close to
  `KubernetesPodOperator`, but Airflow needs the Docker socket (effectively
  root on the host). Worth revisiting on MWAA or Kubernetes.
- *Install the pipeline into Airflow's Python (`PythonOperator`):* least code,
  but dbt and Airflow dependency conflicts, and pipeline code tied to Airflow.

**Consequences.** One `docker compose up` starts everything; the UI shows run
history, task logs and retries. Airflow 3 does not reliably put the DAGs
folder on `sys.path`, so the Compose file sets `PYTHONPATH=/opt/airflow/dags`
for the shared `jobpipe_common.py` (excluded from parsing by `.airflowignore`).
The DAGs were verified with Airflow 3.3.2's `DagBag` in a throwaway venv
(`tests/test_dags.py`). Requires Docker Desktop's WSL integration. The laptop
must be on: a local scheduler can't run while it's asleep (ADR-028).

---

## ADR-028: DAG design: no catch-up, keep building after partial failures, one pool

**Status:** Accepted, 2026-11-25

**Context.** The two schedules (ADR-007) need concrete failure, timing and
concurrency rules, and the laptop isn't always on.

**Decision.**

| DAG | Schedule (America/Chicago) | Tasks |
|---|---|---|
| `jobpipe_daily` | 07:00 every day | `ingest` → `dbt_build` → `report` |
| `jobpipe_weekly_discovery` | Monday 06:00 | `discover` |

- **No catch-up** (`catchup=False`, `CronTriggerTimetable`). When Airflow
  starts after missing a slot, it runs only the **most recent** one, once.
  Ingestion can only see today's postings, so a run "for" a past day would
  store today's data under an old date. Missed days stay gaps, which SCD2
  treats as unknown (ADR-014). `ingest` is never given `--dt`.
- **`ingest` retries once** after 10 minutes (re-fetching is idempotent,
  ADR-002). It still exits 1 if any board failed, so the task shows red.
- **`dbt_build` runs even if `ingest` failed** (`trigger_rule=all_done`): the
  other boards' data is worth building, and dbt rebuilds from all raw files.
- **`report` only runs if `dbt_build` succeeded** (`all_success`): a failed
  data test stops a report from being published from bad data (a circuit
  breaker).
- **One-slot pool `jobpipe`** (created by `airflow-init`) for every task, plus
  `max_active_runs=1`. Discovery appends to `companies.csv` while ingestion
  reads it, and DuckDB allows only one writer; the pool keeps them apart even
  across DAGs.
- 30-minute `execution_timeout` per task.

**Alternatives.**

- *Catch up dbt only:* adds runs without adding information.
- *Stop the daily run on any ingest failure:* one bad board would block
  everyone's fresh data.
- *Separate schedules and hope they don't overlap:* works until discovery
  runs long; the pool makes it a guarantee.

**Consequences.** Failures are visible but contained. Discovery edits a
version-controlled file in the working tree: review it with
`git diff config/companies.csv`. Data written while DuckDB is open elsewhere
(a notebook holding the warehouse open for writing) makes `dbt_build` fail;
open it `read_only=True`.

---

## ADR-029: A dated report file per run

**Status:** Accepted, 2026-11-25

**Context.** The daily run should leave something to look at each morning,
without accounts, secrets or a dashboard server.

**Decision.** `python -m jobpipe.report` reads the warehouse **read-only** and
writes `data/reports/dt=<as_of>/top_matches.csv` and `top_matches.html`. The
date is the scoring "as of" date (latest fetch, ADR-025), not the wall clock,
so re-runs overwrite the same folder. The HTML page is self-contained (no
scripts or external assets), adapts to light and dark mode, flags postings
first seen that day as **new**, and escapes all job-board text. Links are
only rendered for `http(s)` URLs.

**Alternatives.**

- *Email or Slack notification:* nicer each morning, but credentials and an
  external service. Can be added as a task after `report` later.
- *No report:* query `mart_top_matches` yourself.

**Consequences.** One folder per day, so lists can be compared across days.
Reports are git-ignored with the rest of `data/`.

---

## ADR-030: Copy the raw layer to S3 in a separate upload task

**Status:** Accepted, 2026-11-25. First part of Stage 5b.

**Context.** The roadmap moves the raw layer to S3, so Glue and Athena can
query it (Stage 5c) and it survives the laptop. ADR-003 laid out local folders
exactly like S3 partitions so this move would only change the location.
Ingestion already works, writes atomically and is tested against local files.

**Decision.** Ingest stays unchanged and writes to `data/raw/`. A new module,
`python -m jobpipe.upload`, mirrors every raw file to
`s3://<bucket>/raw/<same relative path>`:

- **Only new or changed files.** One `ListObjectsV2` of the prefix gives each
  object's ETag. With single-part `put_object` and SSE-S3 the ETag is the MD5
  of the bytes, so comparing it with the local file's MD5 needs no downloads.
  A second run uploads nothing.
- **Integrity:** each PUT sends `Content-MD5`; S3 rejects a corrupted body.
- **Never deletes** in S3, so a local cleanup can't erase the lake.
- `Content-Type: application/gzip`, deliberately *not*
  `Content-Encoding: gzip`, which makes some clients unzip on download.
- **Exit 99 when `JOBPIPE_S3_BUCKET` isn't set**, which Airflow's
  BashOperator treats as *skipped*, so the DAG works before AWS is set up.
- In `jobpipe_daily`, `upload_raw` is a **side branch after ingest**
  (`ingest >> upload_raw`, `ingest >> dbt_build >> report`), with
  `trigger_rule=all_done` like dbt_build and one retry. dbt reads the local
  files (ADR-031), so an S3 problem never blocks the report.

Bucket settings (Terraform, ADR-033): us-east-2 (closest to Chicago), all
public access blocked, ACLs disabled, SSE-S3, HTTPS-only bucket policy,
versioning with noncurrent versions expiring after 30 days. No transition to
Standard-IA: it bills every object as at least 128 KB, and ours are a few KB.

**Alternatives.**

- *Ingest writes straight to S3:* S3 becomes the system of record, which is
  closer to production. But ingest gets a storage abstraction, its tests
  need an S3 fake, and nothing runs offline.
- *`aws s3 sync` in a BashOperator:* fewest lines, but needs the AWS CLI in
  the image and teaches nothing about the S3 API. It compares size and
  modification time, not content.
- *Upload before dbt_build:* then a network blip would block the report.

**Consequences.** Two copies of the raw layer; the local one is primary and
S3 lags by at most a day if an upload fails (the next run catches up, since
it compares everything). Switching the ETag check to SSE-KMS or multipart
uploads would break it: then store a checksum in object metadata instead.

---

## ADR-031: dbt reads local raw files by default, S3 by switch

**Status:** Accepted, 2026-11-25

**Context.** With the raw layer in S3, dbt could read either copy. DuckDB
reads `s3://` paths (httpfs) including Hive partitions and globs.

**Decision.** `raw_root` defaults to `data/raw` and can be switched with
`JOBPIPE_RAW_ROOT=s3://<bucket>/raw`. In S3 mode an `on-run-start` hook
(`macros/s3_secret.sql`) creates a DuckDB secret with the **credential chain
limited to environment variables**, so the key never appears in SQL, logs or
`target/`. In local mode the hook emits nothing: no extensions, no network.
`JOBPIPE_S3_ENDPOINT` optionally points at an S3-compatible server. The test
suite uses that to build from a moto server and checks that the result
matches a local build.

**Alternatives.**

- *Always read S3:* one source of truth, but every build lists and downloads
  every file, needs the network, and pays for requests.
- *Never read S3:* simplest, but the S3 copy would go untested until Athena.

**Consequences.** Daily builds stay fast and offline. Staging models are
views, so a warehouse built in S3 mode still points at `s3://`: query it
from a connection that has the secret. `chain 'env'`, not `'env;config'`,
because DuckDB rejects the whole secret when `~/.aws/config` has no
`[default]` profile.

---

## ADR-032: The pipeline has its own least-privilege IAM user

**Status:** Accepted, 2026-11-25. Scope extended to `curated/` by ADR-037.

**Context.** Airflow runs unattended at 07:00 in Docker and needs to write
to S3. The person running Terraform has broad admin rights; the pipeline
needs almost none.

**Decision.** Terraform creates an IAM user `jobpipe-pipeline` with one
managed policy: `s3:ListBucket` on the data bucket limited to the `raw/`
prefix, and `s3:GetObject` / `s3:PutObject` on `raw/*`. No delete, no other
buckets, no IAM. **Its access key is created by hand**
(`aws iam create-access-key`) and goes into the git-ignored `.env` (ADR-009),
because a key created by Terraform is stored in plain text in the state.

**Alternatives.**

- *Mount your own SSO profile into the containers:* no long-lived key, but
  SSO sessions expire within hours, so the 07:00 run would fail unless you'd
  logged in that morning, and it would run with admin rights.
- *A user that may only assume a role holding the S3 permissions:* the
  production pattern (short-lived STS credentials), but more Terraform, and
  it still needs a long-lived key for the user on a laptop.
- *`aws_iam_access_key` in Terraform:* one step fewer, secret in state.

**Consequences.** A long-lived key exists; its blast radius is the `raw/`
prefix of one bucket, and versioning (ADR-030) undoes overwrites. Rotate it
by creating a second key, updating `.env`, deleting the old one. Revisit when
this runs on AWS compute, where an instance or task role needs no key.

---

## ADR-033: Terraform state in S3, created by a bootstrap stack

**Status:** Accepted, 2026-11-25

**Context.** Terraform records what it created in a state file. Lose it and
Terraform no longer knows it owns the bucket and the user. The state also
lists resource details, so it shouldn't go into git.

**Decision.** Two stacks in `infra/terraform/`:

- `bootstrap/` creates `jobpipe-tfstate-<account>` (versioned, SSE-S3, no
  public access, old versions expire after 90 days, `prevent_destroy`) and
  keeps its **own** tiny state locally (git-ignored). If that file is lost,
  `terraform import` the bucket.
- `main/` (data bucket, IAM) uses the **S3 backend** in that bucket with
  **S3-native locking** (`use_lockfile = true`, Terraform 1.10+), so two
  applies can't run at once and no DynamoDB table is needed. The bucket
  name comes from a git-ignored `backend.hcl` that bootstrap generates,
  because backend blocks can't use variables.

Providers are pinned with `~> 6.0` and the committed `.terraform.lock.hcl`.

**Alternatives.**

- *Local state for everything:* simplest, but lost with the laptop, with no
  locking or history.
- *DynamoDB for locking:* the pre-1.10 standard, now deprecated in favor of
  S3-native locking.
- *HCP Terraform (Terraform Cloud):* hosted state and runs, but another
  account to set up, and it hides the mechanics this project is meant to
  teach.

**Consequences.** Running `main` needs credentials that can read the state
bucket. Every apply leaves a recoverable state version. Two commands come
first on a new machine: `terraform init -backend-config=backend.hcl`.

---

## ADR-034: Athena queries curated Parquet exported from the dbt warehouse

**Status:** Accepted, 2026-11-27. Stage 5c.

**Context.** The roadmap ends with Glue + Athena: SQL over the lake without
a running database. The raw layer can't be read well by Athena as it stands:
each file is one single-line JSON document of up to ~10 MB, Lever's is a
bare array (Athena's JSON SerDe reads one *object* per line), and
`_metadata.json` spans several lines. All the modeling already runs in
DuckDB/dbt and is tested there.

**Decision.** DuckDB keeps doing every transformation; Athena is a query
layer over its results. A new step, `python -m jobpipe.export`, reads the
warehouse read-only and writes one Parquet file per table and day:

    data/curated/<table>/dt=<as_of>/data.parquet   ->   s3://<bucket>/curated/<table>/dt=<as_of>/

| Athena table | dbt model |
|---|---|
| `postings_history` | `int_postings__scd2` |
| `posting_features` | `int_posting_features` |
| `posting_scores` | `int_posting_scores` |
| `top_matches` | `mart_top_matches` |

- **Daily snapshots**, `dt` = the scoring as-of date (latest fetch,
  ADR-025), so you can query how scores and the top list changed over time.
  A re-run overwrites the same day: PutObject only, no delete permission.
- Parquet with **zstd**, rows sorted (`order by all`), so the same data gives
  identical bytes and the upload skips unchanged files. Written to a `.tmp`
  file and renamed, like the raw layer.
- Upload reuses `jobpipe.upload` (ADR-030) with `--root data/curated --prefix curated`.
- In `jobpipe_daily`: `dbt_build >> [report, export_curated]`,
  `export_curated >> upload_curated`. The export has the report's circuit
  breaker: no snapshot from a build whose data tests failed.

**Alternatives.**

- *Glue tables over the raw JSON:* nothing new to write, but awkward SQL
  (unnest, `json_parse` for Lever), full multi-MB rows scanned per query,
  and it repeats work dbt already does.
- *Run dbt in Athena (dbt-athena, Iceberg):* the most cloud-native, but every
  model must be ported from DuckDB SQL to Trino SQL, each build costs money,
  and local development is lost. Worth revisiting if the data outgrows a laptop.
- *Latest-only tables:* smaller, but no day-to-day history of scores.

**Consequences.** About 3 MB of Parquet a day, mostly `postings_history`
(description text); a year is about 1 GB. Each `postings_history` snapshot
repeats the whole history, so it grows with the data: switch it to
latest-only if that ever matters. The raw layer stays unread by Athena.

---

## ADR-035: Glue tables in Terraform, columns from a generated schema file

**Status:** Accepted, 2026-11-27

**Context.** Athena reads Parquet through Glue Data Catalog tables, which
need explicit column names and types. Those must match what the export
writes, which is whatever the dbt models produce.

**Decision.** `aws_glue_catalog_table` resources in
`infra/terraform/main/glue.tf`, one per curated table, in a Glue database
`jobpipe`. Their columns come from `curated_schemas.json`, which
`python -m jobpipe.export --write-schemas` generates from the warehouse
(DuckDB → Glue type map; an unmapped type is an error, not a guess).
`tests/test_export.py` builds a small warehouse with dbt and fails if the
committed file doesn't match the models **or** the Parquet files'
own schema. Tables are `EXTERNAL`: dropping one never deletes data.

**Alternatives.**

- *Glue crawler:* less code, but inferred types can be wrong, tables can
  split unexpectedly, and each run costs DPU-hours.
- *`CREATE EXTERNAL TABLE` SQL files run through Athena:* readable SQL,
  but outside Terraform state, so drift goes unnoticed.
- *Hand-written columns in HCL:* 80+ lines to keep in sync by eye.

**Consequences.** Changing a model's columns is three steps: regenerate the
file, commit it, `terraform apply`. The test says so when it's forgotten.

---

## ADR-036: Partition projection for the dt partitions

**Status:** Accepted, 2026-11-27

**Context.** Each day adds a `dt=` folder per table. Athena only reads
partitions it knows about.

**Decision.** Partition projection: each table's properties say `dt` is a
date (`yyyy-MM-dd`, daily, from `curated_first_dt` to `NOW`) and where its
folder is (`storage.location.template`). Athena works out the partitions at
query time. `dt` is a `string` column, so filter with `dt = '2026-11-26'`.

**Alternatives.**

- *Register partitions in the DAG (`ALTER TABLE ADD PARTITION` / `MSCK
  REPAIR TABLE`):* teaches the metastore, but it's another task that, if
  skipped, silently hides new data. It would also need Athena and Glue
  permissions for the pipeline user.
- *Glue crawler on a schedule:* same, plus cost and lag.

**Consequences.** Nothing to keep in sync. A projected day with no folder
just returns no rows. Without a `dt` filter a query visits every day since
`curated_first_dt`, which is fine at this size (the workgroup's scan cap,
ADR-037, still applies).

---

## ADR-037: Athena workgroup with a scan cap; pipeline may also write curated/

**Status:** Accepted, 2026-11-27. Extends ADR-032.

**Context.** Athena bills per byte scanned and needs an S3 location for query
results. The pipeline now writes a second prefix.

**Decision.**

- Workgroup `jobpipe` (engine v3) with **enforced** settings: results go
  to `s3://<bucket>/athena-results/` (SSE-S3; a lifecycle rule deletes them
  after 7 days), and a **1 GB per-query scan cutoff** (half a cent) stops a
  runaway query. CloudWatch metrics are on.
- The pipeline user's policy (ADR-032) now covers `curated/` as well as
  `raw/`: list, get, put; still no delete, Glue or Athena. You query Athena
  with your own credentials. The policy keeps its name
  `jobpipe-pipeline-raw-rw`, because renaming would replace it.

**Alternatives.**

- *Default `primary` workgroup:* no setup, but no cap and results in
  whatever location the client picks.
- *A separate results bucket:* cleaner separation; one more bucket for no
  benefit at this size.
- *Give the pipeline Athena access* (e.g. a post-export smoke query): useful
  later, but needs Athena, Glue and results-bucket permissions for a
  long-lived key.

**Consequences.** A query that needs more than 1 GB fails instead of
billing; raise `athena_bytes_scanned_cutoff` if that's ever legitimate.

---

## Open questions

Decisions still to make. Each becomes an ADR once decided.

1. **Board health:** automatically set `active=false` after N consecutive
   failed or empty fetches? Should SCD2 then close that board's postings
   (ADR-014)?
2. **Cross-provider duplicates:** the same company on two providers, or the
   same role posted once per city. Dedupe in a mart, or keep and group?
3. **Compensation:** Lever's `salaryRange` and Ashby's `compensation` are
   structured, while Greenhouse puts pay in the description text. Extract in
   staging or in Stage 3?
4. **Incremental SCD2:** at what build time or data size do we switch from a
   full rebuild to merging new days (ADR-013)?
5. **Airflow after the laptop sleeps:** on 2026-11-26 Docker Desktop came back
   with the containers' bind mounts empty, so the DAG files and repo vanished
   inside them and a run hung for a day until `up -d --force-recreate`. Detect
   it (a healthcheck that the DAG file exists, an alert on runs older than N
   hours) or recover automatically?
