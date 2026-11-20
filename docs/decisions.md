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
| [015](#adr-015-extract-skills-with-a-regex-dictionary-in-dbt) | Extract skills with a regex dictionary in dbt | Accepted | 2026-11-19 |
| [016](#adr-016-tag-mentions-by-section-required-preferred-or-unspecified) | Tag mentions by section: required, preferred, or unspecified | Accepted | 2026-11-19 |
| [017](#adr-017-keep-every-years-of-experience-mention-summarize-as-min-and-max) | Keep every years-of-experience mention; summarize as min and max | Accepted | 2026-11-19 |
| [018](#adr-018-seniority-from-title-keywords-then-years) | Seniority from title keywords, then years | Accepted | 2026-11-19 |
| [019](#adr-019-audit-the-skill-dictionary-with-claude-haiku-on-a-fixed-sample) | Audit the skill dictionary with Claude Haiku on a fixed sample | Accepted | 2026-11-19 |

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

**Status:** Accepted, 2026-11-19. An LLM audit of the dictionary is planned
as a separate decision.

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

## Open questions

Decisions still to make. Each becomes an ADR once decided.

1. **Board health:** automatically set `active=false` after N consecutive
   failed or empty fetches? Should SCD2 then close that board's postings
   (ADR-014)?
2. **Profile:** which criteria are hard filters and which are scores (ADR-006)?
3. **Cross-provider duplicates:** the same company on two providers, or the
   same role posted once per city. Dedupe in a mart, or keep and group?
4. **Compensation:** Lever's `salaryRange` and Ashby's `compensation` are
   structured, while Greenhouse puts pay in the description text. Extract in
   staging or in Stage 3?
5. **Incremental SCD2:** at what build time or data size do we switch from a
   full rebuild to merging new days (ADR-013)?
