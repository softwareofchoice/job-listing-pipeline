# job-listing-pipeline

A learning project for data engineering practices. The owner wants to fully
understand each part of the system.

## Working conventions

- **Ask before implementing a feature for the first time.** Lay out the design
  options and trade-offs and let the owner choose before writing code.
- **Record every design decision in [docs/decisions.md](docs/decisions.md)**
  as a new ADR (next number, Context / Decision / Alternatives / Consequences,
  dated). Never rewrite an accepted ADR: add a new one that supersedes it.
  Move items out of "Open questions" when they're decided.
- Keep the raw layer untouched (ADR-002); filtering and ranking belong in the
  final mart (ADR-006).
- Never commit secrets. They live in the git-ignored `.env` (ADR-009); add new
  variable names, without values, to `.env.example`.

## Commands

```bash
.venv/bin/pytest -q
PYTHONPATH=src .venv/bin/python -m jobpipe.ingest
PYTHONPATH=src .venv/bin/python -m jobpipe.discover --dry-run
```
