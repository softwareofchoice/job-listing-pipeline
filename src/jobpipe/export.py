"""Export the finished dbt tables as Parquet for Athena (ADR-034).

    python -m jobpipe.export                  # write data/curated/<table>/dt=<as_of>/data.parquet
    python -m jobpipe.export --write-schemas  # also refresh the Glue column list for Terraform

DuckDB keeps doing every transformation. This step only copies the results
out, one daily snapshot per table, partitioned by the scoring "as of" date
(the latest fetch, ADR-025), like the report (ADR-029):

    data/curated/postings_history/dt=2026-11-26/data.parquet
    data/curated/posting_features/dt=2026-11-26/data.parquet
    data/curated/posting_scores/dt=2026-11-26/data.parquet
    data/curated/top_matches/dt=2026-11-26/data.parquet

`python -m jobpipe.upload --root data/curated --prefix curated` then copies
them to s3://<bucket>/curated/, where Glue tables (Terraform) describe them
and Athena queries them.

Glue needs explicit column types. They live in
infra/terraform/main/curated_schemas.json, generated from the warehouse by
--write-schemas, and a test fails if the file and the export disagree
(ADR-035), so the catalog can't silently drift from the data.

Re-running the same day overwrites the same files. Rows are sorted, so the
same data gives the same Parquet bytes and upload skips unchanged files.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import duckdb

log = logging.getLogger("jobpipe.export")

DEFAULT_DB = os.environ.get("JOBPIPE_DUCKDB_PATH", "data/warehouse.duckdb")
DEFAULT_OUT = Path("data/curated")
SCHEMAS_FILE = Path("infra/terraform/main/curated_schemas.json")

# Athena table name -> (dbt model, what it is). Athena names drop dbt's
# layer prefixes: analysts query "postings_history", not "int_postings__scd2".
TABLES = {
    "postings_history": ("int_postings__scd2", "Every version of every posting (SCD Type 2, ADR-013)"),
    "posting_features": ("int_posting_features", "Skills, years and seniority per posting version (Stage 3)"),
    "posting_scores": ("int_posting_scores", "Filters and score parts for current postings (Stage 4)"),
    "top_matches": ("mart_top_matches", "Ranked postings that pass both filters (Stage 4)"),
}

# DuckDB type -> Glue/Hive type (what Athena reads Parquet as).
GLUE_TYPES = {
    "VARCHAR": "string",
    "BIGINT": "bigint",
    "INTEGER": "int",
    "DOUBLE": "double",
    "BOOLEAN": "boolean",
    "DATE": "date",
    # Parquet stores it as UTC microseconds; Athena reads it as timestamp (UTC).
    "TIMESTAMP WITH TIME ZONE": "timestamp",
    "TIMESTAMP": "timestamp",
}


def glue_type(duckdb_type: str) -> str:
    if duckdb_type.endswith("[]"):
        return f"array<{glue_type(duckdb_type[:-2])}>"
    try:
        return GLUE_TYPES[duckdb_type]
    except KeyError:
        # Fail loudly: a new type needs a deliberate mapping, not a guess.
        raise ValueError(f"no Glue type for DuckDB type {duckdb_type!r}; add it to GLUE_TYPES") from None


def schemas(con: duckdb.DuckDBPyConnection) -> dict:
    """The Glue table definitions, from the warehouse's actual columns."""
    out = {}
    for name, (model, description) in TABLES.items():
        cols = con.execute(f"describe {model}").fetchall()
        out[name] = {
            "description": description,
            "dbt_model": model,
            "columns": [{"name": c[0], "type": glue_type(c[1])} for c in cols],
        }
    return out


def dump_schemas(tables: dict) -> str:
    """JSON with one column per line: easy to read and to diff in a PR."""
    parts = []
    for name, t in tables.items():
        cols = ",\n".join(f"      {json.dumps(c)}" for c in t["columns"])
        parts.append(f'  {json.dumps(name)}: {{\n'
                     f'    "description": {json.dumps(t["description"])},\n'
                     f'    "dbt_model": {json.dumps(t["dbt_model"])},\n'
                     f'    "columns": [\n{cols}\n    ]\n  }}')
    return "{\n" + ",\n".join(parts) + "\n}\n"


def as_of(con: duckdb.DuckDBPyConnection) -> str:
    value = con.execute("select max(as_of_date) from int_posting_scores").fetchone()[0]
    if value is None:
        raise SystemExit("int_posting_scores is empty: run `dbt build` first")
    return value.isoformat()


def export(con: duckdb.DuckDBPyConnection, out_root: Path) -> tuple[str, list[Path]]:
    dt = as_of(con)
    written = []
    for name, (model, _) in TABLES.items():
        d = out_root / name / f"dt={dt}"
        d.mkdir(parents=True, exist_ok=True)
        path = d / "data.parquet"
        tmp = d / "data.parquet.tmp"  # upload ignores *.tmp
        # ORDER BY ALL: same rows -> same file bytes -> upload sees "unchanged".
        con.execute(f"copy (select * from {model} order by all) to '{tmp}' "
                    "(format parquet, compression zstd)")
        os.replace(tmp, path)  # atomic: Athena or upload never see half a file
        n = con.execute(f"select count(*) from '{path}'").fetchone()[0]
        log.info("%-17s %6d rows  %8d bytes  %s", name, n, path.stat().st_size, path)
        written.append(path)
    return dt, written


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument("--write-schemas", action="store_true",
                   help=f"also rewrite {SCHEMAS_FILE} (then terraform plan/apply)")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Read-only: never competes with a dbt build for DuckDB's single writer lock.
    with duckdb.connect(args.db, read_only=True) as con:
        dt, _ = export(con, args.out)
        if args.write_schemas:
            SCHEMAS_FILE.write_text(dump_schemas(schemas(con)))
            log.info("wrote %s", SCHEMAS_FILE)
    log.info("exported dt=%s to %s", dt, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
