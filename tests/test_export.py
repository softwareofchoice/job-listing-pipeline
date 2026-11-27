"""jobpipe.export: curated Parquet for Athena (ADR-034, ADR-035).

The drift test is the important one: Terraform builds the Glue tables from
infra/terraform/main/curated_schemas.json. If a dbt model gains, loses or
retypes a column and nobody regenerates that file, Athena would read the
Parquet with the wrong schema. This test fails first.
"""
import json
import shutil

import duckdb
import pytest

from dbt_harness import DBT, REPO, build_warehouse
from jobpipe import export

SCHEMAS = REPO / "infra" / "terraform" / "main" / "curated_schemas.json"

FETCHES = {
    ("greenhouse", "acme"): {
        1: [{"id": 1, "title": "Senior Data Engineer", "location": {"name": "Remote (US)"},
             "absolute_url": "https://example.test/1",
             "content": "&lt;p&gt;Requirements: 5+ years of Python, SQL and Airflow&lt;/p&gt;"}],
    },
    ("lever", "beta"): {
        1: [{"id": "L1", "text": "Analytics Engineer", "categories": {"location": "Austin, TX"},
             "hostedUrl": "https://example.test/L1", "descriptionPlain": "dbt and Snowflake"}],
    },
    ("ashby", "gamma"): {
        1: [{"id": "X", "title": "Data Engineer", "location": "Remote", "secondaryLocations": [],
             "jobUrl": "https://example.test/X", "descriptionPlain": "Kafka"}],
    },
}


def test_glue_types():
    assert export.glue_type("VARCHAR") == "string"
    assert export.glue_type("VARCHAR[]") == "array<string>"
    assert export.glue_type("TIMESTAMP WITH TIME ZONE") == "timestamp"
    with pytest.raises(ValueError, match="HUGEINT"):
        export.glue_type("HUGEINT")


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    if not DBT.exists() and not shutil.which("dbt"):
        pytest.skip("dbt not installed")
    tmp = tmp_path_factory.mktemp("export")
    return tmp, build_warehouse(tmp, FETCHES)


@pytest.fixture(scope="module")
def exported(warehouse):
    tmp, con = warehouse
    dt, paths = export.export(con, tmp / "curated")
    return tmp, con, dt, paths


def test_one_snapshot_per_table_under_dt(exported):
    tmp, _, dt, paths = exported
    assert dt == "2026-01-01"
    assert sorted(p.relative_to(tmp / "curated").as_posix() for p in paths) == sorted(
        f"{name}/dt={dt}/data.parquet" for name in export.TABLES)
    assert not list((tmp / "curated").rglob("*.tmp"))


def test_parquet_has_every_row(exported):
    _, con, _, paths = exported
    for path in paths:
        model = export.TABLES[path.parent.parent.name][0]
        assert duckdb.connect().execute(f"select count(*) from '{path}'").fetchone() == \
            con.execute(f"select count(*) from {model}").fetchone()


def test_same_data_gives_identical_files(exported):
    tmp, con, _, paths = exported
    before = {p: p.read_bytes() for p in paths}
    export.export(con, tmp / "curated")
    assert all(p.read_bytes() == b for p, b in before.items())


def test_committed_glue_schemas_match_the_warehouse(exported):
    _, con, _, _ = exported
    committed = json.loads(SCHEMAS.read_text())
    assert committed == export.schemas(con), (
        "curated_schemas.json is out of date: run "
        "`PYTHONPATH=src .venv/bin/python -m jobpipe.export --write-schemas`, "
        "then `terraform -chdir=infra/terraform/main apply`")


def test_parquet_files_carry_the_glue_column_types(exported):
    """What Athena actually reads is the Parquet file, so check its schema too."""
    _, _, _, paths = exported
    committed = json.loads(SCHEMAS.read_text())
    for path in paths:
        # hive_partitioning off: DuckDB would otherwise add dt from the folder name.
        cols = duckdb.connect().execute(
            f"describe select * from read_parquet('{path}', hive_partitioning = false)").fetchall()
        assert [{"name": c[0], "type": export.glue_type(c[1])} for c in cols] == \
            committed[path.parent.parent.name]["columns"], path


def test_schema_file_is_valid_for_terraform():
    tables = json.loads(SCHEMAS.read_text())
    assert set(tables) == set(export.TABLES)
    for t in tables.values():
        names = [c["name"] for c in t["columns"]]
        assert len(names) == len(set(names))
        assert "dt" not in names  # dt is the partition key, not a data column
