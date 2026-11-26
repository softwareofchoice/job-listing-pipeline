"""jobpipe.report: CSV + HTML from the scored tables (ADR-029).

Builds a tiny DuckDB file with just the two tables the report reads, so this
runs in milliseconds without dbt.
"""
import csv
from datetime import date

import duckdb

from jobpipe.report import run

AS_OF = date(2026, 1, 15)


def make_db(path, rows):
    con = duckdb.connect(str(path))
    con.execute("""create table int_posting_scores as select * from (values
        ('a', true,  true,  date '2026-01-15', date '2026-01-15'),
        ('b', true,  true,  date '2026-01-01', date '2026-01-15'),
        ('c', true,  false, date '2026-01-15', date '2026-01-15'),
        ('d', false, true,  date '2026-01-15', date '2026-01-15')
    ) t(posting_key, passes_title, passes_location, first_seen_date, as_of_date)""")
    con.execute("""create table mart_top_matches (
        rank int, score double, company_slug varchar, title varchar, location_match varchar,
        seniority varchar, why varchar, posting_url varchar, skill_score double,
        experience_score double, level_score double, freshness_score double,
        matched_skills varchar[], missing_required_skills varchar[], years_min int,
        first_seen_date date, posting_key varchar)""")
    if rows:
        con.executemany("insert into mart_top_matches values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.close()


ROWS = [
    (1, 90.0, "acme", "Data Engineer <script>alert(1)</script>", "Chicago", "mid", "skills 90%",
     "https://example.test/a", 0.9, 1.0, 1.0, 1.0, ["SQL", "Python"], [], 3, date(2026, 1, 15), "a"),
    (2, 70.5, "beta", "Analytics Engineer", "Remote (United States)", "senior", "skills 60%",
     "javascript:alert(1)", None, 0.5, 0.8, 0.5, [], ["dbt"], None, date(2026, 1, 1), "b"),
]


def test_writes_dated_csv_and_html(tmp_path):
    make_db(tmp_path / "w.duckdb", ROWS)
    out = run(tmp_path / "w.duckdb", tmp_path / "reports", top=10)

    assert out == tmp_path / "reports" / "dt=2026-01-15"   # as-of date, not today
    rows = list(csv.DictReader((out / "top_matches.csv").open()))
    assert [r["rank"] for r in rows] == ["1", "2"]
    assert rows[0]["matched_skills"] == "SQL, Python"
    assert rows[0]["is_new"] == "True" and rows[1]["is_new"] == "False"
    assert rows[1]["missing_required_skills"] == "dbt"


def test_html_escapes_job_board_text_and_drops_unsafe_links(tmp_path):
    make_db(tmp_path / "w.duckdb", ROWS)
    page = (run(tmp_path / "w.duckdb", tmp_path / "r", top=10) / "top_matches.html").read_text()

    assert "<script>" not in page and "&lt;script&gt;" in page
    assert 'href="https://example.test/a"' in page
    assert "javascript:" not in page            # non-http(s) URL rendered as plain text
    assert "2 matches (1 new) out of 4 open postings" in page


def test_rerun_overwrites_and_top_limits_rows(tmp_path):
    make_db(tmp_path / "w.duckdb", ROWS)
    run(tmp_path / "w.duckdb", tmp_path / "r", top=10)
    out = run(tmp_path / "w.duckdb", tmp_path / "r", top=1)

    assert sorted(p.name for p in out.iterdir()) == ["top_matches.csv", "top_matches.html"]
    assert len(list(csv.DictReader((out / "top_matches.csv").open()))) == 1


def test_empty_list_still_writes_a_report(tmp_path):
    make_db(tmp_path / "w.duckdb", [])
    out = run(tmp_path / "w.duckdb", tmp_path / "r", top=10)
    assert "No postings pass both filters today." in (out / "top_matches.html").read_text()
