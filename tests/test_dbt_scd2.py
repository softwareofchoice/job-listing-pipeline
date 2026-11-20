"""End-to-end test of the dbt staging + SCD2 models on a hand-made raw layer.

Real data only shows us the cases that happened to occur. This builds a tiny
raw folder where we control every edge case, runs `dbt build` against a
throwaway DuckDB file, and checks the resulting history row by row.

Scenario (Greenhouse board "acme", Lever board "beta", Ashby board "gamma"):

    day   acme (greenhouse)        beta (lever)    gamma (ashby)
    01    A(title v1), B           L1              X
    02    A(title v2), B           L1              X
    03    <fetch failed>           L1              X
    04    A(v2)          (B gone)  []  (L1 gone)   X
    05    A(v2), B       (B back)  <fetch failed>  X
"""
import pytest

from dbt_harness import build_warehouse, requires_dbt
from dbt_harness import day as d

pytestmark = requires_dbt


def gh_job(job_id, title):
    # Greenhouse escapes its HTML, so '&amp;amp;' is a literal '&' after decoding.
    return {"id": job_id, "title": title, "location": {"name": "Remote (US)"},
            "offices": [{"name": "New York "}], "departments": [{"name": "Data"}],
            "first_published": "2025-12-20T10:00:00-05:00", "updated_at": "2026-01-01T00:00:00Z",
            "absolute_url": f"https://example.test/{job_id}",
            "content": "&lt;p&gt;Python &amp;amp; SQL&lt;/p&gt;&lt;ul&gt;&lt;li&gt;Spark&lt;/li&gt;&lt;li&gt;dbt&lt;/li&gt;&lt;/ul&gt;"}


LEVER_L1 = {"id": "L1", "text": "Data Engineer", "createdAt": 1767225600000,
            "categories": {"location": "Austin, TX", "allLocations": ["Austin, TX"],
                           "team": "Platform", "department": "Eng", "commitment": "Full-time"},
            "workplaceType": "hybrid", "hostedUrl": "https://example.test/L1",
            "descriptionPlain": "Intro text.",
            "lists": [{"text": "Requirements", "content": "<li>3+ years of Kafka</li><li>AWS</li>"}],
            "additionalPlain": "Benefits."}

ASHBY_X = {"id": "X", "title": "Analytics Engineer", "location": "Remote", "secondaryLocations": [],
           "department": "Data", "team": "Analytics", "workplaceType": "Remote",
           "employmentType": "FullTime", "publishedAt": "2025-12-01T00:00:00Z",
           "jobUrl": "https://example.test/X", "descriptionPlain": "Uses  Snowflake. "}

A1, A2, B = gh_job(1, "Data Engineer I"), gh_job(1, "Data Engineer II"), gh_job(2, "Analytics Engineer")

FETCHES = {  # (source, company): {day: payload}; a missing day = failed fetch
    ("greenhouse", "acme"): {1: [A1, B], 2: [A2, B], 4: [A2], 5: [A2, B]},
    ("lever", "beta"): {1: [LEVER_L1], 2: [LEVER_L1], 3: [LEVER_L1], 4: []},
    ("ashby", "gamma"): {d: [ASHBY_X] for d in range(1, 6)},
}


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    con = build_warehouse(tmp_path_factory.mktemp("dbt"), FETCHES)
    yield con
    con.close()


def history(con, key):
    return con.execute(
        "select version_number, valid_from, valid_to, is_current, end_reason, title "
        "from int_postings__scd2 where posting_key = ? order by version_number", [key]).fetchall()


def test_edit_creates_new_version_and_failed_fetch_is_not_a_close(warehouse):
    assert history(warehouse, "greenhouse:acme:1") == [
        (1, d(1), d(2), False, "changed", "Data Engineer I"),
        # day 3's fetch failed: A must NOT close and reopen around it
        (2, d(2), None, True, None, "Data Engineer II"),
    ]


def test_missing_from_good_fetch_closes_and_return_reopens(warehouse):
    assert history(warehouse, "greenhouse:acme:2") == [
        (1, d(1), d(4), False, "closed", "Analytics Engineer"),
        (2, d(5), None, True, None, "Analytics Engineer"),
    ]


def test_empty_board_closes_posting_and_later_failure_changes_nothing(warehouse):
    assert history(warehouse, "lever:beta:L1") == [
        (1, d(1), d(4), False, "closed", "Data Engineer"),
    ]


def test_unchanged_posting_is_a_single_version(warehouse):
    assert history(warehouse, "ashby:gamma:X") == [
        (1, d(1), None, True, None, "Analytics Engineer"),
    ]


def test_description_text_is_clean_for_each_provider(warehouse):
    text = dict(warehouse.execute(
        "select posting_key, description_text from int_postings__daily where dt = ?", [d(1)]).fetchall())
    assert text["greenhouse:acme:1"] == "Python & SQL\nSpark\ndbt"
    # Lever's requirement lists are stitched back in
    assert text["lever:beta:L1"] == "Intro text.\nRequirements\n3+ years of Kafka\nAWS\nBenefits."
    assert text["ashby:gamma:X"] == "Uses Snowflake."


def test_locations_are_trimmed_deduplicated_and_sorted(warehouse):
    # Sorted so the list (part of the SCD2 hash) is deterministic day to day.
    locs = warehouse.execute(
        "select locations_raw from int_postings__daily where posting_key = 'greenhouse:acme:1' limit 1").fetchone()[0]
    assert locs == ["New York", "Remote (US)"]
