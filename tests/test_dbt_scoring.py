"""End-to-end test of Stage 4 scoring and the ranked mart.

Uses the real profile seeds (dbt/seeds/profile_*.csv). If you change your
profile, the exact-score test below may need its expected number updated;
the comment next to it shows the arithmetic.

Two fetch days: day 1 and day 15 (one freshness half-life apart).
"""
import pytest

from dbt_harness import build_warehouse, day, requires_dbt

pytestmark = requires_dbt


def ashby(job_id, title, location, text="", secondary=(), workplace=None):
    return {"id": job_id, "title": title, "location": location,
            "secondaryLocations": [{"location": s} for s in secondary],
            "department": "Eng", "team": "Data", "workplaceType": workplace, "employmentType": "FullTime",
            "publishedAt": "2025-01-01T00:00:00Z", "jobUrl": f"https://example.test/{job_id}",
            "descriptionPlain": text or "We are hiring."}


GOOD = ashby("GOOD", "Data Engineer", "Remote (US)", "\n".join([
    "Requirements",
    "3+ years of experience",
    "Strong SQL and Python",
    "Experience with Rust",  # in the dictionary, not in the profile
]))
CA_ONLY = ashby("CA", "Data Engineer", "Remote - California")              # state-scoped remote
CHICAGO = ashby("CHI", "Analytics Engineer", "Chicago, IL", workplace="Hybrid")
BARE = ashby("BARE", "Data Engineer", "Remote")                            # unclear -> passes
LONDON_REMOTE = ashby("LDN", "Data Engineer", "London", secondary=["Remote"])
US_REMOTE = ashby("USR", "Senior Software Engineer, Streaming", "US-Remote")
# Remote entries that list alternatives (ADR-026), from real postings. No
# workplace_type, which is how the NULL passes_location bug showed up.
US_OR_CA = ashby("USORCA", "Data Engineer", "Remote (US or Canada)")
CITY_LIST = ashby("CITIES", "Data Engineer", "San Francisco, Seattle, US-Remote")
REMOTE_IN = ashby("REMIN", "Data Engineer", "Toronto Canada, San Francisco, Remote in US, Remote in Canada")
BARE_IN_LIST = ashby("BARELIST", "Data Engineer", "SF, NYC, Seattle, Remote", secondary=["US"])
STATES_ONLY = ashby("STATES", "Data Engineer", "Remote (California, Washington, and Oregon Only)")
WEST_COAST = ashby("WEST", "Data Engineer", "US-San Francisco, US-West Coast (Remote)")
EUROPE_OR = ashby("EUOR", "Data Engineer", "Poland - Remote OR Romania - Remote")
NO_US = ashby("NOUS", "Data Engineer", "SF, NYC, Seattle, Remote", secondary=["Canada"])
SALES = ashby("SALES", "Solutions Engineer, Data Platform", "Remote (US)")
PM = ashby("PM", "Product Manager - Data Platform", "Chicago, IL")
DESIGNER = ashby("DES", "Product Designer", "Chicago, IL")
SALES_DATA = ashby("SD", "Staff Software Engineer, Sales Data & Agents", "Chicago, IL")  # engineering, not sales

DAY1 = [GOOD, CA_ONLY, LONDON_REMOTE, SALES, PM, DESIGNER, SALES_DATA]
DAY15 = DAY1 + [CHICAGO, BARE, US_REMOTE, US_OR_CA, CITY_LIST, REMOTE_IN, BARE_IN_LIST,
                 STATES_ONLY, WEST_COAST, EUROPE_OR, NO_US]


@pytest.fixture(scope="module")
def wh(tmp_path_factory):
    con = build_warehouse(tmp_path_factory.mktemp("dbt"), {
        ("ashby", "acme"): {1: DAY1, 15: DAY15},
        # every provider needs at least one raw file (ADR-011): empty boards
        ("greenhouse", "empty"): {1: [], 15: []},
        ("lever", "empty"): {1: [], 15: []},
    })
    yield con
    con.close()


def scores(wh):
    cols = ["passes_title", "passes_location", "location_match", "score", "skill_score",
            "experience_score", "level_score", "freshness_score", "missing_required_skills", "first_seen_date"]
    rows = wh.execute(f"select posting_key, {', '.join(cols)} from int_posting_scores").fetchall()
    return {r[0].split(":")[-1]: dict(zip(cols, r[1:])) for r in rows}


@pytest.mark.parametrize("job,expected", [
    ("GOOD", "Remote (United States)"),
    ("USR", "Remote (United States)"),
    ("CHI", "Chicago"),
    ("BARE", "Remote (region unknown)"),
    ("CA", None),
    ("LDN", None),
    # alternatives within one entry (ADR-026)
    ("USORCA", "Remote (United States)"),
    ("CITIES", "Remote (United States)"),
    ("REMIN", "Remote (United States)"),
    ("BARELIST", "Remote (United States)"),   # bare "Remote" item, posting is in the US
    ("STATES", None),                          # every option is a state
    ("WEST", None),                            # "us west coast" isn't the whole US
    ("EUOR", None),
    ("NOUS", None),                            # bare "Remote", but nothing says US
])
def test_location_filter(wh, job, expected):
    s = scores(wh)[job]
    assert s["location_match"] == expected
    assert s["passes_location"] is (expected is not None)


@pytest.mark.parametrize("job,passes", [
    ("GOOD", True), ("CHI", True), ("USR", True),          # include patterns
    ("SALES", False), ("PM", False),                         # exclude patterns win
    ("DES", False),                                          # no include pattern
    ("SD", True),                                            # "Sales Data" isn't a sales role
])
def test_title_filter(wh, job, passes):
    assert scores(wh)[job]["passes_title"] is passes


def test_filter_flags_are_never_null(wh):
    nulls = wh.execute("select count(*) from int_posting_scores "
                       "where passes_title is null or passes_location is null").fetchone()[0]
    assert nulls == 0


def test_exact_score_for_a_known_posting(wh):
    s = scores(wh)["GOOD"]
    # skills: SQL (req, weight 3) + Python (req, 3) matched; Rust (req, 1) missing -> 6/7
    assert s["skill_score"] == pytest.approx(6 / 7)
    assert s["missing_required_skills"] == ["Rust"]
    assert s["experience_score"] == 1.0     # 3 years <= 4
    assert s["level_score"] == 1.0          # no title keyword, 3 years -> mid
    assert s["freshness_score"] == 0.5      # first seen day 1, as of day 15: one half-life
    # 100 * (50*6/7 + 25*1 + 15*1 + 10*0.5) / 100
    assert s["score"] == 87.9


def test_unknown_parts_count_as_neutral(wh):
    s = scores(wh)["CHI"]                   # no skills, no years, no seniority keyword
    assert s["skill_score"] is None and s["experience_score"] is None
    assert s["level_score"] == 0.6          # 'unknown' in profile_level_fit
    assert s["freshness_score"] == 1.0      # first seen today (day 15)
    # 100 * (50*0.5 + 25*0.5 + 15*0.6 + 10*1) / 100
    assert s["score"] == 56.5


def test_freshness_uses_first_seen_not_provider_date(wh):
    s = scores(wh)
    assert s["GOOD"]["first_seen_date"] == day(1)   # provider says 2025-01-01
    assert s["BARE"]["first_seen_date"] == day(15)


def test_mart_contains_only_passing_postings_ranked_by_score(wh):
    rows = wh.execute("select rank, posting_key, score from mart_top_matches order by rank").fetchall()
    assert {r[1].split(":")[-1] for r in rows} == {"GOOD", "CHI", "BARE", "USR", "SD",
                                                   "USORCA", "CITIES", "REMIN", "BARELIST"}
    assert [r[0] for r in rows] == list(range(1, 10))
    assert [r[2] for r in rows] == sorted((r[2] for r in rows), reverse=True)
