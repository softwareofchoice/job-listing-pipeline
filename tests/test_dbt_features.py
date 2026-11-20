"""End-to-end test of Stage 3 feature extraction (skills, experience, seniority).

Each hand-written posting targets specific rules, so a failure points at the
rule that broke. All postings are fetched once (day 1).
"""
import pytest

from dbt_harness import build_warehouse, requires_dbt

pytestmark = requires_dbt


def ashby(job_id, title, text):
    return {"id": job_id, "title": title, "location": "Remote", "secondaryLocations": [],
            "department": "Eng", "team": "Data", "workplaceType": "Remote", "employmentType": "FullTime",
            "publishedAt": "2026-01-01T00:00:00Z", "jobUrl": f"https://example.test/{job_id}",
            "descriptionPlain": text}


SENIOR_DE = ashby("A1", "Senior Data Engineer", "\n".join([
    "About the role",                              # -> responsibilities section
    "You will build streaming pipelines with Kafka.",
    "Requirements",                                # -> required section
    "5+ years of experience in data engineering",
    "Strong Python and SQL",
    "Familiarity with Snowflake is a plus",        # inline cue overrides the section
    "Nice to have",                                # -> preferred section
    "Experience with Airflow",
    "Benefits",                                    # -> other section: ignored
    "Everyone can excel here. We were founded 20 years ago and use Terraform.",
]))

DATABRICKS_DE = ashby("D1", "Data Engineer II", "\n".join([
    "Requirements",
    "Experience with Databricks and Spark",        # employer's own product counts (ADR-020)
    "3-5 years of professional experience",
]))

PLATFORM = ashby("P1", "Platform Engineer", "\n".join([
    "What you'll do",
    "Maintain reports covering the last 3 years of data",   # not a requirement
    "Qualifications",
    "three (3) years building backend services in Go, Rust",
]))

DATABRICKS_SALES = ashby("D2", "Account Executive", "\n".join([
    "As an account executive at Databricks you will sell Spark solutions.",  # intro: name only
    "Requirements",
    "Report to the Databricks sales director",     # required section, but no usage cue
]))

VAGUE = ashby("V1", "Analyst", "We are hiring.")

GREENHOUSE_STAFF = {
    "id": 9, "title": "Senior Staff Engineer", "location": {"name": "NYC"}, "offices": [],
    "departments": [{"name": "Eng"}], "first_published": "2026-01-01T00:00:00Z",
    "updated_at": "2026-01-01T00:00:00Z", "absolute_url": "https://example.test/9",
    "content": "&lt;p&gt;Minimum qualifications&lt;/p&gt;&lt;ul&gt;&lt;li&gt;8+ years with Java&lt;/li&gt;&lt;/ul&gt;",
}

LEVER_EM = {
    "id": "L9", "text": "Engineering Manager, Data", "createdAt": 1767225600000,
    "categories": {"location": "Austin", "allLocations": ["Austin"], "team": "Data",
                   "department": "Eng", "commitment": "Full-time"},
    "workplaceType": "onsite", "hostedUrl": "https://example.test/L9",
    "descriptionPlain": "Lead the team.",
    "lists": [{"text": "Requirements", "content": "<li>Production Kafka experience</li>"}],
    "additionalPlain": "",
}

FETCHES = {
    ("ashby", "gamma"): {1: [SENIOR_DE, PLATFORM, VAGUE]},
    ("ashby", "databricks"): {1: [DATABRICKS_DE, DATABRICKS_SALES]},
    ("greenhouse", "acme"): {1: [GREENHOUSE_STAFF]},
    ("lever", "beta"): {1: [LEVER_EM]},
}


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    con = build_warehouse(tmp_path_factory.mktemp("dbt_features"), FETCHES)
    yield con
    con.close()


def skills(con, posting_key):
    return dict(con.execute(
        "select skill, requirement_level from int_posting_skills where posting_key = ?",
        [posting_key]).fetchall())


def features(con, posting_key):
    return con.execute(
        "select years_min, years_max, seniority, seniority_source from int_posting_features "
        "where posting_key = ?", [posting_key]).fetchone()


def test_sections_decide_requirement_level(warehouse):
    assert skills(warehouse, "ashby:gamma:A1") == {
        "Kafka": "unspecified",      # responsibilities section
        "Streaming": "unspecified",
        "Python": "required",
        "SQL": "required",
        "Snowflake": "preferred",    # "is a plus" inside the requirements section
        "Airflow": "preferred",
        # Terraform/Excel only appear under Benefits -> ignored
    }


def test_employer_product_counts_only_as_a_stated_skill(warehouse):
    # "Experience with Databricks" in Requirements: a real requirement
    assert skills(warehouse, "ashby:databricks:D1") == {"Databricks": "required", "Spark": "required"}
    # The company name in the intro or "report to the Databricks director": not a skill
    assert skills(warehouse, "ashby:databricks:D2") == {"Spark": "unspecified"}


def test_html_and_lever_list_headings_become_sections(warehouse):
    assert skills(warehouse, "greenhouse:acme:9") == {"Java": "required"}
    assert skills(warehouse, "lever:beta:L9") == {"Kafka": "required"}


def test_years_extraction(warehouse):
    assert features(warehouse, "ashby:gamma:A1")[:2] == (5, 5)          # "founded 20 years ago" ignored
    assert features(warehouse, "ashby:databricks:D1")[:2] == (3, 5)     # range
    assert features(warehouse, "ashby:gamma:P1")[:2] == (3, 3)          # "three (3) years"; "last 3 years" ignored
    assert features(warehouse, "greenhouse:acme:9")[:2] == (8, 8)
    assert features(warehouse, "ashby:gamma:V1")[:2] == (None, None)


def test_seniority_title_first_then_years(warehouse):
    assert features(warehouse, "ashby:gamma:A1")[2:] == ("senior", "title")
    assert features(warehouse, "ashby:databricks:D1")[2:] == ("mid", "title")    # "II"
    assert features(warehouse, "greenhouse:acme:9")[2:] == ("staff", "title")    # highest keyword wins
    assert features(warehouse, "lever:beta:L9")[2:] == ("manager", "title")
    assert features(warehouse, "ashby:gamma:P1")[2:] == ("mid", "years")         # no keyword, 3 years
    assert features(warehouse, "ashby:gamma:V1")[2:] == ("unknown", "none")


def test_go_and_rust_found_in_a_list(warehouse):
    assert skills(warehouse, "ashby:gamma:P1") == {"Go": "required", "Rust": "required"}
