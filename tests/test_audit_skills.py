"""Offline tests for the audit's comparison logic (no API calls)."""
from jobpipe.audit_skills import build_request, canonicalize, compare, summarize

CANONICAL = ["Kafka", "PostgreSQL", "Python", "SQL", "Spark"]


def llm(name, level="required", evidence="..."):
    return {"name": name, "requirement_level": level, "evidence": evidence}


def test_canonicalize_maps_case_and_keeps_unknown_names():
    assert canonicalize(" python ", CANONICAL) == "Python"
    assert canonicalize("Snowpark", CANONICAL) == "Snowpark"


def test_compare_sorts_skills_into_the_four_buckets():
    dictionary = {"Python": ("required", "python"), "Spark": ("preferred", "spark"),
                  "Kafka": ("unspecified", "kafka")}
    result = compare("k", "Data Engineer", dictionary,
                     [llm("python"), llm("Spark", "required"), llm("SQL", evidence="strong SQL"),
                      llm("Dagster", "preferred", "Dagster a plus")], CANONICAL)
    assert result.agreed == {"Python": ("required", "required"), "Spark": ("preferred", "required")}
    assert result.dictionary_only == {"Kafka": "kafka"}             # possible false match
    assert result.llm_only_in_seed == {"SQL": "strong SQL"}         # possible missed match
    assert result.llm_only_not_in_seed == {"Dagster": "Dagster a plus"}  # missing from seed


def test_duplicate_llm_skill_keeps_strongest_level():
    result = compare("k", "t", {"Python": ("required", "python")},
                     [llm("Python", "preferred"), llm("python", "required")], CANONICAL)
    assert result.agreed == {"Python": ("required", "required")}


def test_summary_rates():
    a = compare("a", "t", {"Python": ("required", "")}, [llm("Python")], CANONICAL)
    b = compare("b", "t", {"Kafka": ("required", "")}, [llm("SQL", "preferred")], CANONICAL)
    s = summarize([a, b])
    assert (s["agreed"], s["dictionary_only"], s["llm_only_in_seed"]) == (1, 1, 1)
    assert s["precision"] == 0.5 and s["recall"] == 0.5 and s["level_agreement"] == 1.0


def test_request_forces_the_tool_and_lists_canonical_names():
    req = build_request({"company_slug": "acme", "title": "DE", "description_text": "Python"},
                        CANONICAL, "claude-haiku-5-5")
    assert req["tool_choice"] == {"type": "tool", "name": "record_skills"}
    assert "PostgreSQL" in req["system"] and "Title: DE" in req["messages"][0]["content"]
