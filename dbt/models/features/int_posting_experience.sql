{{ config(materialized='table') }}
{#
  Every "years of experience" mention, one row each (ADR-017). Kept as a
  detail table so a wrong years_min/years_max can be traced back to the text.

  Matches "3+ years", "3-5 years", "three (3) years", "5 or more years",
  "at least 4 yrs"; captures up to ~40 characters before and ~50 after for
  the context filters (digits are excluded from the context so that
  "3+ years of Python and 5+ years of SQL" yields two mentions, not one).

  Kept only if it looks like a requirement, not company trivia:
    * not in an 'other' section (benefits like "after 5 years of service")
    * in a required/preferred section (where "8+ years designing..." is
      always a requirement), OR the context mentions experience, OR the
      number is followed by "of/in/with/working/building..."
    * not "founded 20 years ago", "for over 15 years", "the last 3 years",
      "20 years in business"
    * the number is at most 25
#}
{% set num = "\\d{1,2}|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen" %}
{% set words = "['one','two','three','four','five','six','seven','eight','nine','ten','eleven','twelve','thirteen','fourteen','fifteen']" %}
{% set mention = "([^.;\\d]{0,40}?)\\b(" ~ num ~ ")\\s*(?:\\(\\d{1,2}\\+?\\)\\s*)?\\+?\\s*(?:(?:-|–|to)\\s*(" ~ num ~ ")\\s*\\+?\\s*)?(?:or more\\s*)?(?:years?|yrs?)\\b([^.;\\d]{0,50})" %}

with candidate_lines as (
    select *
    from {{ ref('int_posting_lines') }}
    where section <> 'other'
      and not is_title
      and regexp_matches(line_lower, '(years?|yrs?)\b')
),

matches as (
    select
        posting_version_key, posting_key, version_number, line_no, section, requirement_level,
        unnest(regexp_extract_all(line_lower, '{{ mention }}')) as match_text
    from candidate_lines
),

parsed as (
    select
        *,
        regexp_extract(match_text, '{{ mention }}', ['before', 'lo', 'hi', 'after']) as p
    from matches
),

typed as (
    select
        posting_version_key, posting_key, version_number, line_no, section, requirement_level,
        trim(match_text)                                                            as match_text,
        coalesce(try_cast(p.lo as int), list_position({{ words }}, p.lo))           as years_lo,
        coalesce(try_cast(p.hi as int), list_position({{ words }}, p.hi))           as years_hi_raw,
        p.before                                                                    as ctx_before,
        p.after                                                                     as ctx_after
    from parsed
)

select
    posting_version_key,
    posting_key,
    version_number,
    line_no,
    section,
    requirement_level,
    match_text,
    years_lo,
    coalesce(years_hi_raw, years_lo)                                                as years_hi
from typed
where years_lo <= 25
  and coalesce(years_hi_raw, years_lo) >= years_lo
  and (
        section in ('required', 'preferred')
     or regexp_matches(ctx_before || ' ' || ctx_after, 'experience|\bexp\b')
     or regexp_matches(ctx_after, '^\s*(\+\s*)?(of|in|with|working|building|developing|doing|as|at|on|post|professional|relevant|related|industry|hands|full[- ]time|programming|engineering|software|data|leading|managing)\b')
  )
  and not regexp_matches(ctx_before || ' ' || ctx_after, '\bago\b|years? old|of age|\bhistory\b|founded|anniversary|^\s*in business\s*($|,|!)|of service|tenure|warrant')
  and not regexp_matches(ctx_before, '(for (over|more than|nearly|almost|about)|over the|in the|within the|the past|the last|the next|every)\s*(the past|the last)?\s*$')
