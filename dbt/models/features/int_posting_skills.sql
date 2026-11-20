{{ config(materialized='table') }}
-- depends_on: {{ ref('skills') }}
{#
  One row per (posting version, skill) found in its title or description
  (ADR-015), with the strongest requirement level across its mentions
  (ADR-016): required > preferred > unspecified.

  Skipped on purpose:
    * mentions in 'other' sections (about us, benefits, EEO): they describe
      the company, not the role
    * the employer's own product (Databricks at Databricks), UNLESS the line
      is in a required/preferred section and has a usage cue ("proficient
      on the Databricks platform"). Otherwise it's just the company name:
      "reporting to Databricks' senior director" (ADR-020)

  Performance: the seed's patterns are read at COMPILE time (run_query) and
  written into the SQL as constants. Joining lines to the seed and calling
  regexp_matches(line, s.pattern) made DuckDB recompile a regex for every
  (line, skill) pair, ~12M times: 51s. With constants each regex compiles
  once: ~2s. The `depends_on` comment above tells dbt to build the seed first.
#}
{% if execute %}
    {% set skills = run_query("select skill, pattern from " ~ ref('skills') ~ " order by skill").rows %}
{% else %}
    {% set skills = [] %}
{% endif %}

{% set employer_skill_cue = "experience|proficien|understanding of|knowledge of|familiar|expertise|hands[- ]on|certif|working with|skilled" %}

with lines as (
    select *
    from {{ ref('int_posting_lines') }}
    where section <> 'other'
),

mentions as (
    select
        posting_version_key, posting_key, version_number, company_slug, line_no, line_lower, section, requirement_level,
        unnest(list_filter([
            {%- for s in skills %}
            {'skill': '{{ s[0] | replace("'", "''") }}', 'matched_text': regexp_extract(line_lower, '{{ s[1] | replace("'", "''") }}')}
            {%- if not loop.last %},{% endif %}
            {%- endfor %}
        ], lambda m: m.matched_text <> ''), recursive := true)
    from lines
)

select
    m.posting_version_key,
    any_value(m.posting_key)                                 as posting_key,
    any_value(m.version_number)                              as version_number,
    m.skill,
    any_value(s.category)                                    as category,
    case
        when bool_or(m.requirement_level = 'required')  then 'required'
        when bool_or(m.requirement_level = 'preferred') then 'preferred'
        else 'unspecified'
    end                                                      as requirement_level,
    count(*)                                                 as n_mentions,
    list_sort(list_distinct(list(m.section)))                as sections,
    arg_min(m.matched_text, m.line_no)                       as example_match
from mentions as m
join {{ ref('skills') }} as s on s.skill = m.skill
where lower(m.skill) <> m.company_slug
   or (m.section in ('required', 'preferred')
       and regexp_matches(m.line_lower, '{{ employer_skill_cue }}'))
group by m.posting_version_key, m.skill
