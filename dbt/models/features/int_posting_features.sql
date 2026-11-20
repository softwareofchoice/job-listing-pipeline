{{ config(materialized='table') }}
{#
  One row per posting version: everything Stage 4 (scoring) needs.

  Seniority (ADR-018): title keywords first, checked from the top of the
  ladder down, so "Senior Staff Engineer" is staff and "Senior Engineering
  Manager" is manager. Only if the title has no keyword do we fall back to
  years_min: 0-2 entry, 3-5 mid, 6+ senior. seniority_source says which rule
  decided, so a surprising level can be explained.

  "manager" only counts as a management level for people-management titles:
  titles that START with (Senior) Manager ("Sr. Manager, Field Engineering")
  or name a team ("Engineering Manager"). Product/Program/Account Manager are
  individual-contributor roles, so their level comes from other keywords or
  from years.
#}
{% set role = "(engineer|developer|scientist|analyst|architect)" %}
{% set title_levels = [
    ('intern',    "\\b(intern|internship|co-?op|apprentice|student)\\b"),
    ('executive', "\\b(chief|cto|cdo|cio|vp|svp|evp|vice president|head of)\\b"),
    ('director',  "\\bdirector\\b"),
    ('manager',   "^((senior|sr)\\.?\\s+)?manager\\b|\\b(engineering|software|data|analytics|science|development|people|team)\\s+manager\\b|\\bmanager of\\b"),
    ('principal', "\\b(principal|distinguished|fellow)\\b"),
    ('staff',     "\\bstaff\\b|\\b" ~ role ~ "\\s+(iv|4)\\b"),
    ('lead',      "\\b(lead|tech lead)\\b"),
    ('senior',    "\\b(senior|sr)\\b|\\b" ~ role ~ "\\s+(iii|3)\\b"),
    ('mid',       "\\b(mid[- ]level|intermediate)\\b|\\b" ~ role ~ "\\s+(ii|2)\\b"),
    ('entry',     "\\b(junior|jr|entry[- ]level|new grad(uate)?|graduate|early[- ]career|associate)\\b|\\b" ~ role ~ "\\s+(i|1)\\b"),
] %}

with versions as (
    select
        posting_key || ':v' || version_number as posting_version_key,
        *
    from {{ ref('int_postings__scd2') }}
),

experience as (
    select
        posting_version_key,
        min(years_lo)       as years_min,
        max(years_hi)       as years_max,
        count(*)            as n_experience_mentions
    from {{ ref('int_posting_experience') }}
    group by 1
),

skills as (
    select
        posting_version_key,
        list_sort(list(skill) filter (where requirement_level = 'required'))     as skills_required,
        list_sort(list(skill) filter (where requirement_level = 'preferred'))    as skills_preferred,
        list_sort(list(skill) filter (where requirement_level = 'unspecified'))  as skills_unspecified
    from {{ ref('int_posting_skills') }}
    group by 1
),

title_level as (
    select
        posting_version_key,
        case
            {% for level, pattern in title_levels %}
            when regexp_matches(lower(title), '{{ pattern }}') then '{{ level }}'
            {% endfor %}
        end as title_seniority
    from versions
)

select
    v.posting_version_key,
    v.posting_key,
    v.version_number,
    v.source,
    v.company_slug,
    v.title,
    v.valid_from,
    v.valid_to,
    v.is_current,
    e.years_min,
    e.years_max,
    coalesce(e.n_experience_mentions, 0)                     as n_experience_mentions,
    case
        when t.title_seniority is not null then t.title_seniority
        when e.years_min <= 2 then 'entry'
        when e.years_min <= 5 then 'mid'
        when e.years_min >= 6 then 'senior'
        else 'unknown'
    end                                                      as seniority,
    case
        when t.title_seniority is not null then 'title'
        when e.years_min is not null then 'years'
        else 'none'
    end                                                      as seniority_source,
    coalesce(s.skills_required, [])                          as skills_required,
    coalesce(s.skills_preferred, [])                         as skills_preferred,
    coalesce(s.skills_unspecified, [])                       as skills_unspecified
from versions as v
left join title_level as t using (posting_version_key)
left join experience  as e using (posting_version_key)
left join skills      as s using (posting_version_key)
