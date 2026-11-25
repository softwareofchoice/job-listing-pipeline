{{ config(materialized='view') }}
{#
  Your ranked list: current postings that pass BOTH hard filters (job
  function and location, ADR-023), best score first. This is the only place
  postings are filtered out (ADR-006); everything else is in int_posting_scores.
#}
select
    row_number() over (order by score desc, first_seen_date desc, posting_key) as rank,
    score,
    company_slug,
    title,
    location_match,
    seniority,
    why,
    posting_url,
    skill_score,
    experience_score,
    level_score,
    freshness_score,
    matched_skills,
    missing_required_skills,
    years_min,
    first_seen_date,
    posting_key
from {{ ref('int_posting_scores') }}
where passes_title and passes_location
