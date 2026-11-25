{{ config(materialized='table') }}
-- depends_on: {{ ref('profile_titles') }}
-- depends_on: {{ ref('profile_locations') }}
{#
  Every CURRENT posting, scored against your profile (Stage 4).

  Nothing is filtered out here: the hard filters are computed as flags
  (passes_title, passes_location) and applied only in mart_top_matches
  (ADR-006, ADR-023). That way you can always ask "why didn't X show up?".

  Score (ADR-024): a weighted sum of four 0-1 parts, scaled to 0-100.
    skill_score       share of the posting's skill "mass" you have
    experience_score  1 if years asked <= yours, minus a penalty per extra year
    level_score       from profile_level_fit
    freshness_score   halves every N days since we FIRST SAW it (ADR-025)
  A part that can't be computed (no skills or years found) is stored as null
  and counts as `unknown_part_score` (0.5) in the total, neither helping nor
  sinking the posting.

  Title and location patterns are read at compile time (like Stage 3's skill
  patterns) so each regex is compiled once.
#}
{% if execute %}
    {% set titles = run_query("select kind, label, pattern from " ~ ref('profile_titles') ~ " order by kind, label").rows %}
    {% set places = run_query("select label, pattern from " ~ ref('profile_locations') ~ " where kind = 'place' order by label").rows %}
    {% set regions = run_query("select label, pattern from " ~ ref('profile_locations') ~ " where kind = 'remote_region' order by label").rows %}
{% else %}
    {% set titles, places, regions = [], [], [] %}
{% endif %}


with settings as (
    select * from {{ ref('profile_settings') }}
),

-- "Today" for scoring is the latest successful fetch, not the wall clock, so
-- re-running dbt on old data gives the same scores (reproducible builds).
as_of as (
    select max(dt) as as_of_date from {{ ref('stg_raw__fetches') }}
),

first_seen as (
    select posting_key, min(valid_from) as first_seen_date
    from {{ ref('int_postings__scd2') }}
    group by 1
),

current_postings as (
    select
        f.*,
        s.location_raw,
        s.locations_raw,
        s.workplace_type,
        s.posting_url,
        fs.first_seen_date
    from {{ ref('int_posting_features') }} as f
    join {{ ref('int_postings__scd2') }} as s
      on s.posting_key = f.posting_key
     and s.version_number = f.version_number
    join first_seen as fs
      on fs.posting_key = f.posting_key
    where f.is_current
),

-- ---------------------------------------------------------------- filters

titles as (
    select
        posting_version_key,
        {{ label_list(titles | selectattr(0, 'equalto', 'include') | list, 'lower(title)') }} as title_includes,
        {{ label_list(titles | selectattr(0, 'equalto', 'exclude') | list, 'lower(title)') }} as title_excludes
    from current_postings
),

-- One row per location entry ("Remote - California", "Chicago, IL", ...).
location_entries as (
    select
        posting_version_key,
        loc,
        {{ label_list(places, 'loc') }}                                       as place_labels,
        regexp_matches(loc, '\bremote\b')                                     as is_remote,
        -- What's left once "remote" and filler words are removed: "" for a
        -- bare "Remote", "(us)" for "Remote (US)", "california" for
        -- "Remote - California".
        trim(regexp_replace(
            regexp_replace(loc, '\b(remote|in|the|or|and|only|based)\b', ' ', 'g'),
            '[^a-z.]+', ' ', 'g'))                                            as remote_scope
    from current_postings, unnest(coalesce(locations_raw, [])) as t(loc_original),
         lateral (select lower(loc_original) as loc)
),

location_entries_scored as (
    select
        *,
        -- The scope names only allowed regions: strip each region's pattern
        -- and check nothing is left ("us or canada" -> "or canada" isn't empty,
        -- but "us" alone is). Only meaningful for remote entries.
        case when is_remote and remote_scope <> '' then
            list_filter([
                {%- for r in regions %}
                case when trim(regexp_replace(regexp_replace(remote_scope, '{{ r[1] | replace("'", "''") }}', ' ', 'g'), '[^a-z]+', ' ', 'g')) = ''
                     then '{{ r[0] }}' end{{ "," if not loop.last }}
                {%- endfor %}
            ], lambda x: x is not null)
        else [] end                                                           as remote_region_labels,
        is_remote and remote_scope = ''                                       as is_bare_remote,
        {{ label_list(regions, 'loc') }}                                      as region_mentions
    from location_entries
),

locations as (
    select
        p.posting_version_key,
        list_distinct(flatten(list(e.place_labels)))                          as place_matches,
        list_distinct(flatten(list(e.remote_region_labels)))                  as remote_region_matches,
        coalesce(bool_or(e.is_bare_remote), false)                            as has_bare_remote,
        coalesce(bool_and(e.is_bare_remote), false)                           as only_bare_remote,
        list_distinct(flatten(list(e.region_mentions)))                       as region_mentions
    from current_postings as p
    left join location_entries_scored as e using (posting_version_key)
    group by 1
),

-- ----------------------------------------------------------------- skills

posting_skills as (
    select
        ps.posting_version_key,
        ps.skill,
        ps.requirement_level,
        my.weight                                                             as my_weight,
        case ps.requirement_level
            when 'required'  then st.importance_required
            when 'preferred' then st.importance_preferred
            else st.importance_unspecified
        end * coalesce(my.weight, 1)                                          as mass
    from {{ ref('int_posting_skills') }} as ps
    join current_postings as p using (posting_version_key)
    left join {{ ref('profile_skills') }} as my using (skill)
    cross join settings as st
),

skills as (
    select
        posting_version_key,
        sum(mass) filter (where my_weight is not null) / sum(mass)            as skill_score,
        list_sort(list(skill) filter (where my_weight is not null))           as matched_skills,
        list_sort(list(skill) filter (where my_weight is null and requirement_level = 'required')) as missing_required_skills
    from posting_skills
    group by 1
),

-- ------------------------------------------------------------ the 4 parts

parts as (
    select
        p.*,
        t.title_includes,
        t.title_excludes,
        len(t.title_includes) > 0 and len(t.title_excludes) = 0              as passes_title,
        l.place_matches,
        l.remote_region_matches,
        -- passes if: an allowed place, OR remote scoped to an allowed region,
        -- OR a bare "Remote" / workplace_type=remote on a posting that's
        -- otherwise located in an allowed region ("Remote | US"),
        -- OR the posting says only "Remote": unclear isn't a "no" (ADR-023).
        len(l.place_matches) > 0
          or len(l.remote_region_matches) > 0
          or ((l.has_bare_remote or p.workplace_type = 'remote') and len(l.region_mentions) > 0)
          or l.only_bare_remote
                                                                              as passes_location,
        case
            when len(l.place_matches) > 0 then array_to_string(l.place_matches, ', ')
            when len(l.remote_region_matches) > 0 then 'Remote (' || array_to_string(l.remote_region_matches, ', ') || ')'
            when (l.has_bare_remote or p.workplace_type = 'remote') and len(l.region_mentions) > 0
                then 'Remote (' || array_to_string(l.region_mentions, ', ') || ')'
            when l.only_bare_remote then 'Remote (region unknown)'
        end                                                                   as location_match,
        coalesce(s.matched_skills, [])                                        as matched_skills,
        coalesce(s.missing_required_skills, [])                               as missing_required_skills,
        s.skill_score,
        case
            when p.years_min is null then null
            when p.years_min <= st.years_experience then 1.0
            else greatest(0.0, 1.0 - st.years_penalty_per_year * (p.years_min - st.years_experience))
        end                                                                   as experience_score,
        lf.fit                                                                as level_score,
        a.as_of_date,
        a.as_of_date - p.first_seen_date                                      as days_since_first_seen,
        power(0.5, (a.as_of_date - p.first_seen_date) / st.freshness_half_life_days) as freshness_score,
        st.*
    from current_postings as p
    join titles as t using (posting_version_key)
    join locations as l using (posting_version_key)
    left join skills as s using (posting_version_key)
    left join {{ ref('profile_level_fit') }} as lf using (seniority)
    cross join settings as st
    cross join as_of as a
)

select
    posting_version_key,
    posting_key,
    source,
    company_slug,
    title,
    seniority,
    years_min,
    years_max,
    location_raw,
    locations_raw,
    workplace_type,
    posting_url,
    first_seen_date,
    days_since_first_seen,
    as_of_date,
    passes_title,
    passes_location,
    title_includes,
    title_excludes,
    location_match,
    matched_skills,
    missing_required_skills,
    skill_score,
    experience_score,
    level_score,
    freshness_score,
    round(100 * (
          weight_skill      * coalesce(skill_score,      unknown_part_score)
        + weight_experience * coalesce(experience_score, unknown_part_score)
        + weight_level      * coalesce(level_score,      unknown_part_score)
        + weight_freshness  * freshness_score
    ) / (weight_skill + weight_experience + weight_level + weight_freshness), 1)      as score,
    -- One-line "why" for the ranked list.
    concat_ws(' · ',
        case when skill_score is null then 'skills: none found'
             else 'skills ' || round(100 * skill_score)::int || '%'
                  || case when len(missing_required_skills) > 0
                          then ' (missing ' || array_to_string(missing_required_skills, ', ') || ')' else '' end
        end,
        case when years_min is null then 'years: not stated'
             else years_min || '+ yrs' || case when years_min > years_experience then ' (over by ' || (years_min - years_experience)::int || ')' else '' end
        end,
        seniority,
        'first seen ' || days_since_first_seen || 'd ago'
    )                                                                         as why
from parts
