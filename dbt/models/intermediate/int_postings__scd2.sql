{{ config(materialized='table') }}
{#
  SCD Type 2 history of every posting (ADR-013, ADR-014).

  One row per VERSION of a posting: a stretch of days during which the posting
  was open and its tracked attributes didn't change.

    valid_from   first fetch day this version was seen (inclusive)
    valid_to     first fetch day it was NOT this version anymore (exclusive);
                 null while it's still current
    end_reason   why the version ended: 'changed' (edited) or 'closed'
                 (missing from a successful fetch of its board); null if current

  Built from scratch from all raw partitions on every run, so history can be
  rebuilt or backfilled at any time (ADR-002). The core trick: compare each
  posting's state on one successful fetch day with the previous successful
  fetch day of the same board.
#}

-- Columns that define a "change". Deliberately NOT included:
--   source_updated_at  Greenhouse bumps it without visible edits (noise)
--   published_at       an attribute of the posting, not something we version on
{% set tracked = ['title', 'department', 'team', 'location_raw', 'locations_raw',
                  'workplace_type', 'employment_type', 'description_text'] %}

with daily as (
    select
        *,
        md5(concat_ws('|' {%- for c in tracked %}, coalesce({{ c }}::varchar, '∅'){% endfor %})) as attributes_hash
    from {{ ref('int_postings__daily') }}
),

-- Days on which each board was fetched successfully. On a day with no
-- successful fetch we don't know what was open, so that day is skipped
-- rather than treated as "everything closed" (ADR-014).
good_fetches as (
    select distinct source, company_slug, dt
    from {{ ref('stg_raw__fetches') }}
    where http_status = 200
),

first_seen as (
    select posting_key, source, company_slug, min(dt) as first_dt
    from daily
    group by all
),

-- For every posting: one row per successful fetch of its board since it was
-- first seen, with its attributes hash if present that day, else null.
timeline as (
    select
        f.posting_key,
        g.dt,
        d.attributes_hash
    from first_seen as f
    join good_fetches as g
      on g.source = f.source
     and g.company_slug = f.company_slug
     and g.dt >= f.first_dt
    left join daily as d
      on d.posting_key = f.posting_key
     and d.dt = g.dt
),

-- "State" is the attributes hash, or 'CLOSED' if absent from that fetch.
-- A version starts wherever the state differs from the previous fetch day.
state_changes as (
    select posting_key, dt, state
    from (
        select
            posting_key,
            dt,
            coalesce(attributes_hash, 'CLOSED') as state,
            lag(coalesce(attributes_hash, 'CLOSED')) over (partition by posting_key order by dt) as prev_state
        from timeline
    )
    where prev_state is null or state <> prev_state
),

-- Each state change runs until the next one.
versions as (
    select
        posting_key,
        dt                                                             as valid_from,
        state,
        lead(dt)    over (partition by posting_key order by dt)        as valid_to,
        lead(state) over (partition by posting_key order by dt)        as next_state
    from state_changes
)

-- Keep only "open" versions. CLOSED stretches are gaps between versions; if
-- a posting comes back, it simply gets a new version.
select
    v.posting_key,
    d.source,
    d.company_slug,
    d.posting_id,
    row_number() over (partition by v.posting_key order by v.valid_from)  as version_number,
    v.valid_from,
    v.valid_to,
    v.valid_to is null                                                    as is_current,
    case
        when v.valid_to is null then null
        when v.next_state = 'CLOSED' then 'closed'
        else 'changed'
    end                                                                   as end_reason,
    d.attributes_hash,
    {% for c in tracked %}d.{{ c }},
    {% endfor -%}
    d.published_at,
    d.posting_url
from versions as v
join daily as d
  on d.posting_key = v.posting_key
 and d.dt = v.valid_from
where v.state <> 'CLOSED'
