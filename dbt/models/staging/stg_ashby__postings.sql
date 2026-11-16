-- One row per Ashby posting per fetch day.
-- Raw shape: one file per board, {"jobs": [...], "apiVersion": "..."}
with responses as (
    select source, company, dt, json
    from {{ read_raw('ashby') }}
),

postings as (
    select source, company, dt, unnest(json_extract(json, '$.jobs[*]')) as j
    from responses
)

select
    source || ':' || company || ':' || (j ->> '$.id')        as posting_key,
    source,
    company                                                  as company_slug,
    j ->> '$.id'                                             as posting_id,
    dt,
    trim(j ->> '$.title')                                    as title,
    j ->> '$.department'                                     as department,
    j ->> '$.team'                                           as team,
    j ->> '$.location'                                       as location_raw,
    list_sort(list_distinct(list_filter(list_transform(
        [j ->> '$.location'] || json_extract_string(j, '$.secondaryLocations[*].location'), lambda x: nullif(trim(x), '')),
        lambda x: x is not null
    )))                                                        as locations_raw,
    lower(j ->> '$.workplaceType')                           as workplace_type,
    j ->> '$.employmentType'                                 as employment_type,
    (j ->> '$.publishedAt')::timestamptz                     as published_at,
    null::timestamptz                                        as source_updated_at,
    j ->> '$.jobUrl'                                         as posting_url,
    -- Ashby provides a plain-text version; fall back to cleaning the HTML.
    coalesce(
        {{ normalize_whitespace("j ->> '$.descriptionPlain'") }},
        {{ html_to_text("j ->> '$.descriptionHtml'") }}
    )                                                        as description_text
from postings
