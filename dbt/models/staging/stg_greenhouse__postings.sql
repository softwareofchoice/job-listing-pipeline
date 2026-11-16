-- One row per Greenhouse posting per fetch day.
-- Raw shape: one file per board, {"jobs": [...], "meta": {...}}
with responses as (
    select source, company, dt, json
    from {{ read_raw('greenhouse') }}
),

postings as (
    select source, company, dt, unnest(json_extract(json, '$.jobs[*]')) as j
    from responses
)

select
    source || ':' || company || ':' || (j ->> '$.id')            as posting_key,
    source,
    company                                                      as company_slug,
    j ->> '$.id'                                                 as posting_id,
    dt,
    trim(j ->> '$.title')                                        as title,
    j ->> '$.departments[0].name'                                as department,
    null::varchar                                                as team,
    j ->> '$.location.name'                                      as location_raw,
    list_sort(list_distinct(list_filter(list_transform(
        [j ->> '$.location.name'] || json_extract_string(j, '$.offices[*].name'), lambda x: nullif(trim(x), '')),
        lambda x: x is not null
    )))                                                            as locations_raw,
    null::varchar                                                as workplace_type,
    null::varchar                                                as employment_type,
    (j ->> '$.first_published')::timestamptz                     as published_at,
    (j ->> '$.updated_at')::timestamptz                          as source_updated_at,
    j ->> '$.absolute_url'                                       as posting_url,
    {{ html_to_text("j ->> '$.content'", double_escaped=true) }} as description_text
from postings
