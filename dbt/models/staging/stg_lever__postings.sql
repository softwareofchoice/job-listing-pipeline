-- One row per Lever posting per fetch day.
-- Raw shape: a bare JSON array, so format='array' yields one row per posting.
with postings as (
    select source, company, dt, json as j
    from {{ read_raw('lever', format='array') }}
),

with_text as (
    select
        *,
        -- descriptionPlain leaves out the "lists" (requirements, responsibilities),
        -- which is where most skills are mentioned, so stitch them back in.
        concat_ws(chr(10),
            j ->> '$.descriptionPlain',
            array_to_string(list_transform(
                json_extract(j, '$.lists[*]'),
                lambda l: concat_ws(chr(10), l ->> '$.text', {{ html_to_text("l ->> '$.content'") }})
            ), chr(10)),
            j ->> '$.additionalPlain'
        ) as full_text
    from postings
)

select
    source || ':' || company || ':' || (j ->> '$.id')              as posting_key,
    source,
    company                                                        as company_slug,
    j ->> '$.id'                                                   as posting_id,
    dt,
    trim(j ->> '$.text')                                           as title,
    j ->> '$.categories.department'                                as department,
    j ->> '$.categories.team'                                      as team,
    j ->> '$.categories.location'                                  as location_raw,
    coalesce(
        list_sort(list_distinct(list_filter(list_transform(json_extract_string(j, '$.categories.allLocations[*]'), lambda x: nullif(trim(x), '')), lambda x: x is not null))),
        list_filter([j ->> '$.categories.location'], lambda x: x is not null)
    )                                                              as locations_raw,
    lower(nullif(j ->> '$.workplaceType', 'unspecified'))          as workplace_type,
    j ->> '$.categories.commitment'                                as employment_type,
    -- Lever timestamps are epoch milliseconds
    to_timestamp((j ->> '$.createdAt')::bigint / 1000)             as published_at,
    null::timestamptz                                              as source_updated_at,
    j ->> '$.hostedUrl'                                            as posting_url,
    {{ normalize_whitespace('full_text') }}                        as description_text
from with_text
