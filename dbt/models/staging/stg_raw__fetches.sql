-- One row per successful fetch (source, company, dt), from _metadata.json.
-- The ingest job only writes a partition when the fetch succeeded, so a
-- missing row here means "we don't know what was open that day" (ADR-014).
select
    source,
    company                                    as company_slug,
    dt,
    json ->> '$.run_id'                        as run_id,
    (json ->> '$.fetched_at')::timestamptz     as fetched_at,
    (json ->> '$.http_status')::int            as http_status,
    (json ->> '$.n_postings')::int             as n_postings,
    json ->> '$.sha256'                        as response_sha256
from {{ read_raw('*', file_name='_metadata.json') }}
