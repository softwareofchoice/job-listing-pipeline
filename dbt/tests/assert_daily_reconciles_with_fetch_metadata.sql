-- Reconciliation: the postings staging produced for each board and day must
-- equal the count the ingest job recorded when it fetched the response.
-- Catches a staging model silently dropping or duplicating rows.
with staged as (
    select source, company_slug, dt, count(*) as n_staged
    from {{ ref('int_postings__daily') }}
    group by all
)
select f.source, f.company_slug, f.dt, f.n_postings, coalesce(s.n_staged, 0) as n_staged
from {{ ref('stg_raw__fetches') }} as f
left join staged as s using (source, company_slug, dt)
where f.n_postings <> coalesce(s.n_staged, 0)
