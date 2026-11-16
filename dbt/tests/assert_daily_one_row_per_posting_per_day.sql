-- A posting appears at most once per fetch day. Duplicates would create
-- false "changes" in SCD2.
select posting_key, dt, count(*) as n
from {{ ref('int_postings__daily') }}
group by all
having count(*) > 1
