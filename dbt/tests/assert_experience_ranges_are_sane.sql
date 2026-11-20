-- years_hi never below years_lo, and nothing absurd (ADR-017 caps at 25).
select *
from {{ ref('int_posting_experience') }}
where years_hi < years_lo or years_lo < 0 or years_hi > 25
