{{ config(severity='warn', warn_if='>0') }}
-- Section detection is heuristic (ADR-016). Warn if, for any provider, fewer
-- than 80% of posting versions have a recognized 'required' section: a
-- company probably uses heading wording we don't recognize yet.
select
    split_part(posting_key, ':', 1) as source,
    count(distinct posting_version_key) as n_versions,
    count(distinct posting_version_key) filter (where section = 'required') as n_with_required
from {{ ref('int_posting_lines') }}
group by 1
having n_with_required < 0.8 * n_versions
