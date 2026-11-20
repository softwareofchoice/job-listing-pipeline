-- Structural rules every SCD2 table must obey:
--   * a version never ends before it starts
--   * at most one current version per posting
--   * versions of a posting don't overlap
--   * a 'changed' version is followed by the next version on the same day
--     (no gap), while a 'closed' one leaves a gap or ends the history
--   * end_reason is null exactly when the version is current
with v as (
    select
        *,
        lead(valid_from) over (partition by posting_key order by version_number) as next_valid_from
    from {{ ref('int_postings__scd2') }}
)
select posting_key, version_number, 'ends before it starts' as problem from v where valid_to <= valid_from
union all
select posting_key, null, 'more than one current version' from v where is_current group by posting_key having count(*) > 1
union all
select posting_key, version_number, 'overlaps next version' from v where next_valid_from < valid_to
union all
select posting_key, version_number, 'changed but next version does not start at valid_to'
from v where end_reason = 'changed' and next_valid_from is distinct from valid_to
union all
select posting_key, version_number, 'end_reason inconsistent with is_current'
from v where is_current <> (end_reason is null)
