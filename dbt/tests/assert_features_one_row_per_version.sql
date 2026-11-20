-- Every SCD2 version gets exactly one features row: nothing dropped by a join.
select
    (select count(*) from {{ ref('int_postings__scd2') }})   as n_versions,
    (select count(*) from {{ ref('int_posting_features') }}) as n_features
where n_versions <> n_features
