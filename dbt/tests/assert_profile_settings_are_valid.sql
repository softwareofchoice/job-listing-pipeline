-- profile_settings must be exactly one row with sensible values. A second
-- row would silently duplicate every score (it's cross-joined).
with s as (select * from {{ ref('profile_settings') }})
select 'expected exactly 1 row, found ' || count(*) as problem from s having count(*) <> 1
union all
select 'weights must be >= 0 and not all zero' from s
where least(weight_skill, weight_experience, weight_level, weight_freshness) < 0
   or weight_skill + weight_experience + weight_level + weight_freshness = 0
union all
select 'freshness_half_life_days must be > 0' from s where freshness_half_life_days <= 0
union all
select 'unknown_part_score must be between 0 and 1' from s where unknown_part_score not between 0 and 1
