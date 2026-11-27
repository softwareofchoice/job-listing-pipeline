-- Example Athena queries over the curated tables (Stage 5c, ADR-034).
-- Run them in the Athena console with workgroup "jobpipe" and database
-- "jobpipe", or from the CLI:
--
--   aws athena start-query-execution --work-group jobpipe \
--     --query-execution-context Database=jobpipe \
--     --query-string "select dt, count(*) from top_matches group by dt"
--
-- Always filter on dt when you can: it's the partition key, so Athena only
-- reads that day's files. dt is a string: dt = '2026-11-26'.

-- 1. Today's ranked list (latest snapshot).
select rank, score, company_slug, title, location_match, why
from top_matches
where dt = (select max(dt) from top_matches)
order by rank;

-- 2. How many postings passed both filters each day.
select dt,
       count(*)                                          as current_postings,
       count_if(passes_title and passes_location)        as matches
from posting_scores
group by dt
order by dt;

-- 3. Skills most often REQUIRED across all current postings (any job function).
select skill, count(*) as postings
from posting_features
cross join unnest(skills_required) as t(skill)
where dt = (select max(dt) from posting_features)
  and is_current
group by skill
order by postings desc
limit 20;

-- 4. Postings that closed in the last 7 days, and how long they were open.
select company_slug, title, valid_from, valid_to,
       date_diff('day', valid_from, valid_to) as days_open
from postings_history
where dt = (select max(dt) from postings_history)
  and end_reason = 'closed'
  and valid_to >= date_add('day', -7, current_date)
order by valid_to desc;

-- 5. A posting's score over time (one row per daily snapshot).
select dt, score, skill_score, freshness_score, why
from posting_scores
where posting_key = 'lever:spotify:ced2c220-6b5c-4e74-8e06-37d50985097c'
order by dt;
