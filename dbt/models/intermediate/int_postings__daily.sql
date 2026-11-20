-- All providers in one schema: one row per posting per successful fetch day.
-- This is the "which postings were open on which day" input for SCD2.
{% set providers = ['greenhouse', 'lever', 'ashby'] %}
{% for p in providers %}
select * from {{ ref('stg_' ~ p ~ '__postings') }}
{% if not loop.last %}union all by name{% endif %}
{% endfor %}
