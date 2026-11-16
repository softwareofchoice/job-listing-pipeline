{{ config(severity='warn', warn_if='>0') }}
-- html_to_text is best-effort (ADR-012). Warn (don't fail) when entities or
-- tags survive cleaning, so we notice if a provider changes its HTML.
select posting_key, dt
from {{ ref('int_postings__daily') }}
where regexp_matches(description_text, '&[a-zA-Z#0-9]+;|</?[a-zA-Z][^>]*>')
