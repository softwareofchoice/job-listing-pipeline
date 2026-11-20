{{ config(materialized='table') }}
{#
  Every posting version's title + description split into lines, each tagged
  with the section it belongs to (ADR-016). Skills and years are extracted
  from these lines, so they inherit the section.

  Stage 2 kept the line breaks, so a heading like "Preferred qualifications"
  sits on its own line. A line counts as a HEADING if it's short (<= 5 words,
  or <= 10 words ending in ':') and matches one of the patterns below. Every
  following line belongs to that section until the next heading.

  Order matters: 'preferred' is checked before 'required', because
  "Preferred qualifications" contains "qualification".
#}
{% set heading = {
    'preferred':        "prefer|nice[- ]?to[- ]?haves?|bonus|a plus|pluses|desired|good to have|ideally|stand out|even better|extra credit",
    'required':         "require|qualification|must[- ]?haves?|who you are|about you|what you('ll| will)? (need|bring|have)|you (have|bring|should have)|skills (you|we)|looking for|look for|your expertise|you('ll| will) bring|you might be|ideal candidate|your (background|experience|skills|profile)|^(experience|skills|competencies):?$",
    'responsibilities': "responsib|what you('ll| will)? (do|be doing|achieve|work on|own)|the role|your role|day[- ]to[- ]day|in this role|your impact|duties|the opportunity|the job|team overview|impact you('ll| will) have|a typical day|difference you('ll| will) make",
    'other':            "benefit|perks|about (us|the company|the team)|compensation|salary|\\bpay\\b|who we are|our (team|company|mission|values|culture)|why (join|work)|equal opportunity|\\beeo|what we offer|we offer|how we work|life at|hiring process|interview|accommodation|privacy|\\bat [a-z]+, we|^about [a-z]+:?$|our commitment|^compliance|take care of you|community you('ll| will) join|your location",
} %}
{# Inline cue: "Kafka is a plus", "Spark experience preferred". #}
{% set preferred_cue = "\\b(preferred|preferably|preferable|nice[- ]to[- ]have|a plus|is a plus|are a plus|bonus|desired|ideally)\\b" %}

with versions as (
    select
        posting_key || ':v' || version_number as posting_version_key,
        posting_key,
        version_number,
        company_slug,
        title,
        description_text
    from {{ ref('int_postings__scd2') }}
),

-- Line 0 is the title. Lines 1..n are the description, in order.
lines as (
    select
        posting_version_key, posting_key, version_number, company_slug,
        0          as line_no,
        title      as line_text,
        true       as is_title
    from versions
    union all
    select
        posting_version_key, posting_key, version_number, company_slug,
        ln.i       as line_no,
        ln.t       as line_text,
        false      as is_title
    from (
        select
            *,
            unnest(list_transform(string_split(description_text, chr(10)), lambda t, i: {'i': i, 't': t})) as ln
        from versions
        where description_text is not null
    )
),

classified as (
    select
        *,
        lower(trim(line_text))                                       as line_lower,
        len(string_split(trim(line_text), ' '))                      as n_words
    from lines
    where trim(line_text) <> ''
),

headings as (
    select
        *,
        case
            when not is_title
             and (n_words <= 5 or (n_words <= 10 and line_lower like '%:'))
            then case
                {% for section, pattern in heading.items() %}
                when regexp_matches(line_lower, '{{ pattern | replace("'", "''") }}') then '{{ section }}'
                {% endfor %}
            end
        end as heading_section
    from classified
)

select
    posting_version_key,
    posting_key,
    version_number,
    company_slug,
    line_no,
    line_text,
    line_lower,
    is_title,
    heading_section is not null                                       as is_heading,
    case
        when is_title then 'title'
        else coalesce(
            last_value(heading_section ignore nulls) over (
                partition by posting_version_key order by line_no
                rows between unbounded preceding and current row),
            'intro')                                                  -- before any heading
    end                                                               as section,
    -- What a mention on this line implies for the candidate:
    --   title / required section -> required
    --   preferred section or an inline "is a plus" -> preferred
    --   everything else (intro, responsibilities, unrecognized) -> unspecified
    case
        when not is_title and regexp_matches(line_lower, '{{ preferred_cue }}') then 'preferred'
        when is_title then 'required'
        when section = 'required' then 'required'
        when section = 'preferred' then 'preferred'
        else 'unspecified'
    end                                                               as requirement_level
from headings
