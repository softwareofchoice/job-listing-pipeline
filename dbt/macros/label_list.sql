{#
  Which labelled regex patterns does `expr` match? Returns a list of labels.

  rows: (..., label, pattern) tuples read from a seed at compile time, so each
  pattern becomes a SQL constant and its regex is compiled once (see
  int_posting_skills for why that matters). Single quotes are escaped.

  Example: label_list([('Chicago', 'chicago')], 'loc')
    -> list_filter([case when regexp_matches(loc, 'chicago') then 'Chicago' end], ...)
#}
{% macro label_list(rows, expr) -%}
list_filter([
    {%- for r in rows %}
    case when regexp_matches({{ expr }}, '{{ r[-1] | replace("'", "''") }}') then '{{ r[-2] }}' end{{ "," if not loop.last }}
    {%- endfor %}
], lambda x: x is not null)
{%- endmacro %}
