{#
  Convert job-description HTML to plain text in SQL (ADR-012).

  Steps:
    1. (double_escaped only) Undo the outer escaping layer. Greenhouse returns
       '&lt;p&gt;Tom &amp;amp; Jerry&lt;/p&gt;', i.e. HTML escaped as text.
       Decoding &amp; LAST matters: '&amp;lt;' must become '&lt;' (literal
       text), not '<' (a tag).
    2. Turn block-level closing tags and <br> into newlines, so paragraphs and
       list items don't run together ("Python</li><li>SQL" -> "Python\nSQL").
    3. Remove all remaining tags.
    4. Decode common entities (again &amp; last).
    5. Normalize whitespace.

  This is "good enough" cleaning for keyword matching, not a full HTML parser.
  Rare entities (e.g. &#8217;) may survive; a warn-level test tracks how often.
#}
{% macro decode_basic_entities(expr) -%}
replace(replace(replace(replace(replace(
    {{ expr }},
    '&lt;', '<'), '&gt;', '>'), '&quot;', '"'), '&#39;', ''''), '&amp;', '&')
{%- endmacro %}

{% macro decode_entities(expr) -%}
{{ decode_basic_entities(
    "replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace("
    ~ expr ~
    ", '&nbsp;', ' '), '&mdash;', '—'), '&ndash;', '–'), '&rsquo;', '’'), '&lsquo;', '‘')"
    ~ ", '&ldquo;', '“'), '&rdquo;', '”'), '&hellip;', '…'), '&bull;', '•'), '&#x27;', ''''), '&#8217;', '’')"
) }}
{%- endmacro %}

{# Collapse runs of spaces (incl. non-breaking, chr(160)) and blank lines; '' -> null. #}
{% macro normalize_whitespace(expr) -%}
nullif(
    trim(
        regexp_replace(
            regexp_replace({{ expr }}, '[ \t\r' || chr(160) || ']+', ' ', 'g'),  -- collapse spaces
            '\s*\n\s*', chr(10), 'g'),                                          -- collapse blank lines
        ' ' || chr(10)),  -- trim() strips only spaces by default; strip newlines too
    '')
{%- endmacro %}

{% macro html_to_text(expr, double_escaped=false) -%}
{%- set html = decode_basic_entities(expr) if double_escaped else expr -%}
{{ normalize_whitespace(decode_entities(
    "regexp_replace(regexp_replace(" ~ html ~ ", '<\\s*(br|/p|/div|/li|/h[1-6]|/tr)[^>]*>', chr(10), 'gi'), '<[^>]*>', ' ', 'g')"
)) }}
{%- endmacro %}
