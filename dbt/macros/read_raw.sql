{#
  Table function over the raw layer (ADR-002, ADR-010, ADR-011).

  read_json_objects returns each JSON value as an untyped `json` column instead
  of guessing a schema. That's deliberate: the three APIs add, drop and change
  fields over time, and schema inference across hundreds of files can fail or
  silently pick the wrong type. Staging models pull out exactly the fields
  they need, with explicit types ("schema on read").

  hive_partitioning=true turns source=/dt=/company= folder names into columns.

  format: 'auto'  -> one row per file (Greenhouse/Ashby: {"jobs": [...]})
          'array' -> one row per array element (Lever: bare [...])
#}
{% macro read_raw(source_name, file_name='response.json.gz', format='auto') %}
read_json_objects(
    '{{ var("raw_root") }}/source={{ source_name }}/dt=*/company=*/{{ file_name }}',
    format = '{{ format }}',
    hive_partitioning = true,
    maximum_object_size = {{ var("max_json_bytes") }}
)
{% endmacro %}
