{#
  Generic test: every non-null value of the column is between min and max
  (inclusive). Written here instead of adding the dbt_utils package for one test.
#}
{% test between(model, column_name, min_value, max_value) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}
