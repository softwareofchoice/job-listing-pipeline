{#
  Generic test: no two rows share the same values in `columns`.
  (The same idea as dbt_utils.unique_combination_of_columns, written out here
  to avoid a package dependency for one test.)
#}
{% test unique_combination_of_columns(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
