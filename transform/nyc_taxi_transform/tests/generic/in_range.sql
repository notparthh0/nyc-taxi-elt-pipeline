{#
  Fails for every row where the column is outside [min_value, max_value].
  Nulls are ignored here; pair with not_null when nulls are not allowed.
#}
{% test in_range(model, column_name, min_value=none, max_value=none) %}

select {{ column_name }}
from {{ model }}
where
    {% if min_value is not none and max_value is not none %}
        {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
    {% elif min_value is not none %}
        {{ column_name }} < {{ min_value }}
    {% else %}
        {{ column_name }} > {{ max_value }}
    {% endif %}

{% endtest %}
