{% macro confluent__truncate_relation(relation) -%}
  {% set msg %}
    This adapter (confluent) does not support TRUNCATE. Relation: {{ relation }}.
    You might need to set `full_refresh` to true.
  {% endset %}
  {% do exceptions.raise_compiler_error(msg) %}
{% endmacro %}

