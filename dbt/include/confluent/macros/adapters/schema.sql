{% macro confluent__create_schema(relation) -%}
  {% set msg %}
    This adapter (confluent) does not support CREATE for schemas.
  {% endset %}
  {% do exceptions.raise_compiler_error(msg) %}
{% endmacro %}

{% macro confluent__drop_schema(relation) -%}
  {% set msg %}
    This adapter (confluent) does not support DROP for schemas.
  {% endset %}
  {% do exceptions.raise_compiler_error(msg) %}
{% endmacro %}

