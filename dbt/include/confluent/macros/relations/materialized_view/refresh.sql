{% macro confluent__refresh_materialized_view(relation) %}
  {# In Confluent Flink, CTAS tables are continuously updated by streaming
     queries - there's no manual refresh needed. Return empty string (no-op). #}
  {{ return('') }}
{% endmacro %}
