{% macro confluent__get_create_materialized_view_as_sql(relation, sql) -%}
  {# In Confluent Flink SQL, materialized views are implemented as tables
     created via CTAS (CREATE TABLE AS SELECT). These tables are continuously
     updated by streaming queries, making them functionally equivalent to
     materialized views in other systems. #}
  {{ return(create_table_as(False, relation, sql)) }}
{%- endmacro %}
