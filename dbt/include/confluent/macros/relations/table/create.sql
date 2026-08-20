{% macro confluent__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  {# Flink SQL does not support TEMPORARY tables, so we ignore the temporary parameter
     and create a regular table with a temporary name instead #}
  create table
    {{ relation }}
  {%- set contract = render_contract_columns_and_reproject_sql(sql) -%}
  {{ contract.ddl }}
  {%- set sql = contract.sql %}
  {{ get_distributed_by_clause() }}
  as (
    {{ sql }}
  );
{%- endmacro %}

