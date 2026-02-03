{% macro confluent__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  {# Flink SQL does not support TEMPORARY tables, so we ignore the temporary parameter
     and create a regular table with a temporary name instead #}
  create table
    {{ relation }}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
    {{ get_table_columns_and_constraints() }}
    {%- set sql = get_select_subquery(sql) %}
  {% endif %}
  as (
    {{ sql }}
  );
{%- endmacro %}

