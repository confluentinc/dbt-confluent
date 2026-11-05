{% macro confluent__get_replace_view_sql(relation, sql) -%}
  {# Flink has no CREATE OR REPLACE VIEW, but ALTER VIEW ... AS replaces an
     existing view's query in place, and a statement the server rejects leaves
     the view as it was. Mirrors default__create_view_as, minus the contract
     check: the view materialization runs that before any DDL. #}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  alter view {{ relation.render() }} as (
    {{ sql }}
  );
{%- endmacro %}
