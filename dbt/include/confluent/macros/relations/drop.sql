{% macro confluent__drop_relation(relation) -%}
  {% if relation.is_table or relation.is_materialized_view -%}
    {% call statement('drop_relation', fetch_result=False) -%}
      DROP TABLE IF EXISTS {{ relation }}
    {% endcall %}
  {%- elif relation.is_view -%}
    {% call statement('drop_relation', fetch_result=False) -%}
      DROP VIEW IF EXISTS {{ relation }}
    {% endcall %}
  {%- else -%}
    {% set msg %}
      This adapter (confluent) does not support DROP for {{ relation }}.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {%- endif -%}
{% endmacro %}

{% macro confluent__get_drop_sql(relation) -%}
    {%- if relation.is_view -%}
        drop view if exists {{ relation.render() }}
    {%- elif relation.is_table or relation.is_materialized_view -%}
        drop table if exists {{ relation.render() }}
    {%- else -%}
        drop {{ relation.type }} if exists {{ relation.render() }}
    {%- endif -%}
{%- endmacro %}

