{# dbt hard-codes a function node's `language` to 'sql'; the real UDF language
   (java/python) is the `language` *config*, validated by the adapter. #}
{%- materialization function, adapter='confluent', supported_languages=['sql'] -%}
  {%- set udf = adapter.validate_function_config(config) -%}
  {%- set target_relation = this.incorporate(type='function') -%}

  {{ run_hooks(pre_hooks) }}

  {# Confluent UDFs are immutable (no ALTER / CREATE OR REPLACE), so this only creates. #}
  {% call statement('main') -%}
    {{ get_create_function_sql(target_relation, udf) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization -%}
