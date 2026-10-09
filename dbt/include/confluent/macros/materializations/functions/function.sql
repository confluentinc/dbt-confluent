{# dbt hard-codes a function node's `language` to 'sql'; the real UDF language
   (java/python) is the `language` *config*, validated by the adapter. #}
{%- materialization function, adapter='confluent', supported_languages=['sql'] -%}
  {%- set target_relation = this.incorporate(type='function') -%}
  {%- set udf = adapter.validate_function_config(config, target_relation) -%}
  {#- None if absent; else the differences from the config (empty if identical). -#}
  {%- set changes = adapter.plan_function_change(target_relation, udf) -%}
  {#- What to do about them, per dbt's `on_configuration_change` (apply | continue | fail). -#}
  {%- set plan = adapter.plan_function_action(target_relation, changes, config.get('on_configuration_change')) -%}

  {% if plan.action == 'fail' %}
    {% do exceptions.raise_fail_fast_error(plan.message) %}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  {% if plan.action in ('unchanged', 'keep') %}
    {# Nothing to submit, but dbt's function runner reads the 'main' result. #}
    {% if plan.message %}{% do adapter.warn_function_change(plan.message) %}{% endif %}
    {% do store_result('main', response=adapter.noop_response('Function unchanged'), agate_table=none) %}
  {% else %}
    {# Confluent UDFs are immutable (no ALTER / CREATE OR REPLACE), so a change is a drop + create. #}
    {% if plan.action == 'replace' %}
      {% do adapter.warn_function_change(plan.message) %}
      {% call statement('drop_function') -%}
        drop function {{ target_relation.render() }}
      {%- endcall %}
    {% endif %}
    {% call statement('main') -%}
      {{ get_create_function_sql(target_relation, udf) }}
    {%- endcall %}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization -%}
