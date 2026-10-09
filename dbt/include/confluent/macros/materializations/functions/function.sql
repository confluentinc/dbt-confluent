{# dbt hard-codes a function node's `language` to 'sql'; the real UDF language
   (java/python) is the `language` *config*, validated by the adapter. #}
{%- materialization function, adapter='confluent', supported_languages=['sql'] -%}
  {% do adapter.require_function_materialization_enabled() %}
  {% do validate_materialization_config() %}
  {%- set target_relation = this.incorporate(type='function') -%}
  {%- set udf = adapter.validate_function_config(config, target_relation) -%}
  {#- None if absent; else the differences from the config (empty if identical). Live lookup,
     so skipped at parse/compile time (`execute` is false), where None just means "create". -#}
  {%- set changes = adapter.plan_function_change(target_relation, udf, config.get('compute_pool_id')) if execute else none -%}
  {#- What to do about them, per dbt's `on_configuration_change` (apply | continue | fail),
     unless --full-refresh, which always replaces an existing function. -#}
  {%- set plan = adapter.plan_function_action(target_relation, changes, config.get('on_configuration_change'), should_full_refresh()) -%}

  {% if plan.action == 'fail' %}
    {% do exceptions.raise_compiler_error(plan.message) %}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% if plan.action in ('unchanged', 'keep') %}
    {# Nothing to submit, but dbt's function runner reads the 'main' result. #}
    {% if plan.message %}{% do adapter.warn_function_change(plan.message) %}{% endif %}
    {% do store_result('main', response=adapter.noop_response('Function unchanged'), agate_table=none) %}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {{ return({'relations': [target_relation]}) }}
  {% endif %}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# Confluent UDFs are immutable (no ALTER / CREATE OR REPLACE), so a change is a drop + create. #}
  {% if plan.action == 'replace' %}
    {% do adapter.warn_function_change(plan.message) %}
    {% call statement('drop_function', statement_name=get_statement_name('-drop')) -%}
      drop function {{ target_relation.render() }}
    {%- endcall %}
  {% endif %}
  {% call statement('main', statement_name=get_statement_name()) -%}
    {{ get_create_function_sql(target_relation, udf) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization -%}
