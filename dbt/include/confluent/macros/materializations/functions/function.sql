{# dbt hard-codes a function node's `language` to 'sql'; the real UDF language
   (java/python) is the `language` *config*, validated by the adapter. #}
{%- materialization function, adapter='confluent', supported_languages=['sql'] -%}
  {% do adapter.require_function_materialization_enabled() %}
  {% do validate_materialization_config() %}
  {%- set target_relation = this.incorporate(type='function') -%}
  {%- set udf = adapter.validate_function_config(config, target_relation) -%}
  {#- The live function (None if absent), described once and reused below. Live lookup, so skipped
     at parse/compile time (`execute` is false), where None just means "create". -#}
  {%- set existing = adapter.describe_function(target_relation, config.get('compute_pool_id')) if execute else none -%}
  {% if execute %}{% do adapter.require_artifact_credentials(udf, existing) %}{% endif %}
  {#- None if absent; else the differences from the config (empty if identical). -#}
  {%- set changes = adapter.plan_function_change(existing, udf) if execute else none -%}
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

  {#- Confluent UDFs are immutable (no ALTER / CREATE OR REPLACE), so a change is a drop + create.
     The adapter puts the old function back if the new one can't be created. -#}
  {% if plan.action == 'replace' %}
    {% do adapter.warn_function_change(plan.message) %}
  {% endif %}
  {%- set response = adapter.apply_function(
        target_relation,
        udf,
        existing,
        config.get('compute_pool_id'),
        get_statement_name('-' ~ invocation_id),
        get_statement_name('-drop-' ~ invocation_id),
      ) if execute else adapter.noop_response('Function not applied') -%}
  {% do store_result('main', response=response, agate_table=none) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization -%}
