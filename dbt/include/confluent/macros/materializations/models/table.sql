{% materialization table, adapter='confluent' %}
  {# Pure config validation first, before load_cached_relation (which can be a
     real INFORMATION_SCHEMA round-trip if this schema's relation cache isn't
     already warm) -- neither validator depends on the relation, so a config
     mistake should fail before any warehouse I/O, not after. #}
  {% do validate_materialization_config() %}
  {%- do adapter.validate_distributed_by_config(config.get('distributed_by')) -%}
  {%- do adapter.validate_tableflow_config(config.get('tableflow')) -%}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% if decide_action(existing_relation) == 'skip' %}
    {# dbt requires a 'main' statement result even when skipping #}
    {% call noop_statement('main', 'SKIP') %}{% endcall %}
    {{ ensure_tableflow_config(target_relation) }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {{ return({'relations': [target_relation]}) }}
  {% endif %}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main', execution_mode="snapshot_ddl",
                    statement_name=get_statement_name()) -%}
    {{ get_create_table_as_sql(False, target_relation, sql) }}
  {%- endcall %}

  {{ ensure_tableflow_config(target_relation) }}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
