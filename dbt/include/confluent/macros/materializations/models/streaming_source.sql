{% materialization streaming_source, adapter='confluent' %}
  {# Pure config validation first, before load_cached_relation (which can be a
     real INFORMATION_SCHEMA round-trip if this schema's relation cache isn't
     already warm) -- it doesn't depend on the relation, so a config mistake
     should fail before any warehouse I/O, not after. #}
  {% do validate_materialization_config() %}
  {%- do adapter.validate_distributed_by_config(config.get('distributed_by')) -%}
  {%- do adapter.validate_tableflow_config(config.get('tableflow')) -%}

  -- Check if the relation exists already, and precreate the target_relation
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type=this.Table) %}

  -- The `connector` config is mandatory, to force the
  -- creation of a streaming table that won't get deleted if it's not polled.
  {%- set connector = config.get('connector') -%}
  {% if not connector %}
    {% set msg="'connector' must be specified in 'streaming_source' materialization" %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}
  {# `connector` is just another WITH option in the DDL. Merging it into a
     copy of the options dict (mandatory config wins) lets the shared renderer
     emit the whole clause, and makes a user-supplied 'connector' key in
     `with` harmless instead of a duplicate-key DDL error. #}
  {%- set with_options = {} -%}
  {%- do with_options.update(config.get('with', {})) -%}
  {%- do with_options.update({'connector': connector}) -%}

  -- Run hooks like in the original materializations, so we don't
  -- break any assumption made by the framework.
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- TODO: Support altering table options without full refresh (ALTER TABLE ... SET).
  {# Recovery is not safe for streaming_source: the CREATE statement also
     attaches the connector, and Flink doesn't support re-attaching to an
     existing table. Pass recoverable=false so a dead connector statement
     surfaces as a SKIP (with a log line from decide_action) — user must
     run --full-refresh. Tracked as a follow-up to #32/#33. #}
  {% if decide_action(existing_relation, has_select_query=false, recoverable=false) == 'skip' %}
    {# dbt requires a 'main' statement result even when skipping #}
    {% call noop_statement('main', 'SKIP') %}{% endcall %}
    {{ ensure_tableflow_config(target_relation) }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {{ return({'relations': [target_relation]}) }}
  {% endif %}

  -- See comment above about calling hooks
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- Create the connector-backed table (long-running — gets primary name).
  {% call statement('main', execution_mode="streaming_ddl",
                    statement_name=get_statement_name()) -%}
    CREATE TABLE {{ target_relation }}
    ( {{ sql }})
    {{ get_distributed_by_clause() }}
    {{ render_with_options(with_options) }}
  {%- endcall %}

  {{ ensure_tableflow_config(target_relation) }}

  -- See comment above about calling hooks
  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

