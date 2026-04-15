{% materialization streaming_source, adapter='confluent' %}
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
  {%- set with_options = config.get('with', {}) -%}

  -- Run hooks like in the original materializations, so we don't
  -- break any assumption made by the framework.
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- TODO: Support altering table options without full refresh (ALTER TABLE ... SET).
  {% if skip_or_drop_existing(existing_relation, target_relation, has_select_query=false) %}
    {# dbt requires a 'main' statement result even when skipping #}
    {% call noop_statement('main', 'SKIP') %}{% endcall %}
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
    WITH (
      'connector' = '{{ connector }}'
      {%- for key, value in with_options.items() -%}
      , '{{ key }}' = '{{ value }}'
      {%- endfor -%}
    )
  {%- endcall %}

  -- See comment above about calling hooks
  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

