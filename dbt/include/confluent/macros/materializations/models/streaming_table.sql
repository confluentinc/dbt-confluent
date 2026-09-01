{% materialization streaming_table, adapter='confluent' %}
  {# Pure config validation first, before load_cached_relation (which can be a
     real INFORMATION_SCHEMA round-trip if this schema's relation cache isn't
     already warm) -- it doesn't depend on the relation, so a config mistake
     should fail before any warehouse I/O, not after. `tableflow` isn't
     validated here: unlike distributed_by, it's never baked into this DDL, so a
     bad value can't doom a --full-refresh recreate -- ensure_tableflow_config
     validates it for real when it actually applies the config. #}
  {% do validate_materialization_config() %}
  {%- do adapter.validate_distributed_by_config(config.get('distributed_by')) -%}

  -- Check if the relation exists already, and precreate the target_relation
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type=this.Table) -%}

  -- This is a config option that makes it easier to add WITH clauses.
  -- We could technically avoid this and let users write custom constraints,
  -- but it makes it easier to compose the query:
  -- config(with={'changelog.mode': 'append'})
  -- instead of:
  -- config(constraints=[{"type": "custom", "expression": "WITH ('changelog.mode' = 'append')"}])
  {%- set with_options = config.get('with', {}) -%}

  -- Run hooks like in the original materializations, so we don't break
  -- any assumption from the framework
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- TODO: Support altering table options without full refresh (ALTER TABLE ... SET).
  {% set action = decide_action(existing_relation, recoverable=true) %}
  {% if action == 'skip' %}
    {# dbt requires a 'main' statement result even when skipping #}
    {% call noop_statement('main', 'SKIP') %}{% endcall %}
    {{ ensure_tableflow_config(target_relation) }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {{ return({'relations': [target_relation]}) }}
  {% endif %}

  -- See comment above about calling hooks
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if action != 'restart' %}
    -- Create the table (transient DDL — gets '-ddl' suffix). On 'restart'
    -- the table is intact and the schema matches, so we skip the DDL and
    -- only re-submit the long-running INSERT below.
    {% call statement('ddl', execution_mode="streaming_ddl",
                      statement_name=get_statement_name('-ddl')) -%}
      create table {{ target_relation }}
      {{ get_assert_columns_equivalent(sql) }}
      {{ get_table_columns_and_constraints() }}
      {{ get_distributed_by_clause() }}
      {{ render_with_options(with_options) }}
    {%- endcall -%}
  {% endif %}

  {# The table exists by this point either way -- freshly created just now
     (action == 'create'), or already there and intact (action == 'restart',
     which skips the DDL above but never the table itself). One check
     covers both. #}
  {{ ensure_tableflow_config(target_relation) }}

  -- Long-running INSERT — registered as 'main' so its compiled SQL is the
  -- artifact written to disk for `dbt show` / debugging, and so the restart
  -- path satisfies dbt's "main result" contract without renaming.
  {%- call statement('main', execution_mode="streaming_query",
                     statement_name=get_statement_name(),
                     statement_properties=config.get('statement_properties')) -%}
    INSERT INTO {{ target_relation }} {{ sql }}
  {%- endcall -%}

  -- See comment above, calling hooks even if our transactions are noop.
  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
