{% materialization streaming_table, adapter='confluent' %}
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
  {% if skip_or_drop_existing(existing_relation, target_relation) %}
    {# dbt requires a 'main' statement result even when skipping #}
    {% call noop_statement('main', 'SKIP') %}{% endcall %}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {{ return({'relations': [target_relation]}) }}
  {% endif %}

  -- See comment above about calling hooks
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- First, create the table (transient DDL — gets '-ddl' suffix).
  {% call statement('main', execution_mode="streaming_ddl",
                    statement_name=get_statement_name('-ddl')) -%}
    create table {{ target_relation }}
    {{ get_assert_columns_equivalent(sql) }}
    {{ get_table_columns_and_constraints() }}
    {{ get_distributed_by_clause() }}
    {% if with_options %}
    WITH (
      {%- for key, value in with_options.items() -%}
      '{{ key }}' = '{{ value }}'{%- if not loop.last %},{%- endif %}
      {%- endfor -%}
    )
    {% endif %}
  {%- endcall -%}

  -- Then insert data into it (long-running — gets primary name).
  {%- call statement('insert', execution_mode="streaming_query",
                     statement_name=get_statement_name()) -%}
    INSERT INTO {{ target_relation }} {{ sql }}
  {%- endcall -%}

  -- See comment above, calling hooks even if our transactions are noop.
  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
