{% materialization streaming_table, adapter='confluent' %}
  -- Check if the relation exists already, and precreate the target_relation
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type=this.Table).include(database=true, schema=true) -%}

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

  -- If user asks for full refresh, we drop any pre existing relation.
  -- Otherwise, we fail, because we can't alter a table.
  -- TODO: We can actually alter the options, so we should allow that at least.
  {{ drop_if_full_refresh(existing_relation) }}

  -- See comment above about calling hooks
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- First, create the table.
  {% call statement('main', execution_mode="streaming_ddl") -%}
    create table {{ target_relation }}
    {{ get_assert_columns_equivalent(sql) }}
    {{ get_table_columns_and_constraints() }}
    {% if with_options %}
    WITH (
      {%- for key, value in with_options.items() -%}
      '{{ key }}' = '{{ value }}'{%- if not loop.last %},{%- endif %}
      {%- endfor -%}
    )
    {% endif %}
  {%- endcall -%}

  -- Then insert data into it:
  {%- call statement('insert', execution_mode="streaming_query") -%}
    INSERT INTO {{ target_relation }} {{ sql }}
  {%- endcall -%}

  -- See comment above, calling hooks even if our transactions are noop.
  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
