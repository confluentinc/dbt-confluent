-- {# TODO: Write comments/docs here #}
{% materialization streaming_source, adapter='confluent' %}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type=this.Table) %}
  {%- set connector = config.get('connector') -%}
  {%- set options = config.get('options', {}) -%}

  -- This 'sql' variable captures whatever text is in the .sql file 
  {%- set columns_declaration = sql -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- Drop the existing relation if it already exists
  -- TODO: Check if existing relation is not changed, avoid DROP if so.
  {{ drop_relation_if_exists(existing_relation) }}

  -- Even if we don't support transactions, let's still call the hooks
  -- like every other materialization strategy:
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main') -%}
    CREATE TABLE {{ target_relation }} 
    {{ columns_declaration }}
    WITH (
      'connector' = '{{ connector }}'
      {%- for key, value in options.items() -%}
      , '{{ key }}' = '{{ value }}'
      {%- endfor -%}
    )
  {%- endcall %}

  -- See comment above, calling hooks even if our transactions are noop.
  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

-- {# TODO: Write comments/docs here #}
{% materialization streaming_table, adapter='confluent' %}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type=this.Table).include(database=true, schema=true) -%}
  {%- set options = config.get('options', {}) -%}
  {%- set watermarks = config.get('watermarks', []) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- Drop the existing relation if it already exists
  -- TODO: Check if existing relation is not changed, avoid DROP if so.
  {{ drop_relation_if_exists(existing_relation) }}

  -- Even if we don't support transactions, let's still call the hooks
  -- like every other materialization strategy:
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- First, create the table.
  {% call statement('main') -%}
    create table {{ target_relation }}
    {{ get_assert_columns_equivalent(sql) }}
    {{ get_table_columns_and_constraints() }}
    {% if options %}
    WITH (
      {%- for key, value in options.items() -%}
      '{{ key }}' = '{{ value }}'{%- if not loop.last %},{%- endif %}
      {%- endfor -%}
    )
    {% endif %}
  {%- endcall -%}

  -- Then insert data into it:
  {%- call statement('insert') -%}
    INSERT INTO {{ target_relation }} {{ sql }}
  {%- endcall -%}

  -- See comment above, calling hooks even if our transactions are noop.
  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
