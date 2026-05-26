{%- materialization view, adapter='confluent' -%}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- delete existing statement and drop the relation if it exists already.
  -- If there's no existing relation, the statement probably doesn't exist either,
  -- so suppress the loud warning the adapter emits for a pool-scoped 403.
  {{ delete_statement_if_exists(get_statement_name(), expect_exists=(existing_relation is not none)) }}
  {{ drop_relation_if_exists(existing_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main', statement_name=get_statement_name()) -%}
    {{ get_create_view_as_sql(target_relation, sql) }}
  {%- endcall %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization -%}
