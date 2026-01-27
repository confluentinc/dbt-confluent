{%- materialization view, adapter='confluent' -%}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the relation if it exists already in the database
  {{ drop_relation_if_exists(existing_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ get_create_view_as_sql(target_relation, sql) }}
  {%- endcall %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization -%}
