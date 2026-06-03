{% materialization materialized_table, adapter='confluent' %}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type=this.Table) %}

  {%- set start_mode = config.get('start_mode') -%}
  {%- set with_options = config.get('with', {}) -%}

  {{ validate_distributed_by_config() }}
  {{ validate_materialized_table_config() }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {# Declarative lifecycle: we always re-assert the definition with
     CREATE OR ALTER and let Flink reconcile it — a new table is created, any
     change (columns, WITH options, or query logic) is evolved in place, and an
     unchanged definition is a cheap no-op. `--full-refresh` drops first so the
     table is rebuilt from scratch (the way to change distribution). Re-running
     within Flink's brief establishment window is transiently rejected
     ("being modified") and retried by the connection manager. #}
  {%- if existing_relation and should_full_refresh() -%}
    {% do adapter.drop_materialized_table(existing_relation) %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# Submit under a per-invocation statement name. An MT stays tied to its
     defining CREATE OR ALTER statement, so we must not delete-and-reuse a fixed
     name (that orphans the table); a unique name per run avoids any collision. #}
  {% call statement('main', execution_mode="streaming_ddl",
                    statement_name=get_statement_name('-' ~ invocation_id)) -%}
    CREATE OR ALTER MATERIALIZED TABLE {{ target_relation }}
    {{ get_distributed_by_clause() }}
    {{ render_with_options(with_options) }}
    {%- if start_mode %}
    START_MODE = {{ start_mode }}
    {%- endif %}
    AS
    {{ sql }}
  {%- endcall %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
