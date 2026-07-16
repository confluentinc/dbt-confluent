{% materialization materialized_table, adapter='confluent' %}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type=this.Table) %}

  {# Validates and renders in one step ('' when unset) — must run up here so a
     bad start_mode fails before the full-refresh drop below. #}
  {%- set start_mode = adapter.render_start_mode(config.get('start_mode')) -%}
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
  {# Pre-flight switch guard: Confluent cannot convert an existing regular
     table/view into a materialized table, so CREATE OR ALTER would fail with
     a confusing server error. Detect the switch up front and either fail with
     guidance or, under --full-refresh, drop through the regular-relation path
     (deterministic statements included — e.g. a prior `table` model's CTAS)
     before creating the MT. 'absent' (dropped externally since the cache was
     built) falls through to a plain create. #}
  {%- if existing_relation -%}
    {%- set existing_kind = get_existing_relation_kind(existing_relation) -%}
    {%- if existing_kind == 'regular' -%}
      {%- if should_full_refresh() -%}
        {{ delete_statement_if_exists(get_statement_name()) }}
        {{ delete_statement_if_exists(get_statement_name('-ddl')) }}
        {{ drop_relation_if_exists(existing_relation) }}
      {%- else -%}
        {% set msg %}
Model '{{ this.identifier }}' already exists as a regular table or view, which cannot be converted to a materialized table.
Run with --full-refresh to drop it and recreate it as a materialized table (for tables this permanently deletes the backing Kafka topic and its data).
        {% endset %}
        {% do exceptions.raise_compiler_error(msg) %}
      {%- endif -%}
    {%- elif existing_kind == 'materialized_table' and should_full_refresh() -%}
      {% do adapter.drop_materialized_table(existing_relation) %}
    {%- endif -%}
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
