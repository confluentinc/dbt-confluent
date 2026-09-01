{% materialization materialized_table, adapter='confluent' %}
  {# Pure config validation first, before load_cached_relation (which can be a
     real INFORMATION_SCHEMA round-trip if this schema's relation cache isn't
     already warm) -- none of these validators depend on the relation, so a
     config mistake should fail before any warehouse I/O, not after.
     `tableflow` isn't validated here: unlike start_mode/distributed_by, it's
     never baked into this DDL, so a bad value can't doom a --full-refresh
     recreate -- ensure_tableflow_config validates it for real when it
     actually applies the config. #}
  {% do validate_materialization_config() %}

  {# Validates and renders in one step ('' when unset) — must run up here so a
     bad start_mode fails before the full-refresh drop below. #}
  {%- set start_mode = adapter.render_start_mode(config.get('start_mode')) -%}

  {%- do adapter.validate_distributed_by_config(config.get('distributed_by')) -%}
  {%- do adapter.validate_materialized_table_config(config) -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type=this.Table) %}
  {%- set with_options = config.get('with', {}) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {# Switch guard: the server cannot convert an existing regular table/view
     into an MT (CREATE OR ALTER fails confusingly), so fail with guidance —
     or, under --full-refresh, drop the regular relation and its statements
     first. 'absent' (dropped externally since the cache was built) falls
     through to a plain create. #}
  {%- if existing_relation -%}
    {%- set existing_kind = adapter.get_relation_kind(existing_relation) -%}
    {%- if existing_kind == 'regular' -%}
      {%- if should_full_refresh() -%}
        {{ delete_statement_if_exists(get_statement_name()) }}
        {{ delete_statement_if_exists(get_statement_name('-ddl')) }}
        {{ disable_old_tableflow_before_drop(existing_relation) }}
        {{ drop_relation_if_exists(existing_relation) }}
      {%- else -%}
        {% set msg %}
Model '{{ this.identifier }}' already exists as a regular table or view, which cannot be converted to a materialized table.
Run with --full-refresh to drop it and recreate it as a materialized table (for tables this permanently deletes the backing Kafka topic and its data).
        {% endset %}
        {% do exceptions.raise_compiler_error(msg) %}
      {%- endif -%}
    {%- elif existing_kind == 'materialized_table' and should_full_refresh() -%}
      {{ disable_old_tableflow_before_drop(existing_relation) }}
      {% do adapter.drop_materialized_table(existing_relation) %}
    {%- endif -%}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# CREATE OR ALTER re-asserts the definition every run: the server no-ops
     an unchanged definition and evolves a changed one in place — discarding
     processing state and restarting per start_mode, so stateful models on a
     RESUME_* start_mode (the default) silently reset; see MATERIALIZATIONS.md.
     Submitted under a per-run
     statement name: the DDL is bounded and reaped at cursor close, but a
     FAILED submission lingers and would 409-collide with a reused name.
     statement_properties applies here (not a separate INSERT, unlike
     streaming_table) since this single statement is what runs the model's
     query. #}
  {% call statement('main', execution_mode="streaming_ddl",
                    statement_name=get_statement_name('-' ~ invocation_id),
                    statement_properties=config.get('statement_properties')) -%}
    CREATE OR ALTER MATERIALIZED TABLE {{ target_relation }}
    {%- set contract = render_contract_columns_and_reproject_sql(sql) -%}
    {{ contract.ddl }}
    {%- set sql = contract.sql %}
    {{ get_distributed_by_clause() }}
    {{ render_with_options(with_options) }}
    {%- if start_mode %}
    START_MODE = {{ start_mode }}
    {%- endif %}
    AS
    {{ sql }}
  {%- endcall %}

  {# Same check on every run, whether this statement just created the table,
     evolved it, or was a server-side no-op -- no need to track which. #}
  {{ ensure_tableflow_config(target_relation) }}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
