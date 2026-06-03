{% materialization materialized_table, adapter='confluent' %}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type=this.Table) %}

  {%- set distributed_by = config.get('distributed_by') -%}
  {%- set buckets = config.get('buckets', 6) -%}
  {%- set start_mode = config.get('start_mode') -%}
  {%- set with_options = config.get('with', {}) -%}

  {{ validate_materialized_table_config() }}

  {%- if distributed_by is string -%}
    {%- set distributed_by_clause = distributed_by -%}
  {%- elif distributed_by -%}
    {%- set distributed_by_clause = distributed_by | join(', ') -%}
  {%- else -%}
    {%- set distributed_by_clause = none -%}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {# CREATE OR ALTER evolves the MT in place, so there is no existence check or
     full-refresh drop branch. We still delete any prior Flink statement under the
     deterministic name so it can be reused on re-runs. MT creates a single
     statement (no separate '-ddl' phase); expect_exists is gated on
     existing_relation to avoid the loud pool-scoped-403 warning on first run. #}
  {{ delete_statement_if_exists(get_statement_name(), expect_exists=(existing_relation is not none)) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main', execution_mode="streaming_ddl",
                    statement_name=get_statement_name()) -%}
    CREATE OR ALTER MATERIALIZED TABLE {{ target_relation }}
    {%- if distributed_by_clause %}
    DISTRIBUTED BY HASH({{ distributed_by_clause }}) INTO {{ buckets }} BUCKETS
    {%- endif %}
    {%- if with_options %}
    WITH (
      {%- for key, value in with_options.items() %}
      '{{ key }}' = '{{ value | replace("'", "''") }}'{{ "," if not loop.last }}
      {%- endfor %}
    )
    {%- endif %}
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
