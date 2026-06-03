{% materialization materialized_table, adapter='confluent' %}
  {%- set target_relation = this.incorporate(type=this.Table) %}

  {%- set freshness = config.get('freshness_interval') -%}
  {%- set partition_by = config.get('partition_by') -%}
  {%- set distributed_by = config.get('distributed_by') -%}
  {%- set buckets = config.get('buckets', 6) -%}
  {%- set refresh_mode = config.get('refresh_mode') -%}
  {%- set start_mode = config.get('start_mode') -%}
  {%- set with_options = config.get('with', {}) -%}

  {%- if partition_by is string -%}
    {%- set partition_by_clause = partition_by -%}
  {%- elif partition_by -%}
    {%- set partition_by_clause = partition_by | join(', ') -%}
  {%- else -%}
    {%- set partition_by_clause = none -%}
  {%- endif -%}

  {%- if distributed_by is string -%}
    {%- set distributed_by_clause = distributed_by -%}
  {%- elif distributed_by -%}
    {%- set distributed_by_clause = distributed_by | join(', ') -%}
  {%- else -%}
    {%- set distributed_by_clause = none -%}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {# CREATE OR ALTER swaps the MT in place, so no existence check or
     full-refresh branch is needed. Prior Flink statements still have to be
     deleted so the new statement_name can be reused. #}
  {{ delete_statement_if_exists(get_statement_name()) }}
  {{ delete_statement_if_exists(get_statement_name('-ddl')) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main', execution_mode="streaming_ddl",
                    statement_name=get_statement_name()) -%}
    CREATE OR ALTER MATERIALIZED TABLE {{ target_relation }}
    {%- if distributed_by_clause %}
    DISTRIBUTED BY HASH({{ distributed_by_clause }}) INTO {{ buckets }} BUCKETS
    {%- endif %}
    {%- if partition_by_clause %}
    PARTITIONED BY ({{ partition_by_clause }})
    {%- endif %}
    {%- if with_options %}
    WITH (
      {%- for key, value in with_options.items() %}
      '{{ key }}' = '{{ value | replace("'", "''") }}'{{ "," if not loop.last }}
      {%- endfor %}
    )
    {%- endif %}
    {%- if freshness %}
    FRESHNESS = {{ freshness }}
    {%- endif %}
    {%- if refresh_mode %}
    REFRESH_MODE = {{ refresh_mode }}
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
