{% macro validate_distributed_by_config() %}
  {# Delegate to ConfluentAdapter.validate_distributed_by_config (Python).
     Materializations call this once at the top so downstream consumers
     (`get_distributed_by_clause`, `check_for_schema_drift`) can read the
     config directly without re-validating. #}
  {% do adapter.validate_distributed_by_config(config.get('distributed_by')) %}
{% endmacro %}


{% macro get_distributed_by_clause() %}
  {# Render `DISTRIBUTED BY HASH(col1, ...) [INTO N BUCKETS]` from the
     `distributed_by` config, or nothing if the config is unset.
     Flink only supports the HASH strategy today.
     The materialization is responsible for calling
     `validate_distributed_by_config()` once before render — we trust the
     config here. #}
  {%- set dist = config.get('distributed_by') -%}
  {%- if dist is not none -%}
    {%- set columns = dist.get('columns') -%}
    {%- set buckets = dist.get('buckets') -%}
    DISTRIBUTED BY HASH({% for col in columns %}{{ adapter.quote(col) }}{%- if not loop.last %}, {% endif -%}{% endfor %})
    {%- if buckets %} INTO {{ buckets }} BUCKETS{% endif -%}
  {%- endif -%}
{% endmacro %}

{% macro delete_statement_if_exists(statement_name, expect_exists=true) %}
  {# Delete an existing Flink statement by name so we can re-create it.
     No-op if the statement doesn't exist (confluent-sql handles 404).
     expect_exists: see adapter.delete_statement — controls whether a 403
     (treated as "missing statement" under pool-scoped roles) emits a loud
     warning or just a debug log. #}
  {% if execute %}
    {% do adapter.delete_statement(statement_name, expect_exists=expect_exists) %}
  {% endif %}
{% endmacro %}


{% macro decide_action(existing_relation, target_relation, has_select_query=true, recoverable=false) %}
  {# Decide what to do for an existing-relation materialization. Returns one of:
       'create'  — drop (if needed) and create from scratch
       'restart' — table is intact and matches; only the long-running statement
                   is missing or in a terminal phase, re-submit it (no DDL).
                   Caller is responsible for skipping its DDL block.
       'skip'    — relation exists, schema matches, and the statement is healthy.

     `recoverable` opts a materialization into the 'restart' branch. Only
     `streaming_table` is recoverable today; other materializations either
     don't have a long-running statement to restart (`table`, `view`) or
     can't be safely restarted without dropping the table (`streaming_source`).

     has_select_query: true if the model SQL is a SELECT (table, streaming_table),
                       false if it's column definitions (streaming_source). #}
  {% if not existing_relation %}
    {# No relation exists, but orphaned Flink statements may linger if the table
       was dropped without deleting its statements (e.g. external cleanup).
       Delete them so the materialization can create fresh ones. We don't expect
       them to exist in the common case (first-time run), so suppress the loud
       warning the adapter would otherwise emit for a pool-scoped 403. #}
    {{ delete_statement_if_exists(get_statement_name(), expect_exists=false) }}
    {{ delete_statement_if_exists(get_statement_name('-ddl'), expect_exists=false) }}
    {{ return('create') }}
  {% endif %}

  {% if should_full_refresh() %}
    {{ delete_statement_if_exists(get_statement_name()) }}
    {{ delete_statement_if_exists(get_statement_name('-ddl')) }}
    {{ drop_relation_if_exists(existing_relation) }}
    {{ return('create') }}
  {% endif %}

  {% set on_schema_drift = config.get('on_schema_drift', 'fail') %}
  {% if on_schema_drift == 'fail' %}
    {{ check_for_schema_drift(existing_relation, has_select_query) }}
  {% elif on_schema_drift != 'ignore' %}
    {% set msg = "Invalid value for on_schema_drift ('%s'). Expected 'ignore' or 'fail'." % on_schema_drift %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}

  {% if recoverable %}
    {% set health = adapter.classify_existing_statement(get_statement_name()) %}
    {% if health != 'healthy' %}
      {# Restart re-submits INSERT against the existing table. In 'fail' mode
         we already verified columns match above. In 'ignore' mode we skipped
         the check, so verify columns now — options/distribution drift won't
         break INSERT, but a column mismatch will. #}
      {% if on_schema_drift == 'ignore' %}
        {{ check_for_schema_drift(existing_relation, has_select_query, enforce='columns') }}
      {% endif %}
      {{ log("Statement for " ~ existing_relation ~ " is " ~ health ~ ". Re-submitting (no full refresh required).", info=True) }}
      {{ delete_statement_if_exists(get_statement_name()) }}
      {{ return('restart') }}
    {% endif %}
  {% endif %}

  {{ log("Relation " ~ existing_relation ~ " already exists. Skipping. Use --full-refresh to recreate.", info=True) }}
  {{ return('skip') }}
{% endmacro %}


{% macro check_for_schema_drift(existing_relation, has_select_query, enforce='all') %}
  {# Compare the existing table against what the model would produce and
     raise a compilation error on any drift (columns, types, WITH options,
     or DISTRIBUTED BY).

     Round-trips to Confluent are expensive, so we batch every metadata
     read into a single UNION ALL against INFORMATION_SCHEMA.  The temp
     table is the only way to get a normalized expected schema without
     parsing types ourselves.

     has_select_query: true if the model SQL is a SELECT (table,
                       streaming_table); false if it's column definitions
                       (streaming_source).
     enforce: 'all' (default) checks columns + options + distribution.
              'columns' only raises on column drift — used by the streaming
              restart path under on_schema_drift='ignore' to keep restart
              safe without rejecting benign options/distribution drift. #}

  {% set temp_table_name = adapter.generate_schema_check_temp_name(this.identifier, invocation_id) %}
  {% set temp_relation = adapter.Relation.create(
    database=this.database,
    schema=this.schema,
    identifier=temp_table_name,
    type='table'
  ) %}

  {% if has_select_query %}
    {% call statement('create_temp_table', hidden=True) %}
      CREATE TABLE {{ temp_relation }} AS
      SELECT * FROM (
        {{ sql }}
      ) WHERE FALSE
    {% endcall %}
  {% else %}
    {# streaming_source: column definitions are the source of truth. #}
    {% call statement('create_temp_table', hidden=True) %}
      CREATE TABLE {{ temp_relation }} ( {{ sql }} )
    {% endcall %}
  {% endif %}

  {{ get_drift_catalog(existing_relation, temp_relation) }}

  {% call statement('drop_temp_table', hidden=True) %}
    DROP TABLE IF EXISTS {{ temp_relation }}
  {% endcall %}

  {% do adapter.check_schema_drift(
    existing_relation,
    temp_relation,
    load_result('get_drift_catalog').table,
    config.get('with', {}),
    config.get('distributed_by'),
    enforce
  ) %}
{% endmacro %}


{% macro get_drift_catalog(existing_relation, temp_relation) %}
  {# Fetch every piece of metadata the drift check needs in one query.
     The result is a sparse table with a `section` discriminator and a
     `table_name` discriminator (existing vs temp for the COLUMNS section).
     Confluent's INFORMATION_SCHEMA only supports the primitives we use here:
     SELECT, WHERE with =/<>/IS NULL/IS NOT NULL/AND/OR, UNION ALL, AS,
     and CAST(NULL AS dt).

     Example rows for a table `my_table`(id BIGINT distributed, v STRING)
     with WITH(changelog.mode='upsert') being compared against a temp table
     `tmp`(id BIGINT, v STRING, x INT). Columns shown left-to-right are:
     section, table_name, col_name, data_type, dist_position, option_key,
     option_value, is_distributed, dist_buckets.

       ('COLUMNS',       'my_table', 'id', 'BIGINT', 1,    NULL,             NULL,     NULL,  NULL)
       ('COLUMNS',       'my_table', 'v',  'STRING', NULL, NULL,             NULL,     NULL,  NULL)
       ('COLUMNS',       'tmp',      'id', 'BIGINT', NULL, NULL,             NULL,     NULL,  NULL)
       ('COLUMNS',       'tmp',      'v',  'STRING', NULL, NULL,             NULL,     NULL,  NULL)
       ('COLUMNS',       'tmp',      'x',  'INT',    NULL, NULL,             NULL,     NULL,  NULL)
       ('TABLES',        'my_table', NULL, NULL,     NULL, NULL,             NULL,     'YES', 4)
       ('TABLE_OPTIONS', 'my_table', NULL, NULL,     NULL, 'changelog.mode', 'upsert', NULL,  NULL)

     `_partition_drift_catalog` (Python) splits this back into per-concern
     dicts before any drift check runs. #}
  {% call statement('get_drift_catalog', fetch_result=True, hidden=True) %}
    SELECT
      'COLUMNS' AS section,
      TABLE_NAME AS table_name,
      COLUMN_NAME AS col_name,
      FULL_DATA_TYPE AS data_type,
      DISTRIBUTION_ORDINAL_POSITION AS dist_position,
      CAST(NULL AS STRING) AS option_key,
      CAST(NULL AS STRING) AS option_value,
      CAST(NULL AS STRING) AS is_distributed,
      CAST(NULL AS INT) AS dist_buckets
    FROM INFORMATION_SCHEMA.`COLUMNS`
    WHERE TABLE_CATALOG_ID = '{{ existing_relation.database }}'
      AND TABLE_SCHEMA = '{{ existing_relation.schema }}'
      AND IS_HIDDEN = 'NO'
      AND (TABLE_NAME = '{{ existing_relation.identifier }}'
           OR TABLE_NAME = '{{ temp_relation.identifier }}')
    UNION ALL
    SELECT
      'TABLES' AS section,
      TABLE_NAME AS table_name,
      CAST(NULL AS STRING) AS col_name,
      CAST(NULL AS STRING) AS data_type,
      CAST(NULL AS INT) AS dist_position,
      CAST(NULL AS STRING) AS option_key,
      CAST(NULL AS STRING) AS option_value,
      IS_DISTRIBUTED AS is_distributed,
      DISTRIBUTION_BUCKETS AS dist_buckets
    FROM INFORMATION_SCHEMA.`TABLES`
    WHERE TABLE_CATALOG_ID = '{{ existing_relation.database }}'
      AND TABLE_SCHEMA = '{{ existing_relation.schema }}'
      AND TABLE_NAME = '{{ existing_relation.identifier }}'
    UNION ALL
    SELECT
      'TABLE_OPTIONS' AS section,
      TABLE_NAME AS table_name,
      CAST(NULL AS STRING) AS col_name,
      CAST(NULL AS STRING) AS data_type,
      CAST(NULL AS INT) AS dist_position,
      OPTION_KEY AS option_key,
      OPTION_VALUE AS option_value,
      CAST(NULL AS STRING) AS is_distributed,
      CAST(NULL AS INT) AS dist_buckets
    FROM INFORMATION_SCHEMA.`TABLE_OPTIONS`
    WHERE TABLE_CATALOG_ID = '{{ existing_relation.database }}'
      AND TABLE_SCHEMA = '{{ existing_relation.schema }}'
      AND TABLE_NAME = '{{ existing_relation.identifier }}'
  {% endcall %}
{% endmacro %}
