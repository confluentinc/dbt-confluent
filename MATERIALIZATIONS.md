# Materializations

## Supported

| Materialization | Description |
|---|---|
| `table` | Creates a table via `CREATE TABLE ... AS SELECT` (CTAS). Runs in snapshot mode — the query executes once and completes. If the table already exists, checks for schema drift (column names, data types, WITH options) and skips creation (use `--full-refresh` to drop and recreate). |
| `view` | Drop-and-recreate view. |
| `streaming_table` | Creates a table then runs a separate continuous `INSERT INTO ... SELECT` statement. This two-statement approach is currently the preferred way to build streaming pipelines (until Flink's materialized table feature reaches GA). Supports table options via `config(with={...})`. If the table already exists, checks for schema drift (column names, data types, WITH options) and skips creation (use `--full-refresh` to drop and recreate). |
| `streaming_source` | Creates a connector-backed source table (e.g., Datagen). Requires `config(connector='...')`. The model SQL defines the column definitions. Supports additional connector options via `config(with={...})`. If the table already exists, checks for schema drift (column names, data types, WITH options) and skips creation (use `--full-refresh` to drop and recreate). See the [Confluent connector catalog](https://docs.confluent.io/cloud/current/connectors/index.html) and [Flink CREATE TABLE documentation](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html) for available connectors and options. |
| `ephemeral` | Standard dbt CTE-based query fragment, not materialized in Flink. |

## Schema Drift Detection

When a table already exists and `--full-refresh` is not specified, the adapter performs drift detection before skipping creation.

To determine the expected schema, the adapter creates a short-lived temporary table (named `__dbt_tmp_schema_check_<model>_<invocation_id>`) and queries `INFORMATION_SCHEMA.COLUMNS` for its column names and data types. For `table` and `streaming_table`, this temp table is created from the model's SELECT query; for `streaming_source`, it is created from the model's column definitions (without the connector). The temp table is dropped immediately after the schema is read.

### Configuration

Control drift detection behavior with the `on_schema_drift` config:

```sql
{{ config(
    materialized='table',
    on_schema_drift='fail'  -- 'fail' (default) or 'ignore'
) }}
```

**Options**:
- `fail` (default) - Raise an error if schema drift is detected
- `ignore` - Skip drift detection entirely; always skip if the table exists

**Example**:
```sql
-- Disable drift detection for a specific model
{{ config(
    materialized='streaming_table',
    on_schema_drift='ignore'
) }}
select * from {{ ref('source') }}
```

### Column Drift
- **table, streaming_table**: Compares existing column names and data types with expected columns from the SELECT query. Raises an error if columns are added, removed, renamed, or if data types change. Column reordering is allowed (order doesn't matter for Kafka-backed tables).
- **streaming_source**: Compares existing column names and data types with the column definitions in the model SQL. Raises an error if columns are added, removed, renamed, or if data types change. Uses a temporary table to infer schema from SQL column definitions.

### WITH Options Drift
Compares existing `WITH` options against the model's `config(with={...})`. Raises an error if any configured option value has changed.

**Important limitation**: The adapter only verifies that user-specified options exist with the correct values. It does **not** detect when options are removed from the config, because connectors may add default options automatically (e.g., `fields.*.expression` from the faker connector), and we cannot distinguish between user-specified and auto-generated options.

**Example of undetected drift**:
```sql
-- Initial config
config(with={'changelog.mode': 'upsert'})

-- Changed to (option removed)
config(with={})

-- Result: The table still has changelog.mode=upsert, but dbt will skip (no error)
```

If you need to change or remove WITH options, use `--full-refresh` to drop and recreate the table.

### Query Logic Changes

Schema drift detection only inspects **column names, data types, and WITH options** — it does not detect changes to the query logic itself. If you modify how a column is computed without changing its name or type, the adapter will see no drift and skip the model.

**Example of undetected change**:
```sql
-- Initial model
select order_id, round(price, 2) as price from {{ ref('source') }}

-- Changed to (different rounding)
select order_id, round(price, 4) as price from {{ ref('source') }}

-- Result: Column name and type are unchanged, so dbt will skip (no error)
```

This is an inherent limitation — `INFORMATION_SCHEMA` only stores schema metadata, not the query that produced the table. If you change query logic, use `--full-refresh` to recreate the table.

### When Drift is Detected
If drift is detected, the run will fail with a compilation error. Use `--full-refresh` to drop and recreate the table with the new schema or options.

## Deterministic Statement Names

Each materialization creates Flink statements with deterministic names derived from the dbt project and model names:

```
{prefix}{project_name}-{model_name}
```

The default prefix is `dbt-`. For `streaming_table`, which creates two statements (a DDL and an INSERT), the DDL gets a `-ddl` suffix: `dbt-{project}-{model}-ddl`.

### Flink Naming Constraints

Flink statement names must contain only lowercase alphanumeric characters and hyphens, start with an alphanumeric character, and be at most 100 characters long. The adapter sanitizes names automatically:

- Illegal characters (including underscores) are replaced with hyphens
- A 6-char MD5 hash suffix is appended when characters are replaced, to avoid collisions (e.g. `my_model` and `my.model` produce different names)
- Names exceeding 100 characters are truncated with a 6-char hash suffix

### Custom Statement Names

Override the generated name with the `statement_name` config:

```sql
{{ config(materialized='streaming_table', statement_name='my-custom-name') }}
```

### Statement Lifecycle

On `--full-refresh`, the adapter deletes existing statements before dropping and recreating the table. When no relation exists, orphaned statements are also cleaned up. This enables idempotent re-runs and crash recovery.

## Not Supported

| Materialization | Reason |
|---|---|
| `materialized_view` | Use `table` instead. In Confluent Flink, materialized views are implemented as CTAS tables that are continuously updated by Flink. |
| `incremental` | dbt's batch-incremental semantics does not map to Flink's continuous processing model. Use `streaming_table` instead. |
| `snapshot` | Flink SQL lacks the batch operations (MERGE, UPDATE) required by dbt snapshots. |
