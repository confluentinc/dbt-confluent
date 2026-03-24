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

When a table already exists and `--full-refresh` is not specified, the adapter performs drift detection before skipping creation:

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

### When Drift is Detected
If drift is detected, the run will fail with a compilation error. Use `--full-refresh` to drop and recreate the table with the new schema or options.

## Not Supported

| Materialization | Reason |
|---|---|
| `materialized_view` | Use `table` instead. In Confluent Flink, materialized views are implemented as CTAS tables that are continuously updated by Flink. |
| `incremental` | dbt's batch-incremental semantics does not map to Flink's continuous processing model. Use `streaming_table` instead. |
| `snapshot` | Flink SQL lacks the batch operations (MERGE, UPDATE) required by dbt snapshots. |
