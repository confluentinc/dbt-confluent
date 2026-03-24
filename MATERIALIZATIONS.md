# Materializations

## Supported

| Materialization | Description |
|---|---|
| `table` | Creates a table via `CREATE TABLE ... AS SELECT` (CTAS). Runs in snapshot mode — the query executes once and completes. If the table already exists, checks for schema drift and skips creation (use `--full-refresh` to drop and recreate). |
| `view` | Drop-and-recreate view. |
| `streaming_table` | Creates a table then runs a separate continuous `INSERT INTO ... SELECT` statement. This two-statement approach is currently the preferred way to build streaming pipelines (until Flink's materialized table feature reaches GA). Supports table options via `config(with={...})`. If the table already exists, checks for schema drift and skips creation (use `--full-refresh` to drop and recreate). |
| `streaming_source` | Creates a connector-backed source table (e.g., Datagen). Requires `config(connector='...')`. The model SQL defines the column definitions. Supports additional connector options via `config(with={...})`. If the table already exists, checks for options drift and skips creation (use `--full-refresh` to drop and recreate). See the [Confluent connector catalog](https://docs.confluent.io/cloud/current/connectors/index.html) and [Flink CREATE TABLE documentation](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html) for available connectors and options. |
| `ephemeral` | Standard dbt CTE-based query fragment, not materialized in Flink. |

## Schema Drift Detection

When a table already exists and `--full-refresh` is not specified, the adapter performs drift detection before skipping creation:

- **Column drift** (table, streaming_table): Compares existing column names with expected columns from the SELECT query. Raises an error if columns are added, removed, or renamed. Column reordering is allowed.
- **Options drift** (all table types): Compares existing `WITH` options against the model's `config(with={...})`. Raises an error if any configured option has changed.

**Limitations:**
- Column data types are not checked — only names
- For `streaming_source`, column drift is not detected (only `WITH` options are checked)

If drift is detected, use `--full-refresh` to drop and recreate the table.

## Not Supported

| Materialization | Reason |
|---|---|
| `materialized_view` | Use `table` instead. In Confluent Flink, materialized views are implemented as CTAS tables that are continuously updated by Flink. |
| `incremental` | dbt's batch-incremental semantics does not map to Flink's continuous processing model. Use `streaming_table` instead. |
| `snapshot` | Flink SQL lacks the batch operations (MERGE, UPDATE) required by dbt snapshots. |
