# Materializations

## Supported

| Materialization | Description |
|---|---|
| `table` | Creates a table via `CREATE TABLE ... AS SELECT` (CTAS). Runs in snapshot mode — the query executes once and completes. Requires `--full-refresh` to replace an existing table. |
| `view` | Drop-and-recreate view. |
| `materialized_view` | Creates a Flink table via CTAS that is continuously updated by Flink. No manual refresh needed. Note: despite the dbt name, this creates a Flink table with a continuous query, not a traditional database materialized view. |
| `streaming_table` | Creates a table then runs a separate continuous `INSERT INTO ... SELECT` statement. This two-statement approach is currently the preferred way to build streaming pipelines (until Flink's materialized table feature reaches GA). Supports table options via `config(with={...})`. Requires `--full-refresh` to replace an existing table. |
| `streaming_source` | Creates a connector-backed source table (e.g., Datagen). Requires `config(connector='...')`. The model SQL defines the column definitions. Supports additional connector options via `config(with={...})`. Requires `--full-refresh` to replace an existing table. See the [Confluent connector catalog](https://docs.confluent.io/cloud/current/connectors/index.html) and [Flink CREATE TABLE documentation](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html) for available connectors and options. |
| `ephemeral` | Standard dbt CTE-based query fragment, not materialized in Flink. |

## Not Supported

| Materialization | Reason |
|---|---|
| `incremental` | dbt's batch-incremental semantics does not map to Flink's continuous processing model. Use `streaming_table` instead. |
| `snapshot` | Flink SQL lacks the batch operations (MERGE, UPDATE) required by dbt snapshots. |
