# Materializations

## Supported

| Materialization | Description |
|---|---|
| `table` | Drop-and-recreate table via CTAS. |
| `view` | Drop-and-recreate view. |
| `materialized_view` | CTAS table continuously updated by Flink. No manual refresh needed. |
| `streaming_table` | Creates a table then runs a continuous INSERT query. Supports table options via `config(with={...})`. Requires `--full-refresh` to re-run against an existing table. |
| `streaming_source` | Creates a connector-backed source table (e.g., Datagen). Requires `config(connector='...')`. The model SQL defines the column definitions. Supports additional options via `config(with={...})`. Requires `--full-refresh` to re-run. |
| `ephemeral` | Standard CTE-based, not materialized in Flink. |

## Not Supported

| Materialization | Reason |
|---|---|
| `incremental` | dbt's batch-incremental semantics don't map to Flink's continuous processing model. Use `streaming_table` instead. |
| `snapshot` | Flink SQL lacks the batch operations (MERGE, UPDATE) required by dbt snapshots. |
