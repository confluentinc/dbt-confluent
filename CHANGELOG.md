# Changelog

All notable changes to this dbt adapter will be documented in this file.

<!-- towncrier release notes start -->

## dbt-confluent 0.6.0 (2026-11-20)

### Features

- Add `cloud_api_key`/`cloud_api_secret`, `tableflow_api_key`/`tableflow_api_secret` and `database_kafka_cluster_id` profile configs, passed through to `confluent_sql.connect()`. ([#105](https://github.com/confluentinc/dbt-confluent/issues/105))
- Add an optional `name_prefix` profile setting that isolates tables and statements in a shared environment. It is prepended to statement names, in front of `statement_name_prefix`, and to table names with hyphens as underscores. ([#127](https://github.com/confluentinc/dbt-confluent/issues/127))
- Add a `function` materialization for building Java or Python UDFs. See MATERIALIZATIONS.md#function for details.
- `organization_id`, `cloud_provider` and `cloud_region` may now be omitted from the profile with `auth: oauth` or a Global API key: the driver infers the organization (when your credentials reach exactly one) and the cloud settings (from the Kafka cluster named by `dbname`), and checks any you do set. Connecting fails with guidance naming the profile field to set when several organizations (`organization_id`) or several same-named clusters (`database_kafka_cluster_id`) match, or when `cloud_provider`/`cloud_region` disagree with the cluster. A Flink-region key still requires them, now reported when the profile loads. `dbt init` offers to infer them, and `dbt debug` leaves out the ones omitted. See the README's "Organization, cloud provider and region" section. ([#237](https://github.com/confluentinc/dbt-confluent/issues/237))

### Bugfixes

- `view` now updates an existing view in place with `ALTER VIEW ... AS` instead of dropping it and creating it again. Before, the view was dropped first, so when the new SQL failed (a server-rejected query, or an enforced contract that no longer matched) the run left no view at all, and anything reading it broke until the model was fixed. Now the run fails and the previous view keeps serving. A first run, or a model switching from another materialization to `view`, still creates the view as before, and `--full-refresh` still drops the view and creates it again, as it does for the other materializations. Flink doesn't check an altered view's new columns against the old ones, so a downstream view that uses a column the new SQL removes or retypes is not caught at run time, and fails when queried until it is rebuilt. See MATERIALIZATIONS.md#view-statements-emitted. ([#74](https://github.com/confluentinc/dbt-confluent/issues/74))
- Schema drift detection no longer reports false drift on a `WITH` option when Flink reports its value in a different form than the model wrote it. Durations and sizes are compared by value, so `'kafka.retention.time': '7 days'` matches the `7 d` Flink reports, and `'kafka.retention.size': '1024 mb'` matches `1 gb`; case is ignored, so `'COMPACT'` matches `compact`. Before this change, these models reported drift on every run until rebuilt with `--full-refresh`. A change that only alters the case of a value is not detected, nor is a change to a text value that reads as the same duration or size (`'1 h'` to `'60 min'`). See MATERIALIZATIONS.md#with-options-drift. ([#121](https://github.com/confluentinc/dbt-confluent/issues/121))
- dbt's contract check no longer rejects a `CHAR(n)` or `BINARY(n)` column (e.g. a string literal like `'abc'` or a binary literal like `x'0102'`) for a column declared `string`/`varchar` or `bytes`/`varbinary`. Flink types a literal as `CHAR(n)` or `BINARY(n)`, so the model needed `CAST('abc' AS STRING)`. The schema drift check for a `streaming_table` or a `table` with an enforced contract accepts the same columns, so these models also re-run without reporting drift. A literal for a `varchar(n)` or `varbinary(n)` column below the maximum length is still rejected, as before: the check doesn't compare lengths, and Flink silently truncates a value longer than `n`. See MATERIALIZATIONS.md#schema-drift-detection. ([#182](https://github.com/confluentinc/dbt-confluent/issues/182))

### Misc

- Upgraded the underlying `confluent-sql` driver from 0.8 to 0.9 (`confluent-sql~=0.9.0`). The driver now retries a slow statement-results page and tolerates a single slow status poll, instead of failing the model, and retries more of its idempotent control-plane GETs (such as Tableflow state lookups) on transient errors. The adapter was updated for the driver's reorganized Tableflow API and timeout settings, and Tableflow's "needs a Global API key" guidance now recognizes the 0.9.0 driver's reworded missing-credential errors. Profiles and project configuration are unchanged.
- Upgraded to dbt Core 1.12 (`dbt-core~=1.12.0`, `dbt-adapters~=1.24.5`); dbt Core 1.11 is no longer supported.

## dbt-confluent 0.5.0 (2026-11-06)

### Breaking Changes

- `streaming_table` now runs dbt's contract check before re-submitting a missing or stopped INSERT, as it already did before `create table`, whatever `on_schema_drift` is set to. ([#184](https://github.com/confluentinc/dbt-confluent/issues/184))

### Features

- **Oauth** is now supported as an authentication mechanism. Add an `auth: oauth` profile config for browser-based interactive Confluent Cloud sign-in instead of any API keys! See the [README](README.md#configuration) for more details.
- **Much faster schema drift checks**: The schema drift checks for `table` and `streaming_table` materializations are now done using Confluent Cloud for Flink's `dry-run` capability to determine the SQL schema of a SELECT statement (fast) instead of creating and then dropping a representative table from the new model and performing the diffing in INFORMATION_SCHEMA.COLUMNS-space (slow). See MATERIALIZATIONS.md#schema-drift-detection. ([#118](https://github.com/confluentinc/dbt-confluent/issues/118))

### Bugfixes

- `streaming_table` now inserts into its table's columns by name, guarding it against same-type column reordering schema migration bugs. See MATERIALIZATIONS.md#streaming-table. ([#184](https://github.com/confluentinc/dbt-confluent/issues/184))
- The schema drift check no longer reports drift on every re-run of a Protobuf (`'value.format' = 'proto-registry'`) `streaming_table` with a `CHAR` or `VARCHAR` MAP key or `MULTISET` element. See MATERIALIZATIONS.md#schema-drift-detection. ([#185](https://github.com/confluentinc/dbt-confluent/issues/185))
- `dbt docs generate` now only reads catalog metadata for the project's own relations, in small retried chunks, instead of querying the whole environment's `INFORMATION_SCHEMA`, which often timed out.

### Misc

- The schema drift check now reads only what it needs from `INFORMATION_SCHEMA`: `table` and `streaming_table` models no longer read the existing table's columns (only its distribution columns, when `distributed_by` is set). If `INFORMATION_SCHEMA.TABLES` doesn't have the existing table yet, the check now fails with a retriable error rather than reporting drift. ([#189](https://github.com/confluentinc/dbt-confluent/issues/189))


## dbt-confluent 0.4.1 (2026-10-26)

### Bugfixes

- Avoid reporting success erroneously when a tableflow replacement cannot complete due to cleanup delays.

## dbt-confluent 0.4.0 (2026-10-01)

### Features

- Add a materialized_table materialization built on Confluent Cloud for Apache Flink's `CREATE OR ALTER MATERIALIZED TABLE ... AS SELECT` statement. See [Materialized Tables in Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/concepts/materialized-tables.html). ([#62](https://github.com/confluentinc/dbt-confluent/issues/62))
  - Supports dbt contracts (`config(contract={'enforced': true})`), including model-level primary_key constraints, enabling snapshot queries against the resulting table. ([#81](https://github.com/confluentinc/dbt-confluent/issues/81))
- Add a tableflow config for `table`, `streaming_table`, `streaming_source`, and `materialized_table` materializations to enable Tableflow on a model's backing Kafka topic, materializing it as an Iceberg and/or Delta Lake table. See [Tableflow in Confluent Cloud](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html). ([#99](https://github.com/confluentinc/dbt-confluent/issues/99))
  - All external storage options are supported, include the recently released Google Cloud Storage option as well as AWS S3 & Azure DataLake. ([#167](https://github.com/confluentinc/dbt-confluent/issues/167))
- Add a `statement_properties` config for `materialized_table` and `streaming_table` materializations to set Flink SET-style statement properties. See the [SET Statement documentation](https://docs.confluent.io/cloud/current/flink/reference/statements/set.html) for available options. ([#76](https://github.com/confluentinc/dbt-confluent/issues/76))
  - Applies to the long-running INSERT statement only, not the separate DDL statement (whose options can be configured with the existing 'with' configuration option).
- Setting a dbt-confluent config key (e.g. `with`, `distributed_by`, `statement_properties`, etc.) on a materialization that doesn't consume it now fails the run immediately with a clear error, instead of silently doing nothing. Opt a specific key out per model with `config(ignore_unsupported_config=[...])`.

### Bugfixes

- Fix `dbt-core` dependency specifier from `~=1.11` (allows 1.12+) to `~=1.11.0` (locks to 1.11.x), preventing accidental installation of unsupported dbt-core versions.

### Misc

- Bump `confluent-sql` dependency to `~=0.6.0`.
- Cap supported Python versions at `<3.14`.


## dbt-confluent 0.3.1 (2026-09-09)

### Bugfixes

- The `table` materialization's CTAS statement is now submitted in `snapshot_ddl` mode. Previously it used no explicit execution mode, so it fell back to the connection default (`streaming_query`), contradicting the documented snapshot behavior and leaving an unbounded statement running instead of one that completes. ([#77](https://github.com/confluentinc/dbt-confluent/issues/77))
- Surface the underlying `confluent-sql` exception on connection failure instead of masking it behind a generic `confluent_sql connection error`, so `dbt` shows the driver's actual error message (e.g. authentication failures or read timeouts).
- Fixed `ConfluentColumn` inheriting dbt-core's default `STRING -> TEXT` `TYPE_LABELS` mapping, which produced invalid Flink SQL (`TEXT`) for any contract-enforced model with a `string` column. ([#92](https://github.com/confluentinc/dbt-confluent/issues/92))

### Misc

- Cap supported Python versions at `<3.14` (a dbt-core dependency is not yet Python 3.14 compatible) and bump `confluent-sql` to `~=0.5.4`.
- Split the schema drift `INFORMATION_SCHEMA.COLUMNS` query into two `TABLE_NAME`-scoped `SELECT`s joined by `UNION ALL` instead of one `SELECT` with an `OR`, so the server can push the `TABLE_NAME` predicate down.


## dbt-confluent 0.3.0 (2026-07-09)

### Features

- `streaming_table` re-runs without `--full-refresh` now auto-recover when the long-running INSERT statement is missing or in a terminal phase (`COMPLETED`, `STOPPED`, `FAILED`, `DELETED`). The adapter resubmits only the INSERT under the same deterministic name; the table and its topic state are preserved. Closes the crash-recovery gap (#33) and the dead-statement half of #32.
- When recovering under `on_schema_drift='ignore'`, the restart path still runs a columns-only drift check before resubmitting: `ignore` suppresses benign WITH-options/distribution drift, but a changed column list would make the resubmitted INSERT fail at Flink with a cryptic "Different number of columns" error, so the adapter raises a clear drift error instead.
- Statement classification treats a 403 like a 404: compute-pool-scoped roles return 403 (not 404) for a statement that is missing or lives on a different compute pool than the one in config, so the adapter treats it as missing and resubmits (emitting a warning). A genuine permission problem still surfaces on the resubmitted statement, which runs on the same scope. ([#32](https://github.com/confluentinc/dbt-confluent/issues/32))
- Add `distributed_by` config for `table`, `streaming_table`, and `streaming_source` materializations to emit a `DISTRIBUTED BY HASH(...) INTO N BUCKETS` clause in the generated `CREATE TABLE` DDL. Schema drift detection now also covers `distributed_by` (column list and bucket count when explicitly set). See MATERIALIZATIONS.md ([#34](https://github.com/confluentinc/dbt-confluent/issues/34))
- Add a per-model `compute_pool_id` config to override the profile-default compute pool for an individual model (e.g. to isolate a heavy model or assign workloads across pools). The override flows through every statement the model submits (DDL, the long-running INSERT, and metadata/drift-check queries); recovery and cleanup remain pool-agnostic and resubmit dead statements on the model's configured pool. Pairs well with `env_var()` to inject per-environment/region pools at deploy time. See MATERIALIZATIONS.md ([#37](https://github.com/confluentinc/dbt-confluent/issues/37))
- Document and test adopting existing Flink resources into dbt management: point a `streaming_table` model at a table and a long-running statement deployed outside dbt via the `alias` (existing table) and `statement_name` (existing statement) configs. On re-run the adapter takes over their lifecycle by name — a healthy statement is adopted as-is, a dead one is re-submitted under the same name, and the table is never dropped. See the "Adopting Existing Tables and Statements" section in MATERIALIZATIONS.md for preconditions (schema must match; names are sanitized). ([#38](https://github.com/confluentinc/dbt-confluent/issues/38))
- Upgraded the `confluent-sql` driver to `0.4.0`, unlocking two new credential options:

  - **Global API keys**: authenticate with `global_api_key` / `global_api_secret` (a "Global" Confluent Cloud key that works against every route) as an alternative to the Flink-region `flink_api_key` / `flink_api_secret` pair. Supply exactly one complete pair; the Global pair is preferred when both are given.
  - **Poolless Flink**: `compute_pool_id` is now optional. When omitted, statements run in the environment+region default compute pool (provisioned if necessary), instead of requiring an explicit pool.

  `dbt init` prompts for both choices, and `dbt debug` now reports the configured compute pool.

### Bugfixes

- Recover from a crash between `streaming_table`'s DDL and INSERT: a follow-up `dbt run` (no `--full-refresh`) now resubmits the missing INSERT instead of silently skipping the model. See #32 for the full lifecycle change. ([#33](https://github.com/confluentinc/dbt-confluent/issues/33))
- Changing a `streaming_source` model's `connector` config is now detected as schema drift. Previously the drift check only compared the `with` config, so a connector change was silently skipped and the old connector kept running.
- Schema drift checks no longer permanently leak their temp table (a real Kafka-backed topic) when the run fails between creating and dropping it: the temp name is now deterministic per model (no `invocation_id` suffix) and the check drops any leftover before creating, so the next run reclaims a leak.
- Single quotes in `with` option keys/values and in the `connector` config are now escaped (doubled) when rendered into `streaming_table`/`streaming_source` DDL, instead of breaking the statement or escaping the string literal.
- Temp tables created by the schema-drift check and the unit-test materialization are now dropped in the adapter's `post_model_hook` (which dbt runs in a try/finally around every materialization) instead of by inline drops at the end of the macro. The inline drops never ran when the materialization raised partway — e.g. when building the drift catalog failed, or when a later unit-test fixture errored and left the fixtures created so far undropped — leaking the temp table (a real Kafka-backed topic) until the next run's preemptive drop reclaimed it. Cleanup now runs on the failure path too, and goes through `drop_relation` (type-aware, cache-consistent) rather than a hardcoded `DROP TABLE`. The deterministic temp name plus a preemptive drop-if-exists remain as the backstop for hard-killed runs.
- The schema drift check now surfaces a retriable error when INFORMATION_SCHEMA returns no columns for the *existing* table (metadata propagation lag), instead of falsely reporting every model column as added and advising a destructive `--full-refresh`. This mirrors the guard that already existed for the temp table.


## dbt-confluent 0.2.1 (2026-05-27)

### Bugfixes

- Fix `dbt run` failures under compute-pool-scoped FlinkDeveloper roles caused by DELETE-on-missing Flink statements returning 403 (not 404). The adapter now warns rather than errors on a 403 from statement DELETE, and retries CREATE on 409 name-conflicts to handle the async teardown race. ([#58](https://github.com/confluentinc/dbt-confluent/issues/58))
- Increase the HTTP client timeout to 60s so cold INFORMATION_SCHEMA lookups (notably the unified drift-catalog UNION ALL) no longer surface as "read operation timed out" on the default 5s budget.


## dbt-confluent 0.2.0 (2026-04-22)

### Features

- Removed `materialized_view` materialization (use `table`, see "Not Supported" section in MATERIALIZATIONS.md)
- Schema drift detection configurable via "on_schema_drift: 'fail' | 'ignore'". See MATERIALIZATIONS.md
- Use deterministic names for Flink statements, laying the groundwork for future orphan cleanup, idempotent re-runs, and crash recovery. The default `statement_name_prefix` changed from `dbt-confluent-` to `dbt-`. ([#29](https://github.com/confluentinc/dbt-confluent/issues/29))
- Mark internal/metadata queries with the hidden label so they are filtered by default in the Confluent UI ([#39](https://github.com/confluentinc/dbt-confluent/issues/39))
- Add custom endpoint configuration for private and other non-standard cluster urls ([#44](https://github.com/confluentinc/dbt-confluent/issues/44))

### Bugfixes

- Delete existing statements before re-submitting with the same deterministic name on `--full-refresh` ([#29](https://github.com/confluentinc/dbt-confluent/issues/29))
- Render model-level `PRIMARY KEY` constraints with the column list before the constraint expression (e.g. `PRIMARY KEY (col1, col2) NOT ENFORCED`), so Flink accepts the generated DDL. ([#31](https://github.com/confluentinc/dbt-confluent/issues/31))

### Misc

- Update to confluent-sql 0.3 ([#40](https://github.com/confluentinc/dbt-confluent/issues/40))
