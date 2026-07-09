dbt-confluent 0.3.0 (2026-07-09)

# Features

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

# Bugfixes

- Recover from a crash between `streaming_table`'s DDL and INSERT: a follow-up `dbt run` (no `--full-refresh`) now resubmits the missing INSERT instead of silently skipping the model. See #32 for the full lifecycle change. ([#33](https://github.com/confluentinc/dbt-confluent/issues/33))
- Changing a `streaming_source` model's `connector` config is now detected as schema drift. Previously the drift check only compared the `with` config, so a connector change was silently skipped and the old connector kept running.
- Schema drift checks no longer permanently leak their temp table (a real Kafka-backed topic) when the run fails between creating and dropping it: the temp name is now deterministic per model (no `invocation_id` suffix) and the check drops any leftover before creating, so the next run reclaims a leak.
- Single quotes in `with` option keys/values and in the `connector` config are now escaped (doubled) when rendered into `streaming_table`/`streaming_source` DDL, instead of breaking the statement or escaping the string literal.
- Temp tables created by the schema-drift check and the unit-test materialization are now dropped in the adapter's `post_model_hook` (which dbt runs in a try/finally around every materialization) instead of by inline drops at the end of the macro. The inline drops never ran when the materialization raised partway — e.g. when building the drift catalog failed, or when a later unit-test fixture errored and left the fixtures created so far undropped — leaking the temp table (a real Kafka-backed topic) until the next run's preemptive drop reclaimed it. Cleanup now runs on the failure path too, and goes through `drop_relation` (type-aware, cache-consistent) rather than a hardcoded `DROP TABLE`. The deterministic temp name plus a preemptive drop-if-exists remain as the backstop for hard-killed runs.
- The schema drift check now surfaces a retriable error when INFORMATION_SCHEMA returns no columns for the *existing* table (metadata propagation lag), instead of falsely reporting every model column as added and advising a destructive `--full-refresh`. This mirrors the guard that already existed for the temp table.


dbt-confluent 0.2.1 (2026-05-27)

# Bugfixes

- Fix `dbt run` failures under compute-pool-scoped FlinkDeveloper roles caused by DELETE-on-missing Flink statements returning 403 (not 404). The adapter now warns rather than errors on a 403 from statement DELETE, and retries CREATE on 409 name-conflicts to handle the async teardown race. ([#58](https://github.com/confluentinc/dbt-confluent/issues/58))
- Increase the HTTP client timeout to 60s so cold INFORMATION_SCHEMA lookups (notably the unified drift-catalog UNION ALL) no longer surface as "read operation timed out" on the default 5s budget.


dbt-confluent 0.2.0 (2026-04-22)

# Features

- Removed `materialized_view` materialization (use `table`, see "Not Supported" section in MATERIALIZATIONS.md)
- Schema drift detection configurable via "on_schema_drift: 'fail' | 'ignore'". See MATERIALIZATIONS.md
- Use deterministic names for Flink statements, laying the groundwork for future orphan cleanup, idempotent re-runs, and crash recovery. The default `statement_name_prefix` changed from `dbt-confluent-` to `dbt-`. ([#29](https://github.com/confluentinc/dbt-confluent/issues/29))
- Mark internal/metadata queries with the hidden label so they are filtered by default in the Confluent UI ([#39](https://github.com/confluentinc/dbt-confluent/issues/39))
- Add custom endpoint configuration for private and other non-standard cluster urls ([#44](https://github.com/confluentinc/dbt-confluent/issues/44))

# Bugfixes

- Delete existing statements before re-submitting with the same deterministic name on `--full-refresh` ([#29](https://github.com/confluentinc/dbt-confluent/issues/29))
- Render model-level `PRIMARY KEY` constraints with the column list before the constraint expression (e.g. `PRIMARY KEY (col1, col2) NOT ENFORCED`), so Flink accepts the generated DDL. ([#31](https://github.com/confluentinc/dbt-confluent/issues/31))

# Misc

- Update to confluent-sql 0.3 ([#40](https://github.com/confluentinc/dbt-confluent/issues/40))
