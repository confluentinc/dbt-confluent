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
