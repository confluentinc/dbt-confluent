# Changelog for dbt-confluent

## 0.1.1 (2026-04-17)

### Bugfixes

- Pin `confluent-sql` to `>=0.1,<0.3`. The previous `~=0.1` constraint resolved to `confluent-sql 0.3.0`, which renamed the `environment` keyword argument of `connect()` to `environment_id`. Installing `dbt-confluent==0.1.0` from PyPI therefore produced a broken combination that failed on every connection with `TypeError: connect() got an unexpected keyword argument 'environment'` (surfaced as `confluent_sql connection error` during `dbt debug`). This patch restricts the dependency to versions that still accept the `environment` argument.
