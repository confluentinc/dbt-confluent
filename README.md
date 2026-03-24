# dbt-confluent

The [dbt](https://www.getdbt.com/) adapter for [Confluent Cloud](https://confluent.io/confluent-cloud/) Flink SQL.

Build, test, and manage streaming data transformations on Confluent Cloud using dbt's familiar development workflow.

## Overview

`dbt-confluent` lets you use dbt to define and run SQL transformations on Confluent Cloud's fully managed Apache Flink service. It supports both batch-style and streaming materializations, enabling continuous data pipelines defined as dbt models.

Features:
- Standard dbt materializations (table, view, ephemeral) adapted for Flink SQL
- Streaming-native materializations (`streaming_table`, `streaming_source`) for continuous data pipelines
- Integration with Confluent Cloud connectors (e.g., Datagen/Faker) via `streaming_source`

See [Materializations](MATERIALIZATIONS.md) for the full list and details.

## Installation

```bash
pip install dbt-confluent
```

or with [uv](https://docs.astral.sh/uv/):

```bash
uv add dbt-confluent
```

Requires Python 3.10+.

## Configuration

After installing, scaffold a new project with:

```bash
dbt init my_project
```

Select `confluent` as the adapter and fill in the prompts for your Confluent Cloud credentials (API key, compute pool, environment, etc.).

### Concept mapping

Confluent Cloud Flink uses different terminology than traditional databases. Here's how dbt concepts map to Flink and Confluent Cloud:

| dbt concept | Flink concept | Confluent Cloud entity |
|---|---|---|
| `database` | Catalog | Environment |
| `schema` | Database | Kafka cluster |

### Schema configuration

Unlike most dbt adapters, `dbt-confluent` cannot create or drop schemas — a dbt schema maps to a Flink database (Kafka cluster) in Confluent Cloud, which is managed externally. Both the `dbname` in your `profiles.yml` and any model-level `schema` config must reference an existing Flink database by name:

```yaml
# dbt_project.yml
models:
  my_project:
    +schema: my-kafka-cluster
```

## Usage

### Streaming table

A streaming table creates a table and runs a continuous INSERT query against it:

```sql
-- models/pageviews_enriched.sql
{{
  config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'}
  )
}}

SELECT
  p.user_id,
  p.page_url,
  u.username
FROM {{ ref('pageviews') }} p
JOIN {{ ref('users') }} u ON p.user_id = u.user_id
```

### Streaming source

A streaming source creates a connector-backed source table. The model SQL defines the column definitions:

```sql
-- models/datagen_users.sql
{{
  config(
    materialized='streaming_source',
    connector='faker',
    with={'rows-per-second': '10'}
  )
}}

`user_id` INT,
`username` STRING,
`email` STRING
```

See [Materializations](MATERIALIZATIONS.md) for the full list and details.

## Known Limitations

- **No schema management**: Flink databases (Kafka clusters) cannot be created or dropped — they are managed in Confluent Cloud.
- **No table renames**: `ALTER TABLE RENAME` is not supported; to effectively rename a model you must drop and recreate the underlying table, which for `table`, `streaming_table`, and `streaming_source` materializations requires running with `--full-refresh`.
- **No transactions**: Flink SQL is non-transactional.
- **No snapshots**: Flink SQL lacks the batch operations (MERGE, UPDATE) required by dbt snapshots.
- **No incremental**: dbt's batch-incremental semantics does not map to Flink's continuous processing model. Use `streaming_table` instead.
- **Drift detection for WITH options**: Schema drift detection only verifies that user-specified `WITH` options exist with correct values. It cannot detect when options are removed from the config (because connectors may add default options that cannot be distinguished from user-specified ones). Use `--full-refresh` to change or remove WITH options. See [Materializations](MATERIALIZATIONS.md#schema-drift-detection) for details.

## Development

```bash
git clone https://github.com/confluentinc/dbt-confluent
cd dbt-confluent
uv sync --extra dev --extra test
```

### Code quality

```bash
uv run ruff check dbt/ tests/
uv run ruff format --check dbt/ tests/
```

### Running tests

Tests require a Confluent Cloud environment. Set the following environment variables (or add them to a `test.env` file):

```bash
export CONFLUENT_ENV_ID=env-xxxxxx
export CONFLUENT_ORG_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export CONFLUENT_COMPUTE_POOL_ID=lfcp-xxxxx
export CONFLUENT_CLOUD_PROVIDER=aws
export CONFLUENT_CLOUD_REGION=us-west-6
export CONFLUENT_TEST_DBNAME=dbname
export CONFLUENT_FLINK_API_KEY=xxx
export CONFLUENT_FLINK_API_SECRET=xxx
```

```bash
uv run pytest
```

## Versioning

This adapter follows [semantic versioning](https://semver.org/) and is versioned independently from dbt Core. Compatibility with dbt Core is declared via dependencies (currently requires `dbt-core~=1.11`).

## License

Apache-2.0 — see [LICENSE](./LICENSE) for details.
