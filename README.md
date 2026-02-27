## dbt-confluent

This is a dbt adapter plugin for Confluent Cloud's Flink SQL service.

### Versioning
This adapter plugin follows [semantic versioning](https://semver.org/). The current version is v1.10.0, compatible with dbt Core v1.10.

For Confluent-specific functionality, we aim for backwards-compatibility wherever possible. Backwards-incompatible changes will be clearly communicated and limited to minor versions.

## Getting Started

### Installation

Install from the repository (requires access):

```bash
uv pip install "dbt-confluent @ git+https://github.com/confluentinc/dbt-confluent"
```

Or from a built wheel:

```bash
uv pip install dbt_confluent-<version>-py3-none-any.whl
```

Configure your Confluent Cloud credentials in a `profiles.yml` file or environment variables.

### Development Setup

Clone the repository (with submodules) and install dependencies:

```bash
git clone --recurse-submodules <repo-url>
cd dbt-confluentcloud
uv sync --extra dev --extra test
```

Run tests with `uv run pytest`.
Tests require Confluent Cloud credentials to be set through the following env vars:
```
export CONFLUENT_ENV_ID = <value>
export CONFLUENT_ORG_ID = <value>
export CONFLUENT_COMPUTE_POOL_ID = <value>
export CONFLUENT_CLOUD_PROVIDER = <value>
export CONFLUENT_CLOUD_REGION = <value>
export CONFLUENT_TEST_DBNAME = <value>
export CONFLUENT_FLINK_API_KEY = <value>
export CONFLUENT_FLINK_API_SECRET = <value>
```

To install as an editable dependency in another project:

```bash
uv pip install -e /path/to/dbt-confluentcloud
```

Note: `dbt-confluent` is a namespace package (`dbt.adapters.confluent`), which means goto-definition and other static analysis features may not resolve the adapter's source code in editable mode. If your LSP does not resolve `dbt.adapters.confluent` imports, add the source path to your project's pyright configuration:

```toml
# pyproject.toml
[tool.pyright]
extraPaths = ["/path/to/dbt-confluentcloud"]
```

### Known Limitations

- **Schema Management**: Cannot create or drop schemas/databases (managed in Confluent Cloud)
- **Table Renames**: ALTER TABLE is not supported; tables cannot be renamed
- **Transactions**: Confluent Cloud Flink SQL is non-transactional
- **DBT Snapshots**: Flink SQL does not provide the transaction operations (MERGE, UPDATE with CTEs) required to implement Type 2 Slowly Changing Dimensions in the batch-processing style that dbt snapshots use.
