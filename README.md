## dbt-confluent

This is a dbt adapter plugin for Confluent Cloud's Flink SQL service.

### Versioning
This adapter plugin follows [semantic versioning](https://semver.org/). The current version is v1.10.0, compatible with dbt Core v1.10.

For Confluent-specific functionality, we aim for backwards-compatibility wherever possible. Backwards-incompatible changes will be clearly communicated and limited to minor versions.

## Getting Started

### Setting up Locally
- Run `uv sync` to install dependencies
- Configure your Confluent Cloud credentials in a `profiles.yml` file or environment variables
