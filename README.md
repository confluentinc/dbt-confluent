## dbt-confluent

This is a dbt adapter plugin for Confluent Cloud's Flink SQL service.

### Versioning
This adapter plugin follows [semantic versioning](https://semver.org/). The current version is v1.10.0, compatible with dbt Core v1.10.

For Confluent-specific functionality, we aim for backwards-compatibility wherever possible. Backwards-incompatible changes will be clearly communicated and limited to minor versions.

## Getting Started

### Setting up Locally
- Run `uv sync` to install dependencies
- Configure your Confluent Cloud credentials in a `profiles.yml` file or environment variables

## Known Limitations

### Snapshots

dbt snapshots (Type 2 Slowly Changing Dimensions) are **not supported** in dbt-confluent due to fundamental differences between batch processing and Flink's streaming architecture.

#### Why Snapshots Aren't Supported

Flink SQL does not provide the transaction operations (MERGE, UPDATE with CTEs) required to implement Type 2 Slowly Changing Dimensions in the batch-processing style that dbt snapshots use. Attempting to use snapshots will result in compilation or runtime errors.

#### Recommended Alternatives for Change Tracking

For tracking historical changes in Confluent Flink SQL, consider these streaming-native approaches:

1. **Flink Temporal Tables**: Query data as it existed at specific points in time using Flink's built-in temporal semantics
2. **CDC (Change Data Capture) Streams**: Consume change events directly from Kafka topics with upsert and retract semantics
3. **Versioned Tables**: Leverage Flink's state backend for automatic change tracking in streaming queries
4. **Event Sourcing Pattern**: Store all change events in Kafka and materialize views as needed
5. **Hybrid Approach**: Use Flink to stream changes to a traditional data warehouse (Snowflake, BigQuery, etc.) where dbt snapshots can be applied

For detailed examples and patterns, refer to [Confluent's documentation on temporal tables](https://docs.confluent.io/cloud/current/flink/reference/queries/temporal-tables.html) and CDC patterns.

### Other Limitations

- **Schema Management**: Cannot create or drop schemas/databases (managed in Confluent Cloud)
- **Table Renames**: ALTER TABLE is not supported; tables cannot be renamed
- **Transactions**: Confluent Cloud Flink SQL is non-transactional
