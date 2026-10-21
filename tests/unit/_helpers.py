"""Shared helpers for unit tests."""

import agate
from confluent_sql.tableflow import TableflowTopic

from dbt.adapters.confluent.impl import ConfluentAdapter, ConfluentRelation, DriftMetadata


def relation(
    identifier: str, *, database: str = "env-1", schema: str = "cluster-a"
) -> ConfluentRelation:
    """A real ConfluentRelation for tests that need a relation object (value
    equality, backtick rendering) without a live connection. Defaults mirror
    the adapter's domain: database is the environment id, schema the Kafka
    cluster."""
    return ConfluentRelation.create(
        database=database, schema=schema, identifier=identifier, type="table"
    )


def make_topic(
    *,
    table_formats=("ICEBERG",),
    config=None,
    phase="RUNNING",
    error_message=None,
    failing_table_formats=None,
    storage=None,
) -> TableflowTopic:
    """A real `TableflowTopic`, parsed from realistic response JSON -- a bare `MagicMock`
    won't do anywhere `spec.config`/`spec.table_formats`/`spec.raw` need to be genuinely
    parsed, typed values (diffing) or JSON-serializable (debug logging), not attributes a
    mock would silently make up.
    """
    status: dict = {"phase": phase}
    if error_message is not None:
        status["error_message"] = error_message
    if failing_table_formats is not None:
        status["failing_table_formats"] = failing_table_formats
    return TableflowTopic.from_response(
        {
            "spec": {
                "display_name": "my_table",
                "storage": storage.to_spec()
                if storage is not None
                else {"kind": "Managed", "table_path": "s3://bucket/my_table"},
                "table_formats": list(table_formats),
                "environment": {"id": "env-1"},
                "kafka_cluster": {"id": "lkc-1"},
                "config": config or {},
            },
            "status": status,
        }
    )


def drift_metadata_row(
    *,
    section,
    col_name=None,
    dist_position=None,
    option_key=None,
    option_value=None,
    is_distributed=None,
    dist_buckets=None,
    is_materialized=None,
):
    """One row of `_get_drift_metadata`'s result, for make_drift_metadata."""
    return (
        section,
        col_name,
        dist_position,
        option_key,
        option_value,
        is_distributed,
        dist_buckets,
        is_materialized,
    )


_METADATA_COLUMNS = [
    "section",
    "col_name",
    "dist_position",
    "option_key",
    "option_value",
    "is_distributed",
    "dist_buckets",
    "is_materialized",
]

# Pin types so agate's inference doesn't coerce "YES" to a boolean (Confluent
# returns it as a string, and the partitioner compares against the literal "YES").
_METADATA_TYPES = [
    agate.Text(),  # section
    agate.Text(),  # col_name
    agate.Number(),  # dist_position
    agate.Text(),  # option_key
    agate.Text(),  # option_value
    agate.Text(),  # is_distributed
    agate.Number(),  # dist_buckets
    agate.Text(),  # is_materialized
]


def make_drift_metadata(rows) -> agate.Table:
    """An agate.Table shaped like `_get_drift_metadata`'s result, from drift_metadata_row rows."""
    return agate.Table(rows, column_names=_METADATA_COLUMNS, column_types=_METADATA_TYPES)


def partitioned_drift_metadata(rows) -> DriftMetadata:
    """The DriftMetadata `partition_drift_metadata` makes from drift_metadata_row rows: what
    check_for_schema_drift hands get_columns_from_dry_run and check_schema_drift."""
    return ConfluentAdapter._partition_drift_metadata(make_drift_metadata(rows))


def make_drift_columns(rows) -> agate.Table:
    """An agate.Table shaped like `_get_drift_columns`' result, from (table_name, col_name,
    data_type) tuples."""
    return agate.Table(
        rows,
        column_names=["table_name", "col_name", "data_type"],
        column_types=[agate.Text(), agate.Text(), agate.Text()],
    )
