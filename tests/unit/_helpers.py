"""Shared helpers for unit tests."""

import agate
from confluent_sql.tableflow import TableflowTopic

from dbt.adapters.confluent.impl import ConfluentRelation


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


def drift_catalog_row(
    *,
    section,
    table_name=None,
    col_name=None,
    data_type=None,
    dist_position=None,
    option_key=None,
    option_value=None,
    is_distributed=None,
    dist_buckets=None,
    is_materialized=None,
):
    """One row of `get_drift_catalog`'s result, for make_drift_catalog."""
    return (
        section,
        table_name,
        col_name,
        data_type,
        dist_position,
        option_key,
        option_value,
        is_distributed,
        dist_buckets,
        is_materialized,
    )


_CATALOG_COLUMNS = [
    "section",
    "table_name",
    "col_name",
    "data_type",
    "dist_position",
    "option_key",
    "option_value",
    "is_distributed",
    "dist_buckets",
    "is_materialized",
]

# Pin types so agate's inference doesn't coerce "YES" to a boolean (Confluent
# returns it as a string, and the partitioner compares against the literal "YES").
_CATALOG_TYPES = [
    agate.Text(),  # section
    agate.Text(),  # table_name
    agate.Text(),  # col_name
    agate.Text(),  # data_type
    agate.Number(),  # dist_position
    agate.Text(),  # option_key
    agate.Text(),  # option_value
    agate.Text(),  # is_distributed
    agate.Number(),  # dist_buckets
    agate.Text(),  # is_materialized
]


def make_drift_catalog(rows) -> agate.Table:
    """An agate.Table shaped like `get_drift_catalog`'s result, from drift_catalog_row rows."""
    return agate.Table(rows, column_names=_CATALOG_COLUMNS, column_types=_CATALOG_TYPES)
