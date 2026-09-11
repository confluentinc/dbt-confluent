"""Shared helpers for unit tests."""

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


def make_topic(*, table_formats=("ICEBERG",), config=None, phase="RUNNING") -> TableflowTopic:
    """A real `TableflowTopic`, parsed from realistic response JSON -- a bare `MagicMock`
    won't do anywhere `spec.config`/`spec.table_formats`/`spec.raw` need to be genuinely
    parsed, typed values (diffing) or JSON-serializable (debug logging), not attributes a
    mock would silently make up.
    """
    return TableflowTopic.from_response(
        {
            "spec": {
                "display_name": "my_table",
                "storage": {"kind": "Managed", "table_path": "s3://bucket/my_table"},
                "table_formats": list(table_formats),
                "environment": {"id": "env-1"},
                "kafka_cluster": {"id": "lkc-1"},
                "config": config or {},
            },
            "status": {"phase": phase},
        }
    )
