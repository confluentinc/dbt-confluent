"""Shared helpers for unit tests."""

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
