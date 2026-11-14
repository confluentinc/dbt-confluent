"""Pin relation.render() so a change is deliberate, with a `breaking` changelog entry."""

from tests.unit._helpers import relation


def test_relation_renders_backticked_catalog_database_table():
    """A change here changes the compiled SQL of every model that uses ref()/source(), so every
    running streaming_table reports SQL logic drift after users upgrade (see statement_sql.py's
    maintenance note). Make it deliberately, with a `breaking` changelog entry."""
    assert relation("big_orders").render() == "`env-1`.`cluster-a`.`big_orders`"
