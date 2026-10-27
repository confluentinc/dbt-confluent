"""Unit tests for the SQL the `get_catalog_relations` macro renders."""

from pathlib import Path
from types import SimpleNamespace

import jinja2
import pytest

from dbt.adapters.confluent.impl import ConfluentAdapter
from tests.unit._helpers import relation

METADATA_MACROS = Path(__file__).parents[2] / "dbt/include/confluent/macros/adapters/metadata.sql"


def render_catalog_sql(relations):
    """The SQL `confluent__get_catalog_relations` submits for `relations`."""
    submitted = []

    def statement(*args, caller, **kwargs):
        submitted.append(caller())
        return ""

    environment = jinja2.Environment(extensions=["jinja2.ext.do"])
    environment.globals.update(
        statement=statement,
        adapter=ConfluentAdapter.__new__(ConfluentAdapter),
        load_result=lambda name: SimpleNamespace(table=[]),
        **{"return": lambda value: ""},
    )
    module = environment.from_string(METADATA_MACROS.read_text()).make_module()
    module.confluent__get_catalog_relations(SimpleNamespace(database="env-1"), relations)
    return submitted[0]


class TestGetCatalogRelationsMacro:
    @pytest.mark.parametrize("name", ["customer's_orders", "it''s"])
    def test_apostrophes_in_names_are_escaped_in_string_literals(self, name):
        sql = render_catalog_sql([relation(name, schema=name)])

        escaped = name.replace("'", "''")
        assert f"table_schema = '{escaped}'" in sql
        assert f"table_name = '{escaped}'" in sql
