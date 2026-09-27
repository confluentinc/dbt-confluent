"""Unit tests for get_tested_model_columns / set_relations_cache's contract
columns cache.

Unit tests execute against a manifest trimmed down to just the unit test node
(dbt.parser.unit_tests.UnitTestManifestLoader builds a fresh, near-empty
Manifest), so by the time our unit-test materialization needs the tested
model's columns, its config (contract, columns) is gone -- only its
unique_id string, and the unit test's own compiled query, survive.
set_relations_cache is called with the full manifest's nodes before that
(dbt.task.runnable.GraphRunnableTask.populate_adapter_cache), so an enforced
contract's declared columns are captured there instead. Absent a contract,
get_tested_model_columns falls back to a Flink dry run - see
dbt.adapters.confluent.dry_run and test_dry_run.py - of the unit test's own
compiled query (fixture inputs already substituted with real temp tables by
that point), so a model doesn't need to already be `dbt run` before it's
unit-testable.
"""

from unittest.mock import MagicMock, patch

import pytest
from confluent_sql.statement import Column as RawColumn
from confluent_sql.types import ColumnTypeDefinition
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.confluent.impl import ConfluentAdapter


def _adapter() -> ConfluentAdapter:
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    adapter._contract_columns_by_unique_id = {}
    # set_relations_cache delegates to the base implementation for the actual
    # cache population, which needs a real cache/lock and would otherwise try
    # to introspect a live connection; that behavior isn't under test here.
    adapter.cache = MagicMock()
    adapter._relations_cache_for_schemas = MagicMock()
    return adapter


def _node(unique_id: str, *, enforced: bool, columns: dict[str, str]) -> MagicMock:
    node = MagicMock()
    node.unique_id = unique_id
    node.contract = MagicMock(enforced=enforced)
    node.columns = {name: MagicMock(data_type=data_type) for name, data_type in columns.items()}
    return node


# ---------------------------------------------------------------------------
# set_relations_cache -> _contract_columns_by_unique_id
# ---------------------------------------------------------------------------


class TestContractColumnsCache:
    def test_enforced_contract_columns_are_cached(self):
        adapter = _adapter()
        adapter.set_relations_cache(
            [_node("model.pkg.orders", enforced=True, columns={"Order_Id": "BIGINT"})],
            clear=False,
        )
        assert adapter._contract_columns_by_unique_id == {
            "model.pkg.orders": {"order_id": "BIGINT"}
        }

    def test_unenforced_contract_is_not_cached(self):
        adapter = _adapter()
        adapter.set_relations_cache(
            [_node("model.pkg.orders", enforced=False, columns={"id": "BIGINT"})],
            clear=False,
        )
        assert adapter._contract_columns_by_unique_id == {}

    def test_node_without_contract_attr_is_not_cached(self):
        adapter = _adapter()
        bare = MagicMock(spec=["unique_id"])
        bare.unique_id = "model.pkg.orders"
        adapter.set_relations_cache([bare], clear=False)
        assert adapter._contract_columns_by_unique_id == {}


# ---------------------------------------------------------------------------
# get_tested_model_columns
# ---------------------------------------------------------------------------


class TestGetTestedModelColumns:
    def test_prefers_cached_contract_columns_over_dry_run(self):
        adapter = _adapter()
        adapter._contract_columns_by_unique_id["model.pkg.orders"] = {"id": "BIGINT"}
        adapter._dry_run_columns = MagicMock(side_effect=AssertionError("should not dry run"))

        columns = adapter.get_tested_model_columns("model.pkg.orders", "select 1")

        assert [(c.name, c.data_type) for c in columns] == [("id", "BIGINT")]
        adapter._dry_run_columns.assert_not_called()

    def test_falls_back_to_dry_run_when_no_contract_cached(self):
        adapter = _adapter()
        dry_run_result = [MagicMock(name="id")]
        adapter._dry_run_columns = MagicMock(return_value=dry_run_result)

        result = adapter.get_tested_model_columns("model.pkg.orders", "select 1")

        adapter._dry_run_columns.assert_called_once_with("select 1", None)
        assert result is dry_run_result

    def test_passes_needed_columns_through_to_dry_run(self):
        adapter = _adapter()
        adapter._dry_run_columns = MagicMock(return_value=[])

        adapter.get_tested_model_columns("model.pkg.orders", "select 1", ["id", "name"])

        adapter._dry_run_columns.assert_called_once_with("select 1", ["id", "name"])


# ---------------------------------------------------------------------------
# _dry_run_columns' needed_columns scoping
# ---------------------------------------------------------------------------


def _raw_columns() -> list[RawColumn]:
    """One ordinary castable column (`id`) plus one NOT_CASTABLE_TYPES column
    (`tags`, ARRAY) - representative of a model that selects an unsupported
    type the unit test itself never touches."""
    return [
        RawColumn(name="id", type=ColumnTypeDefinition(type="BIGINT", nullable=True)),
        RawColumn(
            name="tags",
            type=ColumnTypeDefinition(
                type="ARRAY",
                nullable=True,
                element_type=ColumnTypeDefinition(type="INT", nullable=True),
            ),
        ),
    ]


class TestDryRunColumnsNeededColumnsScoping:
    """A unit test's `expect` block only ever looks up the columns its own
    fixture rows name (dbt-core's format_row) - so only those need to survive
    try_get_castable_type's stricter checks; every other column just needs a
    valid DDL type, via get_ddl_type, for name/quoting purposes."""

    def test_unsupported_column_outside_needed_columns_does_not_raise(self):
        adapter = _adapter()
        adapter.connections = MagicMock()

        with patch(
            "dbt.adapters.confluent.impl.dry_run.get_raw_columns",
            return_value=_raw_columns(),
        ):
            columns = adapter._dry_run_columns("select 1", needed_columns=["id"])

        assert [(c.name, c.data_type) for c in columns] == [
            ("id", "BIGINT"),
            ("tags", "ARRAY<INT>"),
        ]

    def test_unsupported_column_inside_needed_columns_still_raises(self):
        adapter = _adapter()
        adapter.connections = MagicMock()

        with patch(
            "dbt.adapters.confluent.impl.dry_run.get_raw_columns",
            return_value=_raw_columns(),
        ):
            with pytest.raises(DbtDatabaseError, match="'ARRAY'"):
                adapter._dry_run_columns("select 1", needed_columns=["id", "tags"])

    def test_no_needed_columns_requires_every_column_to_be_castable(self):
        adapter = _adapter()
        adapter.connections = MagicMock()

        with patch(
            "dbt.adapters.confluent.impl.dry_run.get_raw_columns",
            return_value=_raw_columns(),
        ):
            with pytest.raises(DbtDatabaseError, match="'ARRAY'"):
                adapter._dry_run_columns("select 1")
