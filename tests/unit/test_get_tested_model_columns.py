"""Unit tests for get_tested_model_columns / set_relations_cache's contract
cache / _dry_run_columns / _dry_run_castable_type.

Unit tests execute against a manifest trimmed down to just the unit test node
(dbt.parser.unit_tests.UnitTestManifestLoader builds a fresh, near-empty
Manifest), so by the time our unit-test materialization needs the tested
model's columns, its config (contract, columns) is gone -- only its
unique_id string, and the unit test's own compiled query, survive.
set_relations_cache is called with the full manifest's nodes before that
(dbt.task.runnable.GraphRunnableTask.populate_adapter_cache), so an enforced
contract's declared columns are captured there instead. Absent a contract,
_dry_run_columns resolves the same information from a Flink dry run of the
unit test's own compiled query (fixture inputs already substituted with real
temp tables by that point), so a model doesn't need to already be `dbt run`
before it's unit-testable.
"""

from unittest.mock import MagicMock

import pytest
from confluent_sql.types import ColumnTypeDefinition
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.confluent.impl import ConfluentAdapter, _dry_run_castable_type


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


def _statement_response(
    *, phase: str = "COMPLETED", sql_kind: str = "SELECT", columns: list[tuple[str, dict]] | None
) -> dict:
    """A raw statements-API response shaped like Statement.from_response expects,
    matching what a real dry-run submission returns (confirmed against a live
    Confluent Cloud environment): already COMPLETED, with its full schema, in
    the submission response itself - see _dry_run_columns for why that matters.

    `columns=None` with sql_kind="SELECT" isn't a real server response (a
    non-DDL statement always gets a schema); pass a DDL sql_kind for the
    schema-less case (has_schema() is false, so nothing reads the columns).
    """
    traits = None
    if phase not in ("FAILED", "PENDING"):
        schema = (
            {"columns": [{"name": name, "type": type_} for name, type_ in columns]}
            if columns is not None
            else None
        )
        traits = {
            "sql_kind": sql_kind,
            "schema": schema,
            "is_append_only": True,
            "is_bounded": True,
        }
    return {
        "name": "dbt-adapter-test-abc123",
        "metadata": {"uid": "abc123"},
        "spec": {"statement": "select 1"},
        "status": {"phase": phase, "detail": "", "traits": traits},
    }


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

        adapter._dry_run_columns.assert_called_once_with("select 1")
        assert result is dry_run_result


# ---------------------------------------------------------------------------
# _dry_run_columns
# ---------------------------------------------------------------------------


class TestDryRunColumns:
    def _adapter_with_response(self, response: dict) -> ConfluentAdapter:
        adapter = _adapter()
        connection = MagicMock()
        connection.credentials.statement_name_prefix = "dbt-adapter-test-"
        connection.handle._execute_statement.return_value = response
        adapter.connections = MagicMock()
        adapter.connections.get_thread_connection.return_value = connection
        return adapter

    def test_resolves_columns_from_statement_schema(self):
        response = _statement_response(
            columns=[
                ("id", {"type": "BIGINT", "nullable": True}),
                ("amount", {"type": "DECIMAL", "nullable": True, "precision": 10, "scale": 2}),
            ]
        )
        adapter = self._adapter_with_response(response)

        columns = adapter._dry_run_columns("select id, amount from orders")

        assert [(c.name, c.data_type) for c in columns] == [
            ("id", "BIGINT"),
            ("amount", "DECIMAL(10, 2)"),
        ]

    def test_passes_dry_run_statement_property(self):
        adapter = self._adapter_with_response(_statement_response(columns=[]))

        adapter._dry_run_columns("select 1")

        call = adapter.connections.get_thread_connection().handle._execute_statement.call_args
        assert call.args[4] == {"sql.dry-run": "true"}

    def test_raises_on_failed_statement(self):
        adapter = self._adapter_with_response(_statement_response(phase="FAILED", columns=None))

        with pytest.raises(DbtDatabaseError, match="Dry run failed"):
            adapter._dry_run_columns("select tags from orders")

    def test_composite_column_type_raises_a_clear_error(self):
        response = _statement_response(columns=[("tags", {"type": "ARRAY", "nullable": True})])
        adapter = self._adapter_with_response(response)

        with pytest.raises(DbtDatabaseError, match="ARRAY"):
            adapter._dry_run_columns("select tags from orders")

    def test_no_schema_gives_no_columns(self):
        response = _statement_response(sql_kind="CREATE_TABLE", columns=None)
        adapter = self._adapter_with_response(response)

        assert adapter._dry_run_columns("create table t (...)") == []


# ---------------------------------------------------------------------------
# _dry_run_castable_type
# ---------------------------------------------------------------------------


class TestDryRunCastableType:
    @pytest.mark.parametrize(
        "flink_type",
        ["BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DATE", "STRING"],
    )
    def test_bare_types_pass_through_unchanged(self, flink_type):
        type_def = ColumnTypeDefinition(type=flink_type, nullable=True)
        assert _dry_run_castable_type(type_def) == flink_type

    def test_decimal_includes_precision_and_scale(self):
        type_def = ColumnTypeDefinition(type="DECIMAL", nullable=True, precision=10, scale=2)
        assert _dry_run_castable_type(type_def) == "DECIMAL(10, 2)"

    def test_decimal_without_precision_falls_back_to_bare_name(self):
        type_def = ColumnTypeDefinition(type="DECIMAL", nullable=True)
        assert _dry_run_castable_type(type_def) == "DECIMAL"

    def test_varchar_includes_length(self):
        type_def = ColumnTypeDefinition(type="VARCHAR", nullable=True, length=255)
        assert _dry_run_castable_type(type_def) == "VARCHAR(255)"

    def test_timestamp_includes_precision(self):
        type_def = ColumnTypeDefinition(type="TIMESTAMP", nullable=True, precision=3)
        assert _dry_run_castable_type(type_def) == "TIMESTAMP(3)"

    @pytest.mark.parametrize(
        ("dry_run_type", "castable_type"),
        [
            ("TIME_WITHOUT_TIME_ZONE", "TIME"),
            ("TIMESTAMP_WITHOUT_TIME_ZONE", "TIMESTAMP"),
            ("TIMESTAMP_WITH_LOCAL_TIME_ZONE", "TIMESTAMP_LTZ"),
        ],
    )
    def test_internal_type_names_are_aliased_to_their_cast_keyword(
        self, dry_run_type, castable_type
    ):
        """A dry run reports these by Flink's internal/full type name (confirmed
        against a live Confluent Cloud environment), but CAST(x AS ...) only
        accepts the short keyword form - e.g. `Unknown identifier
        'TIMESTAMP_WITHOUT_TIME_ZONE'` otherwise."""
        type_def = ColumnTypeDefinition(type=dry_run_type, nullable=True, precision=3)
        assert _dry_run_castable_type(type_def) == f"{castable_type}(3)"

    @pytest.mark.parametrize("flink_type", ["ARRAY", "MAP", "MULTISET", "ROW", "RAW"])
    def test_composite_types_raise_a_clear_error(self, flink_type):
        type_def = ColumnTypeDefinition(type=flink_type, nullable=True)
        with pytest.raises(DbtDatabaseError, match=flink_type):
            _dry_run_castable_type(type_def)
