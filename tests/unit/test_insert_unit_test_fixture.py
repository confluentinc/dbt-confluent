"""Unit tests for ConfluentAdapter.insert_unit_test_fixture.

dbt-core's own fixture rendering (format_row/safe_cast) can't emit a literal
for an ARRAY/MAP/ROW/MULTISET/interval column - a `given` row that supplies a
concrete value for one fails the INSERT with a raw Flink SQL parse error
(confirmed live: `Encountered '[' ...`), not something a user could act on.
insert_unit_test_fixture bypasses the generic `statement()` macro for just
this one INSERT so it can catch that failure and, when `original_relation`
actually has such a column, re-raise naming it - see dry_run.is_not_castable.
"""

from unittest.mock import MagicMock, patch

import pytest
from confluent_sql.statement import Column as RawColumn
from confluent_sql.types import ColumnTypeDefinition
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.confluent.impl import ConfluentAdapter


def _adapter() -> ConfluentAdapter:
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    adapter.connections = MagicMock()
    return adapter


class TestInsertUnitTestFixture:
    def test_successful_insert_does_not_look_up_unsupported_columns(self):
        adapter = _adapter()
        adapter.execute = MagicMock(return_value=(MagicMock(), MagicMock()))

        with patch("dbt.adapters.confluent.impl.dry_run.get_raw_columns") as get_raw_columns:
            adapter.insert_unit_test_fixture("temp", "original", "select 1 as id")

        adapter.execute.assert_called_once_with("insert into temp select 1 as id")
        get_raw_columns.assert_not_called()

    def test_failure_with_unsupported_column_names_it_in_a_clearer_error(self):
        adapter = _adapter()
        adapter.execute = MagicMock(side_effect=DbtDatabaseError("Encountered '[' at line 1"))

        with patch(
            "dbt.adapters.confluent.impl.dry_run.get_raw_columns",
            return_value=[
                RawColumn(name="id", type=ColumnTypeDefinition(type="BIGINT", nullable=True)),
                RawColumn(
                    name="tags",
                    type=ColumnTypeDefinition(
                        type="ARRAY",
                        nullable=True,
                        element_type=ColumnTypeDefinition(type="INT", nullable=True),
                    ),
                ),
            ],
        ):
            with pytest.raises(DbtDatabaseError) as exc_info:
                adapter.insert_unit_test_fixture("temp", "original", "select ...")

        assert "column(s) tags" in str(exc_info.value)
        assert "Encountered '[' at line 1" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, DbtDatabaseError)

    def test_failure_without_any_unsupported_column_reraises_unchanged(self):
        adapter = _adapter()
        original_error = DbtDatabaseError("some other failure")
        adapter.execute = MagicMock(side_effect=original_error)

        with patch(
            "dbt.adapters.confluent.impl.dry_run.get_raw_columns",
            return_value=[
                RawColumn(name="id", type=ColumnTypeDefinition(type="BIGINT", nullable=True))
            ],
        ):
            with pytest.raises(DbtDatabaseError) as exc_info:
                adapter.insert_unit_test_fixture("temp", "original", "select ...")

        assert exc_info.value is original_error
