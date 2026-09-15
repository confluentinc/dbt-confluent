"""Unit tests for miscellaneous impl.py code paths that don't need a live
connection — helper functions, class methods, and pure logic.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from dbt_common.contracts.constraints import ConstraintType, ModelLevelConstraint
from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.confluent.impl import (
    ConfluentAdapter,
    ConfluentRelation,
    _CleanupRegistry,
    _is_yes,
)
from tests.unit._helpers import relation


# ---------------------------------------------------------------------------
# _is_yes
# ---------------------------------------------------------------------------

class TestIsYes:
    def test_true_bool(self):
        assert _is_yes(True) is True

    def test_false_bool(self):
        assert _is_yes(False) is False

    def test_nonzero_int(self):
        assert _is_yes(1) is True
        assert _is_yes(42) is True

    def test_zero_int(self):
        assert _is_yes(0) is False

    def test_yes_string(self):
        assert _is_yes("YES") is True

    def test_yes_lowercase(self):
        assert _is_yes("yes") is True

    def test_no_string(self):
        assert _is_yes("NO") is False

    def test_other_string(self):
        assert _is_yes("1") is False
        assert _is_yes("TRUE") is False


# ---------------------------------------------------------------------------
# ConfluentRelation
# ---------------------------------------------------------------------------

class TestConfluentRelation:
    def test_quoted_wraps_identifier_in_backticks(self):
        rel = relation("my_table")
        assert rel.quoted("my_table") == "`my_table`"

    def test_quoted_raises_on_backtick_in_identifier(self):
        rel = relation("my_table")
        with pytest.raises(CompilationError, match="can't be used in identifiers"):
            rel.quoted("bad`name")

    def test_make_confluent_fqn_all_parts(self):
        rel = ConfluentRelation.create(
            database="env-1", schema="cluster-a", identifier="my_table", type="table"
        )
        assert rel.make_confluent_fqn() == "`env-1`.`cluster-a`.`my_table`"

    def test_make_confluent_fqn_skips_none_parts(self):
        rel = ConfluentRelation.create(
            database=None, schema="cluster-a", identifier="my_table", type="table"
        )
        assert rel.make_confluent_fqn() == "`cluster-a`.`my_table`"


# ---------------------------------------------------------------------------
# ConfluentAdapter — class-level methods (no connection needed)
# ---------------------------------------------------------------------------

def _adapter():
    """Bypass __init__ for classmethod/staticmethod tests."""
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    adapter._deferred_cleanups = _CleanupRegistry()
    return adapter


class TestConfluentAdapterClassMethods:
    def test_quote_wraps_in_backticks(self):
        assert ConfluentAdapter.quote("my_table") == "`my_table`"

    def test_date_function(self):
        assert ConfluentAdapter.date_function() == "CURRENT_TIMESTAMP"

    def test_convert_text_type(self):
        table = MagicMock()
        assert ConfluentAdapter.convert_text_type(table, 0) == "STRING"

    def test_convert_integer_type(self):
        table = MagicMock()
        assert ConfluentAdapter.convert_integer_type(table, 0) == "INT"

    def test_convert_datetime_type(self):
        table = MagicMock()
        assert ConfluentAdapter.convert_datetime_type(table, 0) == "TIMESTAMP"

    def test_convert_number_type_with_decimals(self):
        import agate
        table = MagicMock(spec=agate.Table)
        table.aggregate.return_value = 2  # non-zero → FLOAT
        assert ConfluentAdapter.convert_number_type(table, 0) == "FLOAT"

    def test_convert_number_type_no_decimals(self):
        import agate
        table = MagicMock(spec=agate.Table)
        table.aggregate.return_value = 0  # zero → INT
        assert ConfluentAdapter.convert_number_type(table, 0) == "INT"


# ---------------------------------------------------------------------------
# drop_schema (always raises)
# ---------------------------------------------------------------------------

class TestDropSchema:
    def test_drop_schema_raises(self):
        adapter = _adapter()
        rel = relation("my_table")
        with pytest.raises(DbtDatabaseError, match="Cannot drop schema"):
            adapter.drop_schema(rel)


# ---------------------------------------------------------------------------
# render_model_constraint
# ---------------------------------------------------------------------------

class TestRenderModelConstraint:
    def test_primary_key_no_name(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.primary_key,
            name=None,
            columns=["id", "ts"],
            expression=None,
        )
        result = ConfluentAdapter.render_model_constraint(constraint)
        assert result == "primary key (id, ts)"

    def test_primary_key_with_name(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.primary_key,
            name="pk_orders",
            columns=["order_id"],
            expression=None,
        )
        result = ConfluentAdapter.render_model_constraint(constraint)
        assert result == "constraint pk_orders primary key (order_id)"

    def test_primary_key_with_expression(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.primary_key,
            name=None,
            columns=["id"],
            expression="NOT ENFORCED",
        )
        result = ConfluentAdapter.render_model_constraint(constraint)
        assert result == "primary key (id) NOT ENFORCED"

    def test_non_primary_key_delegates_to_super(self):
        # unique/check/foreign_key are handled by the base class; just check
        # that we don't crash and that we don't intercept them.
        constraint = ModelLevelConstraint(
            type=ConstraintType.unique,
            name=None,
            columns=["col"],
            expression=None,
        )
        # Base class may return None for types it doesn't recognize — we only
        # care that ConfluentAdapter did NOT apply the primary key override.
        result = ConfluentAdapter.render_model_constraint(constraint)
        # The result must not look like a primary key clause.
        assert result is None or "primary key" not in str(result)


# ---------------------------------------------------------------------------
# generate_schema_check_temp_name
# ---------------------------------------------------------------------------

class TestGenerateSchemaCheckTempName:
    def test_prefix_applied(self):
        adapter = _adapter()
        assert adapter.generate_schema_check_temp_name("orders") == (
            "__dbt_tmp_schema_check_orders"
        )

    def test_different_identifiers_give_different_names(self):
        adapter = _adapter()
        a = adapter.generate_schema_check_temp_name("orders")
        b = adapter.generate_schema_check_temp_name("users")
        assert a != b


# ---------------------------------------------------------------------------
# get_statement_name
# ---------------------------------------------------------------------------

class TestGetStatementName:
    def _adapter_with_credentials(self, prefix="dbt-"):
        adapter = _adapter()
        credentials = MagicMock()
        credentials.statement_name_prefix = prefix
        config = MagicMock()
        config.credentials = credentials
        adapter.config = config
        return adapter

    def test_builds_name_from_project_and_model(self):
        adapter = self._adapter_with_credentials("dbt-")
        name = adapter.get_statement_name("my_model", "my_project")
        # sanitize_statement_name lowercases and may add a hash for illegal chars
        assert "my-project" in name
        assert "my-model" in name

    def test_override_bypasses_prefix(self):
        adapter = self._adapter_with_credentials("dbt-")
        name = adapter.get_statement_name(
            "my_model", "my_project", statement_name_override="custom-name"
        )
        assert "custom-name" in name
        assert "my-project" not in name

    def test_suffix_appended(self):
        adapter = self._adapter_with_credentials("dbt-")
        name = adapter.get_statement_name(
            "model", "proj", suffix="-check"
        )
        assert name.endswith("-check") or "-check" in name

    def test_override_with_suffix(self):
        adapter = self._adapter_with_credentials("dbt-")
        name = adapter.get_statement_name(
            "model", "proj", suffix="-check", statement_name_override="base"
        )
        assert "base-check" in name


# ---------------------------------------------------------------------------
# parse_unit_test_ctes
# ---------------------------------------------------------------------------

class TestParseUnitTestCtes:
    def _adapter(self):
        return _adapter()

    def test_single_cte_parsed(self):
        adapter = self._adapter()
        extra_ctes = [{"sql": "__dbt__cte__my_fixture as (\nSELECT 1 AS id\n)"}]
        compiled_sql = "with __dbt__cte__my_fixture as (\nSELECT 1 AS id\n) SELECT * FROM my_model"
        result = adapter.parse_unit_test_ctes(extra_ctes, compiled_sql)

        assert len(result["ctes"]) == 1
        cte = result["ctes"][0]
        assert cte["cte_name"] == "__dbt__cte__my_fixture"
        assert cte["original_identifier"] == "my_fixture"
        assert "SELECT 1 AS id" in cte["body"]

    def test_main_sql_stripped_of_cte_prefix(self):
        adapter = self._adapter()
        cte_sql = "__dbt__cte__fix as (\nSELECT 1\n)"
        extra_ctes = [{"sql": cte_sql}]
        compiled_sql = f"with{cte_sql} SELECT * FROM t"
        result = adapter.parse_unit_test_ctes(extra_ctes, compiled_sql)
        assert result["main_sql"] == "SELECT * FROM t"

    def test_no_ctes_returns_compiled_sql_unchanged(self):
        adapter = self._adapter()
        sql = "SELECT * FROM t"
        result = adapter.parse_unit_test_ctes([], sql)
        assert result["ctes"] == []
        assert result["main_sql"] == sql

    def test_malformed_cte_raises(self):
        adapter = self._adapter()
        extra_ctes = [{"sql": "no_as_keyword_here"}]
        with pytest.raises(ValueError, match="expected CTE format"):
            adapter.parse_unit_test_ctes(extra_ctes, "SELECT 1")
