"""Unit tests for dry_run.get_schema / dry_run.get_ddl_type / dry_run.try_get_castable_type.

Absent an enforced contract, get_tested_model_columns (impl.py) resolves a
unit-tested model's columns from a Flink dry run (`sql.dry-run`) of the unit
test's own compiled query (fixture inputs already substituted with real temp
tables by that point), so a model doesn't need to already be `dbt run` before
it's unit-testable.
"""

from unittest.mock import MagicMock

import pytest
from confluent_sql.types import ColumnTypeDefinition, RowColumn
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.confluent import dry_run


def _statement_response(
    *, phase: str = "COMPLETED", sql_kind: str = "SELECT", columns: list[tuple[str, dict]] | None
) -> dict:
    """A raw statements-API response shaped like Statement.from_response expects,
    matching what a real dry-run submission returns (confirmed against a live
    Confluent Cloud environment): already COMPLETED, with its full schema, in
    the submission response itself - see dry_run.get_schema for why that matters.

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


def _connection_with_response(response: dict) -> MagicMock:
    connection = MagicMock()
    connection.credentials.statement_name_prefix = "dbt-adapter-test-"
    connection.handle._execute_statement.return_value = response
    return connection


# ---------------------------------------------------------------------------
# dry_run.get_schema
# ---------------------------------------------------------------------------


class TestGetSchema:
    def test_resolves_columns_from_statement_schema(self):
        response = _statement_response(
            columns=[
                ("id", {"type": "BIGINT", "nullable": True}),
                ("amount", {"type": "DECIMAL", "nullable": True, "precision": 10, "scale": 2}),
            ]
        )
        connection = _connection_with_response(response)

        schema = dry_run.get_schema(connection, "select id, amount from orders")

        assert [(c.name, c.data_type) for c in schema.columns] == [
            ("id", "BIGINT"),
            ("amount", "DECIMAL(10, 2)"),
        ]

    def test_passes_dry_run_statement_property(self):
        connection = _connection_with_response(_statement_response(columns=[]))

        dry_run.get_schema(connection, "select 1")

        call = connection.handle._execute_statement.call_args
        assert call.args[4] == {"sql.dry-run": "true"}

    def test_raises_on_failed_statement(self):
        connection = _connection_with_response(_statement_response(phase="FAILED", columns=None))

        with pytest.raises(DbtDatabaseError, match="Dry run failed"):
            dry_run.get_schema(connection, "select tags from orders")

    def test_unrepresentable_column_type_raises_a_clear_error(self):
        response = _statement_response(columns=[("blob", {"type": "RAW", "nullable": True})])
        connection = _connection_with_response(response)

        with pytest.raises(DbtDatabaseError, match="RAW"):
            dry_run.get_schema(connection, "select blob from orders")

    def test_constructed_column_type_is_supported(self):
        """Unlike try_get_castable_type, get_schema uses the lenient
        get_ddl_type - a constructed type it can't cast a fixture value into
        is still a type it can faithfully reproduce the DDL of."""
        response = _statement_response(
            columns=[
                (
                    "tags",
                    {
                        "type": "ARRAY",
                        "nullable": True,
                        "element_type": {"type": "INT", "nullable": True},
                    },
                )
            ]
        )
        connection = _connection_with_response(response)

        schema = dry_run.get_schema(connection, "select tags from orders")

        assert [(c.name, c.data_type) for c in schema.columns] == [("tags", "ARRAY<INT>")]

    def test_no_schema_gives_no_columns(self):
        response = _statement_response(sql_kind="CREATE_TABLE", columns=None)
        connection = _connection_with_response(response)

        assert dry_run.get_schema(connection, "create table t (...)").columns == []


# ---------------------------------------------------------------------------
# dry_run.get_raw_columns
# ---------------------------------------------------------------------------


class TestGetRawColumns:
    def test_returns_untranslated_columns(self):
        """Unlike get_schema, an unsupported column type doesn't raise -
        get_raw_columns never calls try_get_castable_type at all, so a
        caller can inspect every column even when one of them isn't
        supported (see test_dry_run_type_translation.py, which needs this
        to test try_get_castable_type per-column against a table that
        deliberately includes unsupported types)."""
        response = _statement_response(
            columns=[
                ("id", {"type": "BIGINT", "nullable": True}),
                ("tags", {"type": "ARRAY", "nullable": True}),
            ]
        )
        connection = _connection_with_response(response)

        columns = dry_run.get_raw_columns(connection, "select id, tags from orders")

        assert [(c.name, c.type.type) for c in columns] == [("id", "BIGINT"), ("tags", "ARRAY")]

    def test_no_schema_gives_no_columns(self):
        response = _statement_response(sql_kind="CREATE_TABLE", columns=None)
        connection = _connection_with_response(response)

        assert dry_run.get_raw_columns(connection, "create table t (...)") == []


# ---------------------------------------------------------------------------
# dry_run.get_ddl_type
# ---------------------------------------------------------------------------


class TestGetDdlType:
    @pytest.mark.parametrize(
        "flink_type",
        ["BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DATE", "STRING"],
    )
    def test_bare_types_pass_through_unchanged(self, flink_type):
        type_def = ColumnTypeDefinition(type=flink_type, nullable=True)
        assert dry_run.get_ddl_type(type_def) == flink_type

    def test_decimal_includes_precision_and_scale(self):
        type_def = ColumnTypeDefinition(type="DECIMAL", nullable=True, precision=10, scale=2)
        assert dry_run.get_ddl_type(type_def) == "DECIMAL(10, 2)"

    def test_decimal_without_precision_falls_back_to_bare_name(self):
        type_def = ColumnTypeDefinition(type="DECIMAL", nullable=True)
        assert dry_run.get_ddl_type(type_def) == "DECIMAL"

    def test_decimal_with_only_precision_omits_scale(self):
        type_def = ColumnTypeDefinition(type="DECIMAL", nullable=True, precision=10)
        assert dry_run.get_ddl_type(type_def) == "DECIMAL(10)"

    def test_varchar_includes_length(self):
        type_def = ColumnTypeDefinition(type="VARCHAR", nullable=True, length=255)
        assert dry_run.get_ddl_type(type_def) == "VARCHAR(255)"

    def test_timestamp_includes_precision(self):
        type_def = ColumnTypeDefinition(type="TIMESTAMP", nullable=True, precision=3)
        assert dry_run.get_ddl_type(type_def) == "TIMESTAMP(3)"

    @pytest.mark.parametrize(
        ("dry_run_type", "expected_cast_type"),
        [
            ("TIME_WITHOUT_TIME_ZONE", "TIME"),
            ("TIMESTAMP_WITHOUT_TIME_ZONE", "TIMESTAMP"),
            ("TIMESTAMP_WITH_LOCAL_TIME_ZONE", "TIMESTAMP_LTZ"),
        ],
    )
    def test_internal_type_names_are_aliased_to_their_cast_keyword(
        self, dry_run_type, expected_cast_type
    ):
        """A dry run reports these by Flink's internal/full type name (confirmed
        against a live Confluent Cloud environment), but CAST(x AS ...) only
        accepts the short keyword form - e.g. `Unknown identifier
        'TIMESTAMP_WITHOUT_TIME_ZONE'` otherwise."""
        type_def = ColumnTypeDefinition(type=dry_run_type, nullable=True, precision=3)
        assert dry_run.get_ddl_type(type_def) == f"{expected_cast_type}(3)"

    def test_array_recurses_into_element_type(self):
        type_def = ColumnTypeDefinition(
            type="ARRAY",
            nullable=True,
            element_type=ColumnTypeDefinition(type="VARCHAR", nullable=True, length=10),
        )
        assert dry_run.get_ddl_type(type_def) == "ARRAY<VARCHAR(10)>"

    def test_multiset_recurses_into_element_type(self):
        type_def = ColumnTypeDefinition(
            type="MULTISET",
            nullable=True,
            element_type=ColumnTypeDefinition(type="INTEGER", nullable=True),
        )
        assert dry_run.get_ddl_type(type_def) == "MULTISET<INTEGER>"

    def test_map_recurses_into_key_and_value_types(self):
        type_def = ColumnTypeDefinition(
            type="MAP",
            nullable=True,
            key_type=ColumnTypeDefinition(type="VARCHAR", nullable=True, length=2147483647),
            value_type=ColumnTypeDefinition(type="INTEGER", nullable=True),
        )
        assert dry_run.get_ddl_type(type_def) == "MAP<VARCHAR(2147483647), INTEGER>"

    def test_row_recurses_into_field_types(self):
        type_def = ColumnTypeDefinition(
            type="ROW",
            nullable=True,
            fields=[
                RowColumn(
                    name="a", field_type=ColumnTypeDefinition(type="INTEGER", nullable=True)
                ),
                RowColumn(
                    name="b",
                    field_type=ColumnTypeDefinition(
                        type="VARCHAR", nullable=True, length=2147483647
                    ),
                ),
            ],
        )
        assert dry_run.get_ddl_type(type_def) == "ROW<a INTEGER, b VARCHAR(2147483647)>"

    def test_nested_constructed_types_recurse_fully(self):
        """ARRAY<ROW<...>> - confirmed live that arbitrary nesting round-trips."""
        type_def = ColumnTypeDefinition(
            type="ARRAY",
            nullable=True,
            element_type=ColumnTypeDefinition(
                type="ROW",
                nullable=True,
                fields=[
                    RowColumn(
                        name="a", field_type=ColumnTypeDefinition(type="INTEGER", nullable=True)
                    )
                ],
            ),
        )
        assert dry_run.get_ddl_type(type_def) == "ARRAY<ROW<a INTEGER>>"

    @pytest.mark.parametrize(
        ("flink_type", "expected"),
        [
            ("INTERVAL_YEAR_MONTH", "INTERVAL YEAR TO MONTH"),
            ("INTERVAL_DAY_TIME", "INTERVAL DAY TO SECOND"),
        ],
    )
    def test_interval_types_map_to_a_fixed_form(self, flink_type, expected):
        """Confirmed live: Confluent's Flink SQL Gateway normalizes every
        requested year-month (or day-time) interval form to the same
        reported precision/resolution, so there's only one real form of
        each to reconstruct - see get_ddl_type's docstring."""
        type_def = ColumnTypeDefinition(
            type=flink_type,
            nullable=True,
            precision=2,
            fractional_precision=3,
            resolution="SECOND",
        )
        assert dry_run.get_ddl_type(type_def) == expected

    @pytest.mark.parametrize("flink_type", ["RAW", "TIMESTAMP_WITH_TIME_ZONE"])
    def test_unrepresentable_types_raise_a_clear_error(self, flink_type):
        """These have no Flink DDL spelling at all, confirmed live (see
        UNREPRESENTABLE_TYPES's comment) - the only two types get_ddl_type
        can't reconstruct."""
        type_def = ColumnTypeDefinition(type=flink_type, nullable=True)
        with pytest.raises(DbtDatabaseError, match=flink_type):
            dry_run.get_ddl_type(type_def)

    def test_not_nullable_appends_not_null(self):
        type_def = ColumnTypeDefinition(type="BIGINT", nullable=False)
        assert dry_run.get_ddl_type(type_def) == "BIGINT NOT NULL"

    def test_not_nullable_with_parameters_appends_not_null_after_them(self):
        type_def = ColumnTypeDefinition(type="VARCHAR", nullable=False, length=255)
        assert dry_run.get_ddl_type(type_def) == "VARCHAR(255) NOT NULL"

    def test_not_nullable_element_type_recurses_into_constructed_types(self):
        """ARRAY<INT NOT NULL> is a different Flink type from ARRAY<INT> -
        each level's own nullability has to survive the recursion, not just
        the outermost one."""
        type_def = ColumnTypeDefinition(
            type="ARRAY",
            nullable=True,
            element_type=ColumnTypeDefinition(type="INTEGER", nullable=False),
        )
        assert dry_run.get_ddl_type(type_def) == "ARRAY<INTEGER NOT NULL>"


# ---------------------------------------------------------------------------
# dry_run.try_get_castable_type
# ---------------------------------------------------------------------------


class TestTryGetCastableType:
    @pytest.mark.parametrize(
        ("flink_type", "kwargs", "expected"),
        [
            ("BOOLEAN", {}, "BOOLEAN"),
            ("VARCHAR", {"length": 255}, "VARCHAR(255)"),
            ("DECIMAL", {"precision": 10, "scale": 2}, "DECIMAL(10, 2)"),
        ],
    )
    def test_delegates_to_get_ddl_type_for_supported_types(self, flink_type, kwargs, expected):
        """try_get_castable_type's own logic is only the extra NOT_CASTABLE_
        TYPES check below - everything else is get_ddl_type's job, already
        covered by TestGetDdlType, so this is just a delegation smoke test,
        not exhaustive over every parameter shape again."""
        type_def = ColumnTypeDefinition(type=flink_type, nullable=True, **kwargs)
        assert dry_run.try_get_castable_type(type_def) == expected

    @pytest.mark.parametrize(
        "flink_type",
        ["ARRAY", "MULTISET", "MAP", "ROW", "INTERVAL_YEAR_MONTH", "INTERVAL_DAY_TIME"],
    )
    def test_not_castable_types_raise_a_clear_error(self, flink_type):
        """No YAML-spellable unit test fixture value can ever be CAST into
        any of these, even though get_ddl_type can reconstruct their DDL
        form just fine - see NOT_CASTABLE_TYPES's comment for the
        live-confirmed reason per type. Functional coverage
        (test_dry_run_type_translation.py) proves this against a live
        Confluent Cloud dry run for every one of them; this just proves
        try_get_castable_type raises rather than returning a type string
        that would fail unhelpfully downstream."""
        type_def = ColumnTypeDefinition(type=flink_type, nullable=True)
        with pytest.raises(DbtDatabaseError, match=flink_type):
            dry_run.try_get_castable_type(type_def)

    @pytest.mark.parametrize("flink_type", ["RAW", "TIMESTAMP_WITH_TIME_ZONE"])
    def test_unrepresentable_types_also_raise(self, flink_type):
        """Delegated from get_ddl_type: these have no DDL spelling at all,
        so naturally no CAST target either."""
        type_def = ColumnTypeDefinition(type=flink_type, nullable=True)
        with pytest.raises(DbtDatabaseError, match=flink_type):
            dry_run.try_get_castable_type(type_def)

    def test_not_nullable_source_column_omits_not_null(self):
        """A CAST target can't carry a NOT NULL constraint - that's a column
        declaration concept, not a type-expression one. A NOT NULL/PRIMARY
        KEY source column is entirely ordinary in a real model, so this must
        not turn into `CAST(x AS BIGINT NOT NULL)`, which Flink would reject
        (unlike get_ddl_type, which does include it - see its own tests)."""
        type_def = ColumnTypeDefinition(type="BIGINT", nullable=False)
        assert dry_run.try_get_castable_type(type_def) == "BIGINT"
