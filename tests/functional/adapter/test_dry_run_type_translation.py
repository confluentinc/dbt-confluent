"""Functional coverage for dry_run.get_ddl_type and dry_run.try_get_castable_type
against a live Confluent Cloud Flink SQL Gateway.

tests/unit/test_dry_run.py covers both functions' logic against hand-built
ColumnTypeDefinitions in milliseconds; what it can't prove is that their
output is actually usable Flink SQL - only a live dry run can confirm that.
These are deliberately independent tests of two different guarantees, driven
by one shared manifest (COLUMN_TYPES/INTERVAL_CASES below) of every type
worth distinguishing:

1. get_ddl_type always succeeds (bar UNREPRESENTABLE_TYPES) in faithfully
   reproducing a dry run's reported schema as real DDL - proven here by
   actually creating a second table from the reconstructed DDL and dry
   running it, and asserting its reported schema is byte-for-byte identical
   to the first (not just "some valid DDL was produced").

2. try_get_castable_type - stricter, and expected to fail for some types -
   additionally guarantees that a YAML-spellable unit test fixture value can
   actually be CAST into the type it returns. The fixture value's literal
   rendering isn't reimplemented here: TestTryGetCastableTypeConfluent calls
   dbt-core's own format_row macro (get_fixture_sql.sql) via
   project.adapter.execute_macro, the same rendering a real unit test
   fixture goes through, so this can't quietly drift from dbt-core's actual
   behavior the way a Python stand-in could.

Every type below is exercised whether it's expected to succeed or not, so
COLUMN_TYPES/INTERVAL_CASES - read on their own, no test logic required -
are a live-verified record of exactly what a dbt-confluent unit test fixture
can and can't target, not just an assumption baked into dry_run.py. The
tests themselves just walk the manifest; whether a given type is expected to
support fixture casting lives in exactly one place: TypeCase.is_yaml_castable.

Method for both: create a real table with a column per scalar/DECIMAL-
variant/constructed Flink type worth distinguishing (see
https://nightlies.apache.org/flink/flink-docs-stable/docs/sql/reference/data-types/,
"storable" here excluding types that can never be a table column at all -
RAW, INTERVAL, TIMESTAMP WITH TIME ZONE), dry run `SELECT * FROM` it to get
each column's raw (untranslated) ColumnTypeDefinition, then exercise the
function under test per column.

INTERVAL_YEAR_MONTH/INTERVAL_DAY_TIME are covered separately in both test
classes, not in COLUMN_TYPES: Confluent's CREATE TABLE grammar rejects
INTERVAL as a column type outright (confirmed live - a real table can never
carry one), so the only way to ever produce one is a query projection.

Composite/constructed types (ARRAY, MULTISET, MAP, ROW) are kept to one
simple, non-nested case each - this file's cost is a real network round trip
per case, and nesting behavior is exercised in the unit tests instead.
"""

import dataclasses
from dataclasses import dataclass
from typing import Any

import pytest
from confluent_sql.statement import Column
from confluent_sql.types import ColumnTypeDefinition
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.confluent import dry_run
from dbt.adapters.contracts.connection import Connection
from dbt.tests.fixtures.project import TestProjInfo
from dbt.tests.util import run_dbt
from tests.functional.adapter._helpers import relation
from tests.functional.adapter.fixtures import ConfluentFixtures


@dataclass(frozen=True)
class TypeCase:
    """Everything this file knows/asserts about one Flink column type's
    support across dry-run DDL reconstruction and dbt unit test fixture
    casting - one manifest entry, one row in COLUMN_TYPES/INTERVAL_CASES."""

    ddl_type: str
    """The DDL spelling used to create this column (get_ddl_type must also
    reconstruct exactly a type Flink treats the same as this one)."""
    yaml_value: Any
    """A representative YAML-spellable fixture value for this type, rendered
    through dbt-core's own format_row macro (see
    _assert_castability_matches_the_manifest) - not a Python stand-in for it.
    Unused when is_yaml_castable is False - no literal exists that would
    make the cast succeed, so none is asserted."""
    is_yaml_castable: bool = True
    """Whether try_get_castable_type is expected to succeed *and* the
    resulting CAST(yaml_value AS translated) is expected to succeed. False
    for types with a valid DDL form (get_ddl_type succeeds regardless) that
    no YAML fixture value can ever be cast into - confirmed live, see
    dry_run.NOT_CASTABLE_TYPES."""


# Every distinct CAST-parameter shape either function has to handle (see
# their docstrings) gets its own entry: bare and parameterized forms of
# every length/precision/scale-bearing type, both DECIMAL forms, and one
# simple case of each constructed type. Scalars with no parameters (BOOLEAN,
# TINYINT, ..., DATE) appear once each, since there's only one shape to get
# right.
COLUMN_TYPES: dict[str, TypeCase] = {
    "c_char_bare": TypeCase(ddl_type="CHAR", yaml_value="x"),
    "c_char_n": TypeCase(ddl_type="CHAR(5)", yaml_value="x"),
    "c_varchar_bare": TypeCase(ddl_type="VARCHAR", yaml_value="hello"),
    "c_varchar_n": TypeCase(ddl_type="VARCHAR(255)", yaml_value="hello"),
    "c_string": TypeCase(ddl_type="STRING", yaml_value="hello"),
    "c_binary_bare": TypeCase(ddl_type="BINARY", yaml_value="x"),
    "c_binary_n": TypeCase(ddl_type="BINARY(5)", yaml_value="hello"),
    "c_varbinary_bare": TypeCase(ddl_type="VARBINARY", yaml_value="hello"),
    "c_varbinary_n": TypeCase(ddl_type="VARBINARY(255)", yaml_value="hello"),
    "c_bytes": TypeCase(ddl_type="BYTES", yaml_value="hello"),
    "c_boolean": TypeCase(ddl_type="BOOLEAN", yaml_value=True),
    "c_decimal_bare": TypeCase(ddl_type="DECIMAL", yaml_value=12.5),
    "c_decimal_p": TypeCase(ddl_type="DECIMAL(20)", yaml_value=12.5),
    "c_decimal_ps": TypeCase(ddl_type="DECIMAL(20, 4)", yaml_value=12.5),
    "c_tinyint": TypeCase(ddl_type="TINYINT", yaml_value=5),
    "c_smallint": TypeCase(ddl_type="SMALLINT", yaml_value=5),
    "c_integer": TypeCase(ddl_type="INT", yaml_value=5),
    "c_bigint": TypeCase(ddl_type="BIGINT", yaml_value=5),
    "c_float": TypeCase(ddl_type="FLOAT", yaml_value=12.5),
    "c_double": TypeCase(ddl_type="DOUBLE", yaml_value=12.5),
    "c_date": TypeCase(ddl_type="DATE", yaml_value="2024-01-01"),
    "c_time_bare": TypeCase(ddl_type="TIME", yaml_value="12:00:00"),
    "c_time_p": TypeCase(ddl_type="TIME(3)", yaml_value="12:00:00.123"),
    "c_timestamp_bare": TypeCase(ddl_type="TIMESTAMP", yaml_value="2024-01-01 12:00:00"),
    "c_timestamp_p": TypeCase(ddl_type="TIMESTAMP(3)", yaml_value="2024-01-01 12:00:00.123"),
    "c_timestamp_ltz_bare": TypeCase(ddl_type="TIMESTAMP_LTZ", yaml_value="2024-01-01 12:00:00"),
    "c_timestamp_ltz_p": TypeCase(
        ddl_type="TIMESTAMP_LTZ(3)",
        yaml_value="2024-01-01 12:00:00.123",
    ),
    # No YAML-spellable literal casts into any of these four - a YAML
    # list/dict fixture value is spliced in by dbt-core via bare `str()`
    # (Jinja has no other rendering for a non-string, non-None value),
    # giving e.g. `CAST([1, 2, 3] AS ARRAY<INT>)`, and Flink's CAST doesn't
    # accept Python-literal bracket/brace syntax at all ("Encountered '['
    # ..."/"Encountered '{' ...").
    "c_array": TypeCase(
        ddl_type="ARRAY<INT>",
        yaml_value=[1, 2, 3],
        is_yaml_castable=False,
    ),
    "c_multiset": TypeCase(
        ddl_type="MULTISET<INT>",
        yaml_value=[1, 2, 3],
        is_yaml_castable=False,
    ),
    "c_map": TypeCase(
        ddl_type="MAP<STRING, INT>",
        yaml_value={"a": 1},
        is_yaml_castable=False,
    ),
    "c_row": TypeCase(
        ddl_type="ROW<a INT, b STRING>",
        yaml_value={"a": 1, "b": "x"},
        is_yaml_castable=False,
    ),
}

# INTERVAL can't live in a table column at all (Confluent's CREATE TABLE
# grammar rejects it), so these are exercised via a query projection instead
# of COLUMN_TYPES' table - see _projection_sql. Not YAML-castable either:
# Flink requires its own interval literal syntax (`INTERVAL '1-2' YEAR TO
# MONTH`), so a plain string CAST fails ("Unsupported cast from 'CHAR(n)' to
# 'INTERVAL ...'"), and a plain string/number/bool is the only literal form
# a YAML fixture value ever renders as.
INTERVAL_CASES: dict[str, TypeCase] = {
    "c_interval_year_month": TypeCase(
        ddl_type="INTERVAL YEAR TO MONTH",
        yaml_value="1-2",
        is_yaml_castable=False,
    ),
    "c_interval_day_time": TypeCase(
        ddl_type="INTERVAL DAY TO SECOND",
        yaml_value="1 02:03:04",
        is_yaml_castable=False,
    ),
}


def _projection_sql(cases: dict[str, TypeCase]) -> str:
    """A `SELECT CAST(NULL AS ...) AS ..., ...` projection of every case's
    ddl_type under its column name, for exercising a type that can't live in
    a table column (an interval)."""
    columns = ", ".join(
        f"CAST(NULL AS {case.ddl_type}) AS `{name}`" for name, case in cases.items()
    )
    return f"select {columns}"


def _create_table(project: TestProjInfo, name: str, columns_ddl: str) -> BaseRelation:
    rel = relation(project, name)
    project.run_sql(f"drop table if exists {rel}")
    project.run_sql(f"create table {rel} ({columns_ddl})")
    return rel


# ---------------------------------------------------------------------------
# dry_run.get_ddl_type: always succeeds in reproducing an equivalent table
# ---------------------------------------------------------------------------


class TestGetDdlTypeConfluent(ConfluentFixtures):
    @pytest.mark.parametrize("not_null", [False, True], ids=["nullable", "not_null"])
    def test_reconstructed_ddl_creates_a_structurally_identical_table(
        self, project: TestProjInfo, not_null: bool
    ) -> None:
        """
        Uses this algorithm to verify that we can interpret dry-run schema results correctly:
        - Create a table with a column per supported column type - once with the manifest's
          default (nullable) types, and once with every column declared NOT NULL, so
          get_ddl_type's NOT NULL handling is exercised for every type, not just the default.
        - Dry-run's a select against that table.
        - Generates a new table from the dry-run result, and performs a dry-run against it.
        - Verifies the new table's dry run matches the original exactly.
        """
        not_null_suffix = " NOT NULL" if not_null else ""
        columns_ddl = ", ".join(
            f"`{name}` {case.ddl_type}{not_null_suffix}" for name, case in COLUMN_TYPES.items()
        )
        rel_a = relation(project, "dry_run_ddl_type_original_tmp")
        rel_b = relation(project, "dry_run_ddl_type_reconstructed_tmp")
        try:
            _create_table(project, "dry_run_ddl_type_original_tmp", columns_ddl)

            with project.adapter.connection_named("dry_run_ddl_type_original"):
                connection = project.adapter.connections.get_thread_connection()
                original = dry_run.get_raw_columns(connection, f"select * from {rel_a}")
            assert [c.name.lower() for c in original] == list(COLUMN_TYPES.keys())

            regenerated_types = {
                column.name: dry_run.get_ddl_type(column.type) for column in original
            }
            _print_reconstruction_debug(COLUMN_TYPES, original, regenerated_types)
            reconstructed_ddl = ", ".join(
                f"`{name}` {ddl_type}" for name, ddl_type in regenerated_types.items()
            )
            _create_table(project, "dry_run_ddl_type_reconstructed_tmp", reconstructed_ddl)

            with project.adapter.connection_named("dry_run_ddl_type_reconstructed"):
                connection = project.adapter.connections.get_thread_connection()
                reconstructed = dry_run.get_raw_columns(connection, f"select * from {rel_b}")

            # traits.schema.columns must match exactly, not just "be valid":
            # a mismatch here means get_ddl_type built DDL that Flink accepted
            # but that doesn't actually mean what we assumed it meant. Column
            # (and the ColumnTypeDefinition under .type) are plain dataclasses,
            # so this compares every field - length/precision/scale/nullable/
            # nested element-key-value-field types included - not just name
            # and a hand-picked subset.
            assert reconstructed == original
        finally:
            project.run_sql(f"drop table if exists {rel_a}")
            project.run_sql(f"drop table if exists {rel_b}")

    def test_reconstructed_interval_ddl_reproduces_the_same_schema(
        self, project: TestProjInfo
    ) -> None:
        """INTERVAL can't live in a table column at all, so this proves the
        same "reproduces an identical schema" guarantee via a bare query
        projection instead of table recreation."""
        with project.adapter.connection_named("dry_run_ddl_type_interval"):
            connection = project.adapter.connections.get_thread_connection()
            original = dry_run.get_raw_columns(connection, _projection_sql(INTERVAL_CASES))

            regenerated_types = {
                column.name: dry_run.get_ddl_type(column.type) for column in original
            }
            _print_reconstruction_debug(INTERVAL_CASES, original, regenerated_types)
            reconstructed_select = ", ".join(
                f"CAST(NULL AS {ddl_type}) AS `{name}`"
                for name, ddl_type in regenerated_types.items()
            )
            reconstructed = dry_run.get_raw_columns(connection, f"select {reconstructed_select}")

        assert reconstructed == original


def _print_reconstruction_debug(
    cases: dict[str, TypeCase], original: list[Column], regenerated_types: dict[str, str]
) -> None:
    """Column name / manifest ddl_type / get_ddl_type's regenerated spelling,
    one line per column - visible with `pytest -s` (or `-rP` for a passing
    test), silent otherwise."""
    print(f"\n{'column':<25} {'manifest ddl_type':<25} regenerated ddl_type")
    for column in original:
        case = cases[column.name.lower()]
        print(f"{column.name:<25} {case.ddl_type:<25} {regenerated_types[column.name]}")


# ---------------------------------------------------------------------------
# dry_run.try_get_castable_type: a YAML fixture value casts into every
# supported type; the rest raise a clear, useful error
# ---------------------------------------------------------------------------


class TestTryGetCastableTypeConfluent(ConfluentFixtures):
    def test_yaml_fixture_values_cast_into_every_supported_type(
        self, project: TestProjInfo
    ) -> None:
        """
        Uses this algorithm to verify that we can generate type-correct 'dbt test' input data via casts.
        - Create a table with a column per supported column type.
        - Dry-run's a select against that table to retrieve column type info (as spelled by the server).
        - Generate a single select statement that casts an exemplar YAML value into each type.
        - Verifies the select query dry-run results are identical to the original table.
        """
        # Needed before project.adapter.execute_macro can look anything up
        # below - it populates the adapter's macro resolver from the parsed
        # manifest. No models are required for this; the parse is cheap.
        run_dbt(["parse"])
        rel = relation(project, "try_get_castable_type_tmp")
        columns_ddl = ", ".join(f"`{name}` {case.ddl_type}" for name, case in COLUMN_TYPES.items())
        project.run_sql(f"drop table if exists {rel}")
        try:
            project.run_sql(f"create table {rel} ({columns_ddl})")

            with project.adapter.connection_named("try_get_castable_type"):
                connection = project.adapter.connections.get_thread_connection()
                raw_columns = dry_run.get_raw_columns(connection, f"select * from {rel}")
                assert [c.name.lower() for c in raw_columns] == list(COLUMN_TYPES.keys())

                _assert_castability_matches_the_manifest(
                    project, connection, COLUMN_TYPES, raw_columns
                )
        finally:
            project.run_sql(f"drop table if exists {rel}")

    def test_interval_types_are_not_castable(self, project: TestProjInfo) -> None:
        run_dbt(["parse"])
        with project.adapter.connection_named("try_get_castable_type_interval"):
            connection = project.adapter.connections.get_thread_connection()
            raw_columns = dry_run.get_raw_columns(connection, _projection_sql(INTERVAL_CASES))
            assert [c.name for c in raw_columns] == list(INTERVAL_CASES.keys())

            _assert_castability_matches_the_manifest(
                project, connection, INTERVAL_CASES, raw_columns
            )


def _assert_castability_matches_the_manifest(
    project: TestProjInfo,
    connection: Connection,
    cases: dict[str, TypeCase],
    raw_columns: list[Column],
) -> None:
    """For each raw column: assert try_get_castable_type raises when the
    manifest says it should. Otherwise, collect its manifest fixture value
    and translated type, then render all of them through dbt-core's own
    format_row macro (project.adapter.execute_macro("format_row", ...)) -
    the actual dbt.string_literal/escape_single_quotes/safe_cast dispatch
    chain a real unit test fixture goes through, not a Python stand-in for
    it - and dry run the resulting CAST expressions in one shot.

    Also asserts the recast columns match the *original* table columns
    exactly - the full Column (name, type, description), not a hand-picked
    subset of fields: succeeding at *some* cast isn't enough - a value that
    casts cleanly into the wrong type (silent truncation/coercion) would
    pass a bare "did it error" check but still mean try_get_castable_type
    built the wrong CAST target.
    """
    row: dict[str, Any] = {}
    column_name_to_data_types: dict[str, str] = {}
    original_columns: dict[str, Column] = {}
    for column in raw_columns:
        case = cases[column.name.lower()]
        if not case.is_yaml_castable:
            with pytest.raises(DbtDatabaseError):
                dry_run.try_get_castable_type(column.type)
            continue
        row[column.name] = case.yaml_value
        column_name_to_data_types[column.name.lower()] = dry_run.try_get_castable_type(column.type)
        original_columns[column.name] = column

    if not row:
        return

    formatted_row = project.adapter.execute_macro(
        "format_row",
        kwargs={"row": row, "column_name_to_data_types": column_name_to_data_types},
    )
    supported_casts = [f"{expr} AS `{name}`" for name, expr in formatted_row.items()]
    # One dry run for every supported column at once: proves each translated
    # type is valid DDL *and* that dbt-core's own fixture-value rendering
    # actually casts into it, in a single round trip.
    recast_columns = dry_run.get_raw_columns(connection, f"select {', '.join(supported_casts)}")
    assert [
        dataclasses.replace(column, type=_ignoring_nullability(column.type))
        for column in recast_columns
    ] == [
        dataclasses.replace(
            original_columns[column.name],
            type=_ignoring_nullability(original_columns[column.name].type),
        )
        for column in recast_columns
    ]


def _ignoring_nullability(type_def: ColumnTypeDefinition) -> ColumnTypeDefinition:
    """A copy of `type_def` with `.nullable` (and every nested element/key/
    value/field type's `.nullable`) forced to a fixed value.

    A stored table column is nullable by default, but CAST-ing a concrete,
    non-null literal is correctly reported by Flink as NOT NULL - that's a
    fact about the specific expression, not about whether try_get_castable_
    type reconstructed the right type. Comparing raw ColumnTypeDefinitions
    for the two would always disagree on this one field regardless of
    whether the type itself is right, so it's normalized away before the
    comparison in _assert_castability_matches_the_manifest.
    """
    return dataclasses.replace(
        type_def,
        nullable=True,
        element_type=(
            _ignoring_nullability(type_def.element_type) if type_def.element_type else None
        ),
        key_type=(_ignoring_nullability(type_def.key_type) if type_def.key_type else None),
        value_type=(_ignoring_nullability(type_def.value_type) if type_def.value_type else None),
        fields=(
            [
                dataclasses.replace(field, field_type=_ignoring_nullability(field.field_type))
                for field in type_def.fields
            ]
            if type_def.fields
            else None
        ),
    )
