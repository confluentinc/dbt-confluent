"""Unit tests for rendering dry-run column types as INFORMATION_SCHEMA FULL_DATA_TYPE.

The dry-run JSON shapes come from the GH-118 probes (issue #118). Each rendered case says what
kind of evidence backs it:

- CTAS (run 995e2382): a table created with `CREATE TABLE ... AS SELECT` from the same query
  reported exactly this FULL_DATA_TYPE. This is the path the drift check replaces, so it is
  the strongest evidence.
- declared (run 09dcd5b0): a `CAST(NULL AS <type>)` dry-run reported this shape, and a table
  *declared* with that type reported this FULL_DATA_TYPE. A CTAS was never observed for it.
- composition: not observed as a whole. The string follows from composing observed rules, and
  the test pins that composition.

Everything the renderer does not render must raise UnverifiedTypeError, so the resolver falls
back to the temp table instead of risking a false "Schema drift detected".
"""

import pytest
from confluent_sql.types import ColumnTypeDefinition

from dbt.adapters.confluent.flink_types import UnverifiedTypeError, render_full_data_type


def _type(data: dict) -> ColumnTypeDefinition:
    """Parse dry-run JSON exactly as the driver does for statement traits."""
    return ColumnTypeDefinition.from_response(data)


MAX = 2147483647
INT = {"type": "INTEGER", "nullable": True}
INT_NN = {"type": "INTEGER", "nullable": False}
STR = {"type": "VARCHAR", "length": MAX, "nullable": True}
STR_NN = {"type": "VARCHAR", "length": MAX, "nullable": False}


@pytest.mark.parametrize(
    "data,expected",
    [
        pytest.param(INT, "INT", id="int-ctas"),
        pytest.param({"type": "BIGINT", "nullable": True}, "BIGINT", id="bigint-ctas"),
        pytest.param({"type": "BOOLEAN", "nullable": False}, "BOOLEAN", id="boolean-ctas"),
        pytest.param({"type": "DOUBLE", "nullable": False}, "DOUBLE", id="double-ctas"),
        pytest.param({"type": "DATE", "nullable": False}, "DATE", id="date-ctas"),
        pytest.param(STR, f"VARCHAR({MAX})", id="string-ctas"),
        pytest.param(
            {"type": "VARCHAR", "length": 20, "nullable": False}, "VARCHAR(20)", id="varchar-ctas"
        ),
        pytest.param(
            {"type": "CHAR", "length": 3, "nullable": False}, "CHAR(3)", id="char-literal-ctas"
        ),
        pytest.param(
            {"type": "DECIMAL", "precision": 10, "scale": 2, "nullable": True},
            "DECIMAL(10, 2)",
            id="decimal-ctas",
        ),
        pytest.param(
            {"type": "DECIMAL", "precision": 21, "scale": 2, "nullable": True},
            "DECIMAL(21, 2)",
            id="decimal-arithmetic-ctas",
        ),
        pytest.param(
            {"type": "TIMESTAMP_WITHOUT_TIME_ZONE", "precision": 3, "nullable": True},
            "TIMESTAMP(3)",
            id="timestamp3-ctas",
        ),
        pytest.param(
            {"type": "TIMESTAMP_WITHOUT_TIME_ZONE", "precision": 6, "nullable": False},
            "TIMESTAMP(6)",
            id="timestamp6-ctas",
        ),
        pytest.param(
            {"type": "TIMESTAMP_WITH_LOCAL_TIME_ZONE", "precision": 3, "nullable": True},
            "TIMESTAMP(3) WITH LOCAL TIME ZONE",
            id="ltz3-ctas",
        ),
        pytest.param({"type": "FLOAT", "nullable": True}, "FLOAT", id="float-declared"),
        pytest.param(
            {"type": "CHAR", "length": 5, "nullable": True}, "CHAR(5)", id="char-declared"
        ),
        pytest.param(
            {"type": "TIME_WITHOUT_TIME_ZONE", "precision": 0, "nullable": True},
            "TIME(0)",
            id="time0-declared",
        ),
        pytest.param(
            {"type": "VARBINARY", "length": MAX, "nullable": True},
            f"VARBINARY({MAX})",
            id="bytes-declared",
        ),
    ],
)
def test_scalar_types(data, expected):
    assert render_full_data_type(_type(data)) == expected


def test_top_level_not_null_dropped():
    """CTAS: FULL_DATA_TYPE never carries top-level NOT NULL (that is IS_NULLABLE's job), so a
    NOT NULL column in the query must not render differently from the table."""
    assert render_full_data_type(_type({"type": "BIGINT", "nullable": False})) == "BIGINT"
    assert render_full_data_type(_type(STR_NN)) == f"VARCHAR({MAX})"


def test_array_keeps_nested_not_null():
    """CTAS: ARRAY[1, 2] is ARRAY<INT NOT NULL> in both the dry-run and the table."""
    data = {"type": "ARRAY", "nullable": False, "element_type": INT_NN}
    assert render_full_data_type(_type(data)) == "ARRAY<INT NOT NULL>"


def test_array_with_nullable_element():
    """Declared: ARRAY<INT> and ARRAY<STRING NOT NULL>."""
    assert (
        render_full_data_type(_type({"type": "ARRAY", "nullable": True, "element_type": INT}))
        == "ARRAY<INT>"
    )
    assert (
        render_full_data_type(_type({"type": "ARRAY", "nullable": True, "element_type": STR_NN}))
        == f"ARRAY<VARCHAR({MAX}) NOT NULL>"
    )


def test_map_key_always_not_null():
    """Declared: CAST(NULL AS MAP<STRING, INT>) reports a nullable key, and the declared table
    stores it NOT NULL."""
    data = {"type": "MAP", "nullable": True, "key_type": STR, "value_type": INT}
    assert render_full_data_type(_type(data)) == f"MAP<VARCHAR({MAX}) NOT NULL, INT>"


def test_map_value_keeps_its_own_nullability():
    """Composition: a NOT NULL value keeps its NOT NULL, like any nested type (the CTAS table
    from MAP['a', 1] stored `INT NOT NULL` as the value)."""
    data = {"type": "MAP", "nullable": False, "key_type": STR_NN, "value_type": INT_NN}
    assert render_full_data_type(_type(data)) == f"MAP<VARCHAR({MAX}) NOT NULL, INT NOT NULL>"


def test_row_renders_backticked_field_names():
    """CTAS: CAST(ROW(1, 'b') AS ROW<`a` INT, `b` STRING>)."""
    data = {
        "type": "ROW",
        "nullable": False,
        "fields": [
            {"name": "a", "field_type": INT},
            {"name": "b", "field_type": STR},
        ],
    }
    assert render_full_data_type(_type(data)) == f"ROW<`a` INT, `b` VARCHAR({MAX})>"


def test_nested_composites_follow_composition_rules():
    """Composition, NOT observed live: a ROW inside an ARRAY was never compared against a
    table. This pins how the per-type rules compose recursively. If a live run ever shows a
    different FULL_DATA_TYPE for this shape, change the renderer (or reject the shape) and this
    test together."""
    data = {
        "type": "ARRAY",
        "nullable": True,
        "element_type": {
            "type": "ROW",
            "nullable": False,
            "fields": [
                {
                    "name": "tags",
                    "field_type": {"type": "ARRAY", "nullable": True, "element_type": STR},
                }
            ],
        },
    }
    assert (
        render_full_data_type(_type(data)) == f"ARRAY<ROW<`tags` ARRAY<VARCHAR({MAX})>> NOT NULL>"
    )


@pytest.mark.parametrize(
    "data",
    [
        pytest.param({"type": "TINYINT", "nullable": True}, id="tinyint"),
        pytest.param({"type": "SMALLINT", "nullable": True}, id="smallint"),
        pytest.param({"type": "BINARY", "length": 16, "nullable": True}, id="binary"),
        pytest.param(
            {"type": "MULTISET", "nullable": True, "element_type": INT_NN}, id="multiset"
        ),
        pytest.param(
            {"type": "INTERVAL_DAY_TIME", "nullable": True, "resolution": "DAY", "precision": 2},
            id="interval",
        ),
        pytest.param(
            {"type": "TIMESTAMP_WITH_TIME_ZONE", "precision": 3, "nullable": True},
            id="timestamp-tz",
        ),
        pytest.param({"type": "VARIANT", "nullable": True}, id="variant"),
        pytest.param(
            {
                "type": "ARRAY",
                "nullable": True,
                "element_type": {"type": "SMALLINT", "nullable": True},
            },
            id="array-of-smallint",
        ),
        pytest.param(
            {
                "type": "ROW",
                "nullable": True,
                "fields": [
                    {
                        "name": "m",
                        "field_type": {
                            "type": "MULTISET",
                            "nullable": True,
                            "element_type": INT_NN,
                        },
                    }
                ],
            },
            id="row-with-multiset",
        ),
    ],
)
def test_unverified_type_names_raise(data):
    """A type name never seen in a table's FULL_DATA_TYPE raises, at any depth."""
    with pytest.raises(UnverifiedTypeError):
        render_full_data_type(_type(data))


def _map(key: dict, value: dict | None = None) -> dict:
    return {"type": "MAP", "nullable": True, "key_type": key, "value_type": value or INT}


@pytest.mark.parametrize(
    "data,match",
    [
        # MAP keys: only STRING (VARCHAR(2147483647)) keys were observed in a table, and a CTAS
        # stored a CHAR(1) key as VARCHAR(2147483647) (run 995e2382), so any other key falls
        # back.
        pytest.param(
            _map({"type": "CHAR", "length": 1, "nullable": False}), "MAP key", id="map-char-key"
        ),
        pytest.param(
            _map({"type": "VARCHAR", "length": 20, "nullable": True}),
            "MAP key",
            id="map-varchar20-key",
        ),
        pytest.param(_map(INT_NN), "MAP key", id="map-int-key"),
        pytest.param(_map({"type": "BIGINT", "nullable": False}), "MAP key", id="map-bigint-key"),
        # CHAR, BINARY and non-max VARBINARY never render inside a composite type.
        pytest.param(
            {
                "type": "ARRAY",
                "nullable": False,
                "element_type": {"type": "CHAR", "length": 1, "nullable": False},
            },
            "CHAR",
            id="array-of-char",
        ),
        pytest.param(
            _map(STR, {"type": "CHAR", "length": 1, "nullable": False}),
            "CHAR",
            id="map-char-value",
        ),
        pytest.param(
            {
                "type": "ROW",
                "nullable": True,
                "fields": [
                    {"name": "c", "field_type": {"type": "CHAR", "length": 1, "nullable": True}}
                ],
            },
            "CHAR",
            id="row-char-field",
        ),
        pytest.param(
            {
                "type": "ARRAY",
                "nullable": True,
                "element_type": {"type": "BINARY", "length": 16, "nullable": True},
            },
            "BINARY",
            id="array-of-binary",
        ),
        pytest.param(
            {
                "type": "ARRAY",
                "nullable": True,
                "element_type": {"type": "VARBINARY", "length": 16, "nullable": True},
            },
            "VARBINARY",
            id="array-of-varbinary16",
        ),
        # Top-level parameters outside what was observed.
        pytest.param(
            {"type": "VARBINARY", "length": 16, "nullable": True}, "VARBINARY", id="varbinary16"
        ),
        pytest.param({"type": "CHAR", "length": 0, "nullable": False}, "CHAR", id="char0"),
        pytest.param(
            {"type": "TIME_WITHOUT_TIME_ZONE", "precision": 3, "nullable": True},
            "precision",
            id="time3",
        ),
        pytest.param(
            {"type": "TIMESTAMP_WITHOUT_TIME_ZONE", "precision": 0, "nullable": True},
            "precision",
            id="timestamp0",
        ),
        pytest.param(
            {"type": "TIMESTAMP_WITHOUT_TIME_ZONE", "precision": 9, "nullable": True},
            "precision",
            id="timestamp9",
        ),
        pytest.param(
            {"type": "TIMESTAMP_WITH_LOCAL_TIME_ZONE", "precision": 6, "nullable": True},
            "precision",
            id="ltz6",
        ),
        # A required parameter the server left out would otherwise render as "None".
        pytest.param({"type": "VARCHAR", "nullable": True}, "length", id="varchar-no-length"),
        pytest.param({"type": "CHAR", "nullable": True}, "length", id="char-no-length"),
        pytest.param(
            {"type": "VARBINARY", "nullable": True}, "VARBINARY", id="varbinary-no-length"
        ),
        pytest.param(
            {"type": "DECIMAL", "scale": 2, "nullable": True},
            "precision",
            id="decimal-no-precision",
        ),
        pytest.param(
            {"type": "DECIMAL", "precision": 10, "nullable": True}, "scale", id="decimal-no-scale"
        ),
        pytest.param(
            {"type": "TIME_WITHOUT_TIME_ZONE", "nullable": True},
            "precision",
            id="time-no-precision",
        ),
        # ROW shapes FULL_DATA_TYPE was never observed for.
        pytest.param(
            {"type": "ROW", "nullable": True, "fields": []}, "no fields", id="row-empty-fields"
        ),
        pytest.param({"type": "ROW", "nullable": True}, "no fields", id="row-missing-fields"),
        pytest.param(
            {"type": "ROW", "nullable": True, "fields": [{"name": "a`b", "field_type": INT}]},
            "backtick",
            id="row-backtick-name",
        ),
        pytest.param(
            {
                "type": "ROW",
                "nullable": True,
                "fields": [{"name": "a", "field_type": INT, "description": "the a field"}],
            },
            "description",
            id="row-field-description",
        ),
        pytest.param(
            {"type": "ARRAY", "nullable": True}, "ARRAY element", id="array-no-element-type"
        ),
    ],
)
def test_unverified_shapes_raise(data, match):
    """Allow-listed type names with parameters, nesting or ROW shapes that no table was
    observed with raise, so the resolver falls back to the temp table."""
    with pytest.raises(UnverifiedTypeError, match=match):
        render_full_data_type(_type(data))


def test_map_without_key_type_raises():
    """The driver's parser always sets MAP key/value types; guard the renderer anyway."""
    with pytest.raises(UnverifiedTypeError, match="MAP key"):
        render_full_data_type(ColumnTypeDefinition(type="MAP", nullable=True))
