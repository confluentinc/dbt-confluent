"""Unit tests for rendering dry-run column types as INFORMATION_SCHEMA FULL_DATA_TYPE.

The dry-run JSON shapes come from the GH-118 probes (issue #118). Each rendered case says what
kind of evidence backs it:

- CTAS (runs 995e2382, 0484bde2, 6b109689, 11017c00): a table created with
  `CREATE TABLE ... AS SELECT` from the same query reported exactly this FULL_DATA_TYPE. This is
  the path the drift check replaces, so it is the strongest evidence.
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


def _ts(precision: int) -> dict:
    return {"type": "TIMESTAMP_WITHOUT_TIME_ZONE", "precision": precision, "nullable": False}


def _ltz(precision: int) -> dict:
    return {"type": "TIMESTAMP_WITH_LOCAL_TIME_ZONE", "precision": precision, "nullable": True}


def _time(precision: int) -> dict:
    return {"type": "TIME_WITHOUT_TIME_ZONE", "precision": precision, "nullable": False}


@pytest.mark.parametrize(
    "data,expected",
    [
        pytest.param(INT, "INT", id="int-ctas"),
        pytest.param({"type": "BIGINT", "nullable": True}, "BIGINT", id="bigint-ctas"),
        pytest.param({"type": "TINYINT", "nullable": False}, "TINYINT", id="tinyint-ctas"),
        pytest.param({"type": "SMALLINT", "nullable": False}, "SMALLINT", id="smallint-ctas"),
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
            {"type": "BINARY", "length": 2, "nullable": False}, "BINARY(2)", id="binary-ctas"
        ),
        pytest.param(
            {"type": "VARBINARY", "length": 16, "nullable": False},
            "VARBINARY(16)",
            id="varbinary16-ctas",
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
        pytest.param(_time(1), "TIME(1)", id="time1-ctas"),
        pytest.param(_time(2), "TIME(2)", id="time2-ctas"),
        pytest.param(_time(3), "TIME(3)", id="time3-ctas"),
        *[
            pytest.param(_ts(p), f"TIMESTAMP({p})", id=f"timestamp{p}-ctas")
            for p in (0, 1, 2, 3, 4, 5, 6)
        ],
        *[
            pytest.param(_ltz(p), f"TIMESTAMP({p}) WITH LOCAL TIME ZONE", id=f"ltz{p}-ctas")
            for p in (0, 1, 2, 3, 4, 5, 6)
        ],
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


@pytest.mark.parametrize(
    "element,expected",
    [
        pytest.param(
            {"type": "CHAR", "length": 1, "nullable": False}, "CHAR(1) NOT NULL", id="char"
        ),
        pytest.param(
            {"type": "VARCHAR", "length": 5, "nullable": False},
            "VARCHAR(5) NOT NULL",
            id="varchar5",
        ),
        pytest.param(
            {"type": "BINARY", "length": 1, "nullable": False}, "BINARY(1) NOT NULL", id="binary"
        ),
        pytest.param(
            {"type": "VARBINARY", "length": 16, "nullable": False},
            "VARBINARY(16) NOT NULL",
            id="varbinary16",
        ),
        pytest.param({"type": "TINYINT", "nullable": False}, "TINYINT NOT NULL", id="tinyint"),
        pytest.param(
            {"type": "DECIMAL", "precision": 10, "scale": 2, "nullable": False},
            "DECIMAL(10, 2) NOT NULL",
            id="decimal",
        ),
        pytest.param(_ts(3), "TIMESTAMP(3) NOT NULL", id="timestamp3"),
    ],
)
def test_array_elements_keep_their_parameters(element, expected):
    """CTAS: an ARRAY element keeps its length, precision and scale in the table."""
    data = {"type": "ARRAY", "nullable": False, "element_type": element}
    assert render_full_data_type(_type(data)) == f"ARRAY<{expected}>"


@pytest.mark.parametrize(
    "data,expected",
    [
        pytest.param(
            {"type": "MULTISET", "nullable": False, "element_type": INT_NN},
            "MULTISET<INT NOT NULL>",
            id="collect-ctas",
        ),
        pytest.param(
            {"type": "MULTISET", "nullable": True, "element_type": INT},
            "MULTISET<INT>",
            id="declared-int-ctas",
        ),
        pytest.param(
            {"type": "MULTISET", "nullable": True, "element_type": STR_NN},
            f"MULTISET<VARCHAR({MAX}) NOT NULL>",
            id="declared-string-ctas",
        ),
        pytest.param(
            {
                "type": "ROW",
                "nullable": True,
                "fields": [
                    {
                        "name": "m",
                        "field_type": {"type": "MULTISET", "nullable": True, "element_type": INT},
                    }
                ],
            },
            "ROW<`m` MULTISET<INT>>",
            id="row-field-ctas",
        ),
    ],
)
def test_multiset_renders_like_array(data, expected):
    """CTAS: COLLECT() and tables declared with MULTISET columns."""
    assert render_full_data_type(_type(data)) == expected


def _map(key: dict, value: dict | None = None) -> dict:
    return {"type": "MAP", "nullable": False, "key_type": key, "value_type": value or INT_NN}


@pytest.mark.parametrize(
    "key",
    [
        pytest.param(STR, id="nullable-string-ctas"),
        pytest.param(STR_NN, id="string-ctas"),
        pytest.param({"type": "VARCHAR", "length": 5, "nullable": False}, id="varchar5-ctas"),
        pytest.param({"type": "CHAR", "length": 1, "nullable": False}, id="char1-ctas"),
        pytest.param({"type": "CHAR", "length": 3, "nullable": False}, id="char3-ctas"),
    ],
)
def test_map_string_keys_widen_to_not_null_string(key):
    """CTAS: a table stores any CHAR or VARCHAR key as VARCHAR(2147483647) NOT NULL, whatever
    its length or nullability in the query."""
    assert render_full_data_type(_type(_map(key))) == f"MAP<VARCHAR({MAX}) NOT NULL, INT NOT NULL>"


@pytest.mark.parametrize(
    "key,expected",
    [
        pytest.param(INT_NN, "INT NOT NULL", id="int-ctas"),
        pytest.param(INT, "INT", id="nullable-int-ctas"),
        pytest.param({"type": "BIGINT", "nullable": False}, "BIGINT NOT NULL", id="bigint-ctas"),
        pytest.param({"type": "DATE", "nullable": False}, "DATE NOT NULL", id="date-ctas"),
        pytest.param(
            {"type": "DECIMAL", "precision": 10, "scale": 2, "nullable": False},
            "DECIMAL(10, 2) NOT NULL",
            id="decimal-ctas",
        ),
        pytest.param(
            {"type": "VARBINARY", "length": 16, "nullable": False},
            "VARBINARY(16) NOT NULL",
            id="varbinary16-ctas",
        ),
    ],
)
def test_map_other_keys_keep_type_and_nullability(key, expected):
    """CTAS: a non-string key keeps its type and its own nullability (a nullable INT key is
    stored as `INT`, not `INT NOT NULL`)."""
    assert render_full_data_type(_type(_map(key))) == f"MAP<{expected}, INT NOT NULL>"


@pytest.mark.parametrize(
    "value,expected",
    [
        pytest.param(INT, "INT", id="nullable-int-declared"),
        pytest.param(INT_NN, "INT NOT NULL", id="int-ctas"),
        pytest.param(
            {"type": "VARCHAR", "length": 5, "nullable": False},
            "VARCHAR(5) NOT NULL",
            id="varchar5-ctas",
        ),
        pytest.param(
            {"type": "CHAR", "length": 3, "nullable": False}, "CHAR(3) NOT NULL", id="char3-ctas"
        ),
        pytest.param(
            {"type": "BINARY", "length": 2, "nullable": False},
            "BINARY(2) NOT NULL",
            id="binary2-ctas",
        ),
        pytest.param(
            {"type": "VARBINARY", "length": 16, "nullable": False},
            "VARBINARY(16) NOT NULL",
            id="varbinary16-ctas",
        ),
        pytest.param(
            {"type": "ARRAY", "nullable": False, "element_type": INT_NN},
            "ARRAY<INT NOT NULL> NOT NULL",
            id="array-ctas",
        ),
    ],
)
def test_map_value_keeps_its_type_and_nullability(value, expected):
    """A MAP value renders like any nested type."""
    assert render_full_data_type(_type(_map(STR_NN, value))) == (
        f"MAP<VARCHAR({MAX}) NOT NULL, {expected}>"
    )


def _row(*fields: tuple[str, dict], nullable: bool = False) -> dict:
    return {
        "type": "ROW",
        "nullable": nullable,
        "fields": [{"name": name, "field_type": field_type} for name, field_type in fields],
    }


@pytest.mark.parametrize(
    "data,expected",
    [
        pytest.param(
            _row(("a", INT), ("b", STR)), f"ROW<`a` INT, `b` VARCHAR({MAX})>", id="nullable-ctas"
        ),
        pytest.param(
            _row(("a", INT_NN), ("b", STR_NN)),
            f"ROW<`a` INT NOT NULL, `b` VARCHAR({MAX}) NOT NULL>",
            id="not-null-fields-ctas",
        ),
        pytest.param(
            _row(("x", {"type": "VARCHAR", "length": 5, "nullable": True})),
            "ROW<`x` VARCHAR(5)>",
            id="varchar5-field-ctas",
        ),
        pytest.param(
            _row(("x", {"type": "CHAR", "length": 3, "nullable": True})),
            "ROW<`x` CHAR(3)>",
            id="char3-field-ctas",
        ),
        pytest.param(
            _row(("x", {"type": "BINARY", "length": 2, "nullable": True})),
            "ROW<`x` BINARY(2)>",
            id="binary2-field-ctas",
        ),
        pytest.param(
            _row(("x", {"type": "VARBINARY", "length": 16, "nullable": True})),
            "ROW<`x` VARBINARY(16)>",
            id="varbinary16-field-ctas",
        ),
        pytest.param(
            _row(("a", INT), ("r", _row(("x", STR), nullable=True))),
            f"ROW<`a` INT, `r` ROW<`x` VARCHAR({MAX})>>",
            id="row-in-row-ctas",
        ),
        pytest.param(
            {"type": "ARRAY", "nullable": False, "element_type": _row(("a", INT), ("b", STR))},
            f"ARRAY<ROW<`a` INT, `b` VARCHAR({MAX})> NOT NULL>",
            id="array-of-row-ctas",
        ),
    ],
)
def test_row_renders_backticked_field_names(data, expected):
    assert render_full_data_type(_type(data)) == expected


def test_nested_composites_follow_composition_rules():
    """Composition, NOT observed live as a whole: an ARRAY field inside a ROW inside an ARRAY.
    This pins how the per-type rules compose recursively. If a live run ever shows a different
    FULL_DATA_TYPE for this shape, change the renderer (or reject the shape) and this test
    together."""
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
        pytest.param(
            {
                "type": "INTERVAL_DAY_TIME",
                "nullable": False,
                "precision": 2,
                "fractional_precision": 3,
                "resolution": "SECOND",
            },
            id="interval-day-time",
        ),
        pytest.param(
            {
                "type": "INTERVAL_YEAR_MONTH",
                "nullable": False,
                "precision": 2,
                "resolution": "MONTH",
            },
            id="interval-year-month",
        ),
        pytest.param({"type": "VARIANT", "nullable": False}, id="variant"),
        pytest.param(
            {"type": "TIMESTAMP_WITH_TIME_ZONE", "precision": 3, "nullable": True},
            id="timestamp-tz",
        ),
        pytest.param(
            {
                "type": "ARRAY",
                "nullable": True,
                "element_type": {"type": "VARIANT", "nullable": True},
            },
            id="array-of-variant",
        ),
        pytest.param(
            _map(STR_NN, {"type": "INTERVAL_YEAR_MONTH", "nullable": True, "precision": 2}),
            id="map-of-interval",
        ),
    ],
)
def test_unverified_type_names_raise(data):
    """A type name never seen in a table's FULL_DATA_TYPE raises, at any depth. A table can't
    store INTERVAL or VARIANT, and TIMESTAMP WITH TIME ZONE isn't a Flink type."""
    with pytest.raises(UnverifiedTypeError):
        render_full_data_type(_type(data))


@pytest.mark.parametrize(
    "data,match",
    [
        # Parameters outside what was observed or what a table can store.
        pytest.param({"type": "CHAR", "length": 0, "nullable": False}, "CHAR", id="char0"),
        pytest.param(
            {
                "type": "ARRAY",
                "nullable": False,
                "element_type": {"type": "CHAR", "length": 0, "nullable": False},
            },
            "CHAR",
            id="array-of-char0",
        ),
        # Flink caps TIME at 3, so a dry-run never reports TIME(4); guard the gate anyway.
        pytest.param(_time(4), "precision", id="time4"),
        pytest.param(_ts(9), "precision", id="timestamp9"),
        pytest.param(_ltz(9), "precision", id="ltz9"),
        pytest.param(
            {"type": "ARRAY", "nullable": False, "element_type": _ts(7)},
            "precision",
            id="array-of-timestamp7",
        ),
        # A required parameter the server left out would otherwise render as "None".
        pytest.param({"type": "VARCHAR", "nullable": True}, "length", id="varchar-no-length"),
        pytest.param({"type": "CHAR", "nullable": True}, "length", id="char-no-length"),
        pytest.param({"type": "BINARY", "nullable": True}, "length", id="binary-no-length"),
        pytest.param({"type": "VARBINARY", "nullable": True}, "length", id="varbinary-no-length"),
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
        pytest.param(
            {"type": "MULTISET", "nullable": True},
            "MULTISET element",
            id="multiset-no-element-type",
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
