"""Unit tests for dry_run_types: how the drift check compares and displays dry-run column
types. Types are parsed from dry-run-shaped JSON by confluent-sql, as the adapter gets them.
The dry-run JSON spells STRING as VARCHAR(2147483647)."""

import copy

import pytest
from confluent_sql.types import ColumnTypeDefinition

from dbt.adapters.confluent.dry_run_types import comparable_type, display_type, types_match
from dbt.adapters.confluent.impl import ConfluentAdapter

# A string MAP key or MULTISET element as an Avro or JSON table stores it.
STORED_KEY = {"type": "VARCHAR", "nullable": False, "length": 2147483647}
INT = {"type": "INTEGER", "nullable": True}
# Key and element types a Protobuf table keeps as declared.
CHAR1 = {"type": "CHAR", "nullable": True, "length": 1}
VARCHAR5 = {"type": "VARCHAR", "nullable": True, "length": 5}


def _type(data: dict) -> ColumnTypeDefinition:
    return ColumnTypeDefinition.from_response(data)


def _map(key: dict, value: dict = INT) -> dict:
    return {"type": "MAP", "nullable": True, "key_type": key, "value_type": value}


def _multiset(element: dict) -> dict:
    return {"type": "MULTISET", "nullable": True, "element_type": element}


def _row(*fields: tuple[str, dict]) -> dict:
    return {
        "type": "ROW",
        "nullable": True,
        "fields": [{"name": name, "field_type": field_type} for name, field_type in fields],
    }


def _structured(class_name: str) -> dict:
    return {**_row(("x", INT)), "type": "STRUCTURED_TYPE", "class_name": class_name}


def _interval(resolution: str, fractional_precision: int = 6) -> dict:
    return {
        "type": "INTERVAL_DAY_TIME",
        "nullable": True,
        "resolution": resolution,
        "precision": 2,
        "fractional_precision": fractional_precision,
    }


def _raw(class_name: str) -> dict:
    return {"type": "RAW", "nullable": True, "class_name": class_name}


class TestComparableType:
    def test_top_level_nullability_is_dropped(self):
        not_null = _type({"type": "BIGINT", "nullable": False})
        nullable = _type({"type": "BIGINT", "nullable": True})
        assert comparable_type(not_null) == comparable_type(nullable)

    def test_nested_nullability_is_kept(self):
        not_null = {"type": "ARRAY", "nullable": True, "element_type": {**INT, "nullable": False}}
        nullable = {"type": "ARRAY", "nullable": True, "element_type": INT}
        assert comparable_type(_type(not_null)) != comparable_type(_type(nullable))

    def test_string_map_keys_are_kept(self):
        """A table's string MAP key is compared as the table reports it (types_match decides
        whether the model's fits in it)."""
        char_keyed = comparable_type(_type(_map({"type": "CHAR", "nullable": False, "length": 1})))
        assert char_keyed != comparable_type(_type(_map(STORED_KEY)))

    def test_input_is_not_modified(self):
        column_type = _type(
            {
                "type": "ARRAY",
                "nullable": False,
                "element_type": _map({"type": "CHAR", "nullable": False, "length": 1}),
            }
        )
        before = copy.deepcopy(column_type)
        comparable_type(column_type)
        assert column_type == before


class TestTypesMatch:
    @pytest.mark.parametrize("wrap", [_map, _multiset], ids=["map-key", "multiset-element"])
    @pytest.mark.parametrize(
        "expected_member",
        [
            {"type": "CHAR", "nullable": False, "length": 1},
            {"type": "CHAR", "nullable": True, "length": 3},
            {"type": "VARCHAR", "nullable": False, "length": 5},
            {"type": "VARCHAR", "nullable": True, "length": 2147483647},
            STORED_KEY,
        ],
        ids=["char1", "nullable-char3", "varchar5", "nullable-string", "stored"],
    )
    def test_string_members_fit_an_avro_stored_type(self, wrap, expected_member):
        assert types_match(_type(wrap(STORED_KEY)), _type(wrap(expected_member)))

    @pytest.mark.parametrize("wrap", [_map, _multiset], ids=["map-key", "multiset-element"])
    @pytest.mark.parametrize(
        "existing_member,expected_member,fits",
        [
            (VARCHAR5, VARCHAR5, True),
            (VARCHAR5, {**VARCHAR5, "length": 3}, True),
            (VARCHAR5, {**VARCHAR5, "nullable": False}, True),
            ({**VARCHAR5, "nullable": False}, VARCHAR5, True),
            (VARCHAR5, CHAR1, True),
            (CHAR1, CHAR1, True),
            (VARCHAR5, {**VARCHAR5, "length": 10}, False),
            (CHAR1, {**VARCHAR5, "length": 1}, False),
            (CHAR1, {**CHAR1, "length": 3}, False),
            ({**CHAR1, "length": 3}, CHAR1, False),
            (INT, VARCHAR5, False),
            ({**INT, "nullable": False}, INT, False),
            (STORED_KEY, {"type": "VARCHAR", "nullable": True}, False),
        ],
        ids=[
            "same",
            "shorter",
            "not-null-into-nullable",
            "nullable-into-not-null",
            "char-into-varchar",
            "same-char",
            "longer",
            "varchar-into-char",
            "longer-char",
            "shorter-char",
            "string-into-int",
            "int-nullability",
            "no-length",
        ],
    )
    def test_fits_rule(self, wrap, existing_member, expected_member, fits):
        """A Protobuf table keeps the declared key or element type (GH-118 probe run b68104c4).
        A fitting one matches; any other must be equal, nullability included."""
        assert types_match(_type(wrap(existing_member)), _type(wrap(expected_member))) is fits

    @pytest.mark.parametrize("member", [_map, _multiset], ids=["map-key", "multiset-element"])
    @pytest.mark.parametrize(
        "wrap",
        [
            lambda inner: {"type": "ARRAY", "nullable": True, "element_type": inner},
            lambda inner: _row(("m", inner)),
            lambda inner: _map({**INT, "nullable": False}, inner),
            lambda inner: _map(inner),
            lambda inner: _multiset(inner),
        ],
        ids=["array-element", "row-field", "map-value", "map-key", "multiset-element"],
    )
    def test_nested_members_fit(self, member, wrap):
        existing = _type(wrap(member(VARCHAR5)))
        assert types_match(existing, _type(wrap(member(CHAR1))))
        assert not types_match(existing, _type(wrap(member({**VARCHAR5, "length": 10}))))

    @pytest.mark.parametrize(
        "wrap",
        [
            lambda inner: {"type": "ARRAY", "nullable": True, "element_type": inner},
            lambda inner: _map(INT, inner),
            lambda inner: _row(("s", inner)),
        ],
        ids=["array-element", "map-value", "row-field"],
    )
    def test_other_string_positions_must_match(self, wrap):
        """Only MAP keys and MULTISET elements are fitted; a shorter string anywhere else still
        drifts."""
        assert not types_match(_type(wrap(VARCHAR5)), _type(wrap({**VARCHAR5, "length": 3})))

    @pytest.mark.parametrize(
        "existing,expected",
        [
            ({"type": "ARRAY", "nullable": True, "element_type": STORED_KEY}, _multiset(CHAR1)),
            (_map(STORED_KEY), _multiset(CHAR1)),
            (_row(("m", _multiset(STORED_KEY)), ("n", INT)), _row(("m", _multiset(CHAR1)))),
            (_row(("m", _multiset(STORED_KEY))), _row(("n", _multiset(CHAR1)))),
        ],
        ids=["multiset-to-array", "multiset-to-map", "row-field-count", "row-field-name"],
    )
    def test_different_shapes_do_not_match(self, existing, expected):
        assert not types_match(_type(existing), _type(expected))

    def test_top_level_nullability_is_ignored(self):
        assert types_match(
            _type({"type": "BIGINT", "nullable": True}),
            _type({"type": "BIGINT", "nullable": False}),
        )


class TestDisplayType:
    @pytest.mark.parametrize(
        "data,expected",
        [
            ({"type": "BIGINT", "nullable": True}, "BIGINT"),
            (INT, "INT"),
            ({"type": "DECIMAL", "nullable": True, "precision": 10, "scale": 2}, "DECIMAL(10, 2)"),
            ({"type": "VARCHAR", "nullable": True, "length": 5}, "VARCHAR(5)"),
            (
                {"type": "TIMESTAMP_WITHOUT_TIME_ZONE", "nullable": True, "precision": 3},
                "TIMESTAMP(3)",
            ),
            (
                {"type": "TIMESTAMP_WITH_LOCAL_TIME_ZONE", "nullable": True, "precision": 3},
                "TIMESTAMP(3) WITH LOCAL TIME ZONE",
            ),
            (
                {"type": "TIMESTAMP_WITH_TIME_ZONE", "nullable": True, "precision": 3},
                "TIMESTAMP(3) WITH TIME ZONE",
            ),
            ({"type": "TIME_WITHOUT_TIME_ZONE", "nullable": True, "precision": 3}, "TIME(3)"),
            (
                {"type": "ARRAY", "nullable": True, "element_type": {**INT, "nullable": False}},
                "ARRAY<INT NOT NULL>",
            ),
            (_map(STORED_KEY), "MAP<VARCHAR(2147483647) NOT NULL, INT>"),
            (
                _multiset({"type": "VARCHAR", "nullable": False, "length": 5}),
                "MULTISET<VARCHAR(5) NOT NULL>",
            ),
            (
                {
                    "type": "ROW",
                    "nullable": True,
                    "fields": [
                        {"name": "a", "field_type": {**INT, "nullable": False}},
                        {
                            "name": "b",
                            "field_type": {"type": "VARCHAR", "nullable": True, "length": 3},
                            "description": "it's b",
                        },
                    ],
                },
                "ROW<`a` INT NOT NULL, `b` VARCHAR(3) 'it''s b'>",
            ),
            (_row(("a`b", INT)), "ROW<`a``b` INT>"),
            (_row(("`", INT)), "ROW<```` INT>"),
            ({"type": "BIGINT", "nullable": False}, "BIGINT"),
            ({"type": "VARIANT", "nullable": True}, "VARIANT"),
        ],
        ids=[
            "bigint",
            "int",
            "decimal",
            "varchar",
            "timestamp",
            "timestamp-ltz",
            "timestamp-tz",
            "time",
            "array-not-null-element",
            "map",
            "multiset",
            "row-with-description",
            "row-field-embedded-backtick",
            "row-field-only-backtick",
            "top-level-not-null-omitted",
            "unstorable-type",
        ],
    )
    def test_spells_like_full_data_type(self, data, expected):
        assert display_type(_type(data)) == expected

    @pytest.mark.parametrize(
        "data,expected",
        [
            (
                _interval("DAY_TO_SECOND"),
                "INTERVAL_DAY_TIME(2, resolution=DAY_TO_SECOND, fractional_precision=6)",
            ),
            (
                {
                    "type": "INTERVAL_YEAR_MONTH",
                    "nullable": True,
                    "resolution": "YEAR_TO_MONTH",
                    "precision": 2,
                },
                "INTERVAL_YEAR_MONTH(2, resolution=YEAR_TO_MONTH)",
            ),
            (_raw("java.util.UUID"), "RAW(class_name=java.util.UUID)"),
            (_structured("com.example.A"), "STRUCTURED_TYPE(class_name=com.example.A)<`x` INT>"),
        ],
        ids=["interval-day-second", "interval-year-month", "raw", "structured"],
    )
    def test_spells_other_types_with_every_parameter(self, data, expected):
        """A type without a FULL_DATA_TYPE spelling here isn't spelled as SQL, but keeps every
        parameter, and a structured type isn't spelled as a ROW."""
        assert display_type(_type(data)) == expected

    @pytest.mark.parametrize(
        "left,right",
        [
            (_interval("DAY_TO_SECOND", 3), _interval("DAY_TO_SECOND", 6)),
            (_interval("DAY"), _interval("DAY_TO_HOUR")),
            (_structured("com.example.A"), _structured("com.example.B")),
            (_raw("java.util.UUID"), _raw("java.time.Instant")),
            (_row(("a", INT), ("b", INT)), _row(("a` INT, `b", INT))),
        ],
        ids=[
            "interval-fractional-precision",
            "interval-resolution",
            "structured-class",
            "raw-class",
            "row-field-name-backtick",
        ],
    )
    def test_different_types_spell_differently(self, left, right):
        """Otherwise a drift message reads existing='X', expected='X'."""
        left_type = comparable_type(_type(left))
        right_type = comparable_type(_type(right))
        assert left_type != right_type
        assert display_type(left_type) != display_type(right_type)

    def test_drift_message_spells_the_difference(self):
        """End to end through the adapter's message builder."""
        existing = {"d": comparable_type(_type(_interval("DAY_TO_SECOND", 3)))}
        expected = {"d": comparable_type(_type(_interval("DAY_TO_SECOND", 6)))}
        assert ConfluentAdapter._check_column_drift(existing, expected, display=display_type) == [
            "column type: 'd' "
            "existing='INTERVAL_DAY_TIME(2, resolution=DAY_TO_SECOND, fractional_precision=3)', "
            "expected='INTERVAL_DAY_TIME(2, resolution=DAY_TO_SECOND, fractional_precision=6)'"
        ]
