"""Unit tests for dry_run_types (GH-118): how the drift check compares and displays dry-run
column types. Types are parsed from dry-run-shaped JSON by confluent-sql, as the adapter gets
them. The dry-run JSON spells STRING as VARCHAR(2147483647)."""

import copy

import pytest
from confluent_sql.types import ColumnTypeDefinition

from dbt.adapters.confluent.dry_run_types import comparable_type, display_type, stored_type

# A string MAP key or MULTISET element as an Avro or JSON table stores it.
STORED_KEY = {"type": "VARCHAR", "nullable": False, "length": 2147483647}
INT = {"type": "INTEGER", "nullable": True}


def _type(data: dict) -> ColumnTypeDefinition:
    return ColumnTypeDefinition.from_response(data)


def _map(key: dict, value: dict = INT) -> dict:
    return {"type": "MAP", "nullable": True, "key_type": key, "value_type": value}


def _multiset(element: dict) -> dict:
    return {"type": "MULTISET", "nullable": True, "element_type": element}


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
        """A table's string MAP key is compared as the table reports it (stored_type widens only
        the model's side)."""
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


class TestStoredType:
    @pytest.mark.parametrize(
        "key",
        [
            {"type": "CHAR", "nullable": False, "length": 1},
            {"type": "CHAR", "nullable": True, "length": 3},
            {"type": "VARCHAR", "nullable": False, "length": 5},
            {"type": "VARCHAR", "nullable": True, "length": 2147483647},
            STORED_KEY,
        ],
        ids=["char1", "nullable-char3", "varchar5", "nullable-string", "stored"],
    )
    def test_string_map_keys_compare_as_stored(self, key):
        assert stored_type(_type(_map(key))) == stored_type(_type(_map(STORED_KEY)))

    def test_other_map_keys_keep_type_and_nullability(self):
        nullable_int_key = stored_type(_type(_map(INT)))
        assert nullable_int_key.key_type == _type(INT)
        assert nullable_int_key != stored_type(_type(_map({**INT, "nullable": False})))

    @pytest.mark.parametrize(
        "element",
        [
            {"type": "CHAR", "nullable": False, "length": 1},
            {"type": "VARCHAR", "nullable": False, "length": 5},
            {"type": "VARCHAR", "nullable": True, "length": 2147483647},
            STORED_KEY,
        ],
        ids=["char1", "varchar5", "nullable-string", "stored"],
    )
    def test_string_multiset_elements_compare_as_stored(self, element):
        assert stored_type(_type(_multiset(element))) == stored_type(_type(_multiset(STORED_KEY)))

    def test_other_multiset_elements_keep_type_and_nullability(self):
        not_null_int = stored_type(_type(_multiset({**INT, "nullable": False})))
        assert not_null_int.element_type == _type({**INT, "nullable": False})
        assert not_null_int != stored_type(_type(_multiset(INT)))

    @pytest.mark.parametrize(
        "wrap",
        [
            lambda inner: {"type": "ARRAY", "nullable": True, "element_type": inner},
            lambda inner: {
                "type": "ROW",
                "nullable": True,
                "fields": [{"name": "m", "field_type": inner}],
            },
            lambda inner: _map({**INT, "nullable": False}, inner),
        ],
        ids=["array-element", "row-field", "map-value"],
    )
    def test_nested_map_keys_compare_as_stored(self, wrap):
        char_keyed = _map({"type": "CHAR", "nullable": False, "length": 1})
        assert stored_type(_type(wrap(char_keyed))) == stored_type(_type(wrap(_map(STORED_KEY))))

    def test_top_level_nullability_is_kept(self):
        assert stored_type(_type({"type": "BIGINT", "nullable": False})).nullable is False

    def test_input_is_not_modified(self):
        column_type = _type(
            {
                "type": "ARRAY",
                "nullable": False,
                "element_type": _map({"type": "CHAR", "nullable": False, "length": 1}),
            }
        )
        before = copy.deepcopy(column_type)
        stored_type(column_type)
        assert column_type == before


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
            ({"type": "TIME_WITHOUT_TIME_ZONE", "nullable": True, "precision": 3}, "TIME(3)"),
            (
                {"type": "ARRAY", "nullable": True, "element_type": {**INT, "nullable": False}},
                "ARRAY<INT NOT NULL>",
            ),
            (_map(STORED_KEY), "MAP<VARCHAR(2147483647) NOT NULL, INT>"),
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
            "time",
            "array-not-null-element",
            "map",
            "row-with-description",
            "top-level-not-null-omitted",
            "unstorable-type",
        ],
    )
    def test_spells_like_full_data_type(self, data, expected):
        assert display_type(_type(data)) == expected
