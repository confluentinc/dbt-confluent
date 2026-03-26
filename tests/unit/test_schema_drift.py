"""Unit tests for schema drift detection logic in ConfluentAdapter."""

import agate
import pytest
from dbt_common.exceptions import CompilationError

from dbt.adapters.confluent.impl import ConfluentAdapter

# ---------------------------------------------------------------------------
# _normalize_type
# ---------------------------------------------------------------------------

class TestNormalizeType:
    def test_simple_type(self):
        assert ConfluentAdapter._normalize_type("bigint") == "BIGINT"

    def test_parameterized_type(self):
        assert ConfluentAdapter._normalize_type("DECIMAL(10, 2)") == "DECIMAL(10, 2)"

    def test_no_space_after_comma(self):
        assert ConfluentAdapter._normalize_type("DECIMAL(10,2)") == "DECIMAL(10, 2)"

    def test_extra_spaces(self):
        assert ConfluentAdapter._normalize_type("  decimal( 10 , 2 )  ") == "DECIMAL(10, 2)"

    def test_timestamp_precision(self):
        assert ConfluentAdapter._normalize_type("timestamp(3)") == "TIMESTAMP(3)"

    def test_collapses_internal_whitespace(self):
        assert ConfluentAdapter._normalize_type("DOUBLE   PRECISION") == "DOUBLE"

    def test_array_angle_brackets(self):
        assert ConfluentAdapter._normalize_type("ARRAY< INT >") == "ARRAY<INT>"

    def test_map_angle_brackets(self):
        assert ConfluentAdapter._normalize_type("map< string,int >") == "MAP<STRING, INT>"

    def test_row_angle_brackets(self):
        assert ConfluentAdapter._normalize_type("ROW< name STRING, age INT >") == "ROW<NAME STRING, AGE INT>"

    def test_alias_string(self):
        assert ConfluentAdapter._normalize_type("STRING") == "VARCHAR(2147483647)"

    def test_alias_bytes(self):
        assert ConfluentAdapter._normalize_type("BYTES") == "VARBINARY(2147483647)"

    def test_alias_integer(self):
        assert ConfluentAdapter._normalize_type("INTEGER") == "INT"

    def test_alias_double_precision(self):
        assert ConfluentAdapter._normalize_type("DOUBLE PRECISION") == "DOUBLE"

    def test_alias_not_applied_to_parameterized(self):
        """STRING alias should only match the bare keyword, not VARCHAR(n)."""
        assert ConfluentAdapter._normalize_type("VARCHAR(100)") == "VARCHAR(100)"


# ---------------------------------------------------------------------------
# parse_column_definitions
# ---------------------------------------------------------------------------

class TestParseColumnDefinitions:
    """Test the SQL column definition parser."""

    @pytest.fixture
    def adapter(self):
        """Minimal adapter instance — only need the method, not a real connection."""
        return ConfluentAdapter.__new__(ConfluentAdapter)

    def test_basic_columns(self, adapter):
        sql = "`id` BIGINT,\n`value` STRING"
        result = adapter.parse_column_definitions(sql)
        assert result == [
            {"column_name": "id", "data_type": "BIGINT"},
            {"column_name": "value", "data_type": "VARCHAR(2147483647)"},
        ]

    def test_parameterized_type(self, adapter):
        sql = "`price` DECIMAL(10, 2)"
        result = adapter.parse_column_definitions(sql)
        assert result == [{"column_name": "price", "data_type": "DECIMAL(10, 2)"}]

    def test_timestamp_precision(self, adapter):
        sql = "`ts` TIMESTAMP(3)"
        result = adapter.parse_column_definitions(sql)
        assert result == [{"column_name": "ts", "data_type": "TIMESTAMP(3)"}]

    def test_skips_watermark(self, adapter):
        sql = "`ts` TIMESTAMP(3),\nWATERMARK FOR ts AS ts - INTERVAL '5' SECOND"
        result = adapter.parse_column_definitions(sql)
        assert result == [{"column_name": "ts", "data_type": "TIMESTAMP(3)"}]

    def test_skips_primary_key(self, adapter):
        sql = "`id` BIGINT,\nPRIMARY KEY(`id`) NOT ENFORCED"
        result = adapter.parse_column_definitions(sql)
        assert result == [{"column_name": "id", "data_type": "BIGINT"}]

    def test_full_source_for_drift(self, adapter):
        """Parse the full SOURCE_FOR_DRIFT fixture used in functional tests."""
        sql = (
            "`order_id` BIGINT,\n"
            "`price` DECIMAL(10, 2),\n"
            "`order_time` TIMESTAMP(3),\n"
            "WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,\n"
            "PRIMARY KEY(`order_id`) NOT ENFORCED"
        )
        result = adapter.parse_column_definitions(sql)
        assert result == [
            {"column_name": "order_id", "data_type": "BIGINT"},
            {"column_name": "price", "data_type": "DECIMAL(10, 2)"},
            {"column_name": "order_time", "data_type": "TIMESTAMP(3)"},
        ]

    def test_single_column_no_comma(self, adapter):
        sql = "`id` BIGINT"
        result = adapter.parse_column_definitions(sql)
        assert result == [{"column_name": "id", "data_type": "BIGINT"}]

    def test_normalizes_types(self, adapter):
        sql = "`price` decimal(10,2)"
        result = adapter.parse_column_definitions(sql)
        assert result == [{"column_name": "price", "data_type": "DECIMAL(10, 2)"}]

    def test_array_type(self, adapter):
        sql = "`tags` ARRAY<STRING>"
        result = adapter.parse_column_definitions(sql)
        assert result == [{"column_name": "tags", "data_type": "ARRAY<STRING>"}]

    def test_map_type(self, adapter):
        sql = "`data` MAP<STRING, INT>, `id` BIGINT"
        result = adapter.parse_column_definitions(sql)
        assert result == [
            {"column_name": "data", "data_type": "MAP<STRING, INT>"},
            {"column_name": "id", "data_type": "BIGINT"},
        ]

    def test_row_type(self, adapter):
        sql = "`customer` ROW<name STRING, age INT>, `id` BIGINT"
        result = adapter.parse_column_definitions(sql)
        assert result == [
            {"column_name": "customer", "data_type": "ROW<NAME STRING, AGE INT>"},
            {"column_name": "id", "data_type": "BIGINT"},
        ]

    def test_multiword_type(self, adapter):
        sql = "`ts` TIMESTAMP(3) WITH LOCAL TIME ZONE"
        result = adapter.parse_column_definitions(sql)
        assert result == [{"column_name": "ts", "data_type": "TIMESTAMP(3) WITH LOCAL TIME ZONE"}]

    def test_nested_collection_type(self, adapter):
        sql = "`matrix` ARRAY<ARRAY<INT>>, `id` BIGINT"
        result = adapter.parse_column_definitions(sql)
        assert result == [
            {"column_name": "matrix", "data_type": "ARRAY<ARRAY<INT>>"},
            {"column_name": "id", "data_type": "BIGINT"},
        ]


# ---------------------------------------------------------------------------
# check_schema_drift
# ---------------------------------------------------------------------------

def _make_agate_table(rows):
    """Create an agate.Table with column_name and data_type columns."""
    return agate.Table(rows, column_names=["column_name", "data_type"])


class TestCheckSchemaDrift:
    """Test the drift comparison logic."""

    @pytest.fixture
    def adapter(self):
        return ConfluentAdapter.__new__(ConfluentAdapter)

    def test_no_drift(self, adapter):
        existing = _make_agate_table([("id", "BIGINT"), ("value", "STRING")])
        expected = _make_agate_table([("id", "BIGINT"), ("value", "STRING")])
        # Should not raise
        adapter.check_schema_drift("my_table", existing, expected, {}, {})

    def test_no_drift_with_list_expected(self, adapter):
        existing = _make_agate_table([("id", "BIGINT"), ("value", "STRING")])
        expected = [
            {"column_name": "id", "data_type": "BIGINT"},
            {"column_name": "value", "data_type": "STRING"},
        ]
        adapter.check_schema_drift("my_table", existing, expected, {}, {})

    def test_extra_column_detected(self, adapter):
        existing = _make_agate_table([("id", "BIGINT")])
        expected = _make_agate_table([("id", "BIGINT"), ("extra", "STRING")])
        with pytest.raises(CompilationError, match="drift detected"):
            adapter.check_schema_drift("t", existing, expected, {}, {})

    def test_removed_column_detected(self, adapter):
        existing = _make_agate_table([("id", "BIGINT"), ("value", "STRING")])
        expected = _make_agate_table([("id", "BIGINT")])
        with pytest.raises(CompilationError, match="drift detected"):
            adapter.check_schema_drift("t", existing, expected, {}, {})

    def test_renamed_column_detected(self, adapter):
        existing = _make_agate_table([("id", "BIGINT"), ("value", "STRING")])
        expected = _make_agate_table([("id", "BIGINT"), ("name", "STRING")])
        with pytest.raises(CompilationError, match="drift detected"):
            adapter.check_schema_drift("t", existing, expected, {}, {})

    def test_type_change_detected(self, adapter):
        existing = _make_agate_table([("id", "BIGINT")])
        expected = _make_agate_table([("id", "INT")])
        with pytest.raises(CompilationError, match="type mismatch"):
            adapter.check_schema_drift("t", existing, expected, {}, {})

    def test_column_order_ignored(self, adapter):
        existing = _make_agate_table([("a", "BIGINT"), ("b", "STRING")])
        expected = _make_agate_table([("b", "STRING"), ("a", "BIGINT")])
        adapter.check_schema_drift("t", existing, expected, {}, {})

    def test_case_insensitive_names(self, adapter):
        existing = _make_agate_table([("ID", "BIGINT")])
        expected = _make_agate_table([("id", "BIGINT")])
        adapter.check_schema_drift("t", existing, expected, {}, {})

    def test_type_normalization(self, adapter):
        existing = _make_agate_table([("price", "DECIMAL(10, 2)")])
        expected = [{"column_name": "price", "data_type": "decimal(10,2)"}]
        adapter.check_schema_drift("t", existing, expected, {}, {})

    def test_options_drift_detected(self, adapter):
        existing = _make_agate_table([("id", "BIGINT")])
        expected = _make_agate_table([("id", "BIGINT")])
        with pytest.raises(CompilationError, match="drift detected"):
            adapter.check_schema_drift(
                "t", existing, expected,
                expected_with={"changelog.mode": "append"},
                existing_options={"changelog.mode": "upsert"},
            )

    def test_options_missing_detected(self, adapter):
        existing = _make_agate_table([("id", "BIGINT")])
        expected = _make_agate_table([("id", "BIGINT")])
        with pytest.raises(CompilationError, match="drift detected"):
            adapter.check_schema_drift(
                "t", existing, expected,
                expected_with={"changelog.mode": "append"},
                existing_options={},
            )

    def test_extra_existing_options_allowed(self, adapter):
        """Extra options in the existing table (e.g. connector defaults) are fine."""
        existing = _make_agate_table([("id", "BIGINT")])
        expected = _make_agate_table([("id", "BIGINT")])
        adapter.check_schema_drift(
            "t", existing, expected,
            expected_with={"changelog.mode": "upsert"},
            existing_options={"changelog.mode": "upsert", "connector": "faker"},
        )

    def test_no_options_check_when_empty(self, adapter):
        """When expected_with is empty, no options comparison happens."""
        existing = _make_agate_table([("id", "BIGINT")])
        expected = _make_agate_table([("id", "BIGINT")])
        adapter.check_schema_drift(
            "t", existing, expected,
            expected_with={},
            existing_options={"anything": "here"},
        )
