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
        assert ConfluentAdapter._normalize_type("DOUBLE   PRECISION") == "DOUBLE PRECISION"

    def test_array_angle_brackets(self):
        assert ConfluentAdapter._normalize_type("ARRAY< INT >") == "ARRAY<INT>"

    def test_map_angle_brackets(self):
        assert ConfluentAdapter._normalize_type("map< string,int >") == "MAP<STRING, INT>"

    def test_row_angle_brackets(self):
        assert ConfluentAdapter._normalize_type("ROW< name STRING, age INT >") == "ROW<NAME STRING, AGE INT>"



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
