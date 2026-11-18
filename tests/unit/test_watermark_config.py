"""Unit tests for the `watermark` config: validation and rendering into the column block."""

from types import SimpleNamespace

import pytest
from dbt_common.exceptions import CompilationError

from dbt.adapters.confluent.impl import ConfluentAdapter

WATERMARK_COLUMN = "event_ts"
WATERMARK_EXPRESSION = "`event_ts` - INTERVAL '15' SECOND"
WATERMARK = {"column": WATERMARK_COLUMN, "expression": WATERMARK_EXPRESSION}


class _Config:
    def __init__(self, enforced, materialized="streaming_table"):
        self._values = {
            "contract": SimpleNamespace(enforced=enforced),
            "materialized": materialized,
        }

    def get(self, key, default=None):
        return self._values.get(key, default)


@pytest.fixture
def adapter():
    # bypass __init__ — these methods only need method dispatch
    return ConfluentAdapter.__new__(ConfluentAdapter)


class TestValidateWatermarkConfig:
    def test_none_short_circuits(self, adapter):
        adapter.validate_watermark_config(None, _Config(enforced=False))

    def test_valid_with_enforced_contract(self, adapter):
        adapter.validate_watermark_config(WATERMARK, _Config(enforced=True))

    @pytest.mark.parametrize(
        "bad",
        [
            "event_ts",
            {},
            {"column": "event_ts"},
            {"expression": WATERMARK_EXPRESSION},
            {"column": "", "expression": WATERMARK_EXPRESSION},
            {"column": "event_ts", "expression": "  "},
            {"column": 1, "expression": WATERMARK_EXPRESSION},
        ],
    )
    def test_malformed_shape_rejected(self, adapter, bad):
        with pytest.raises(CompilationError, match="non-empty string 'column' and 'expression'"):
            adapter.validate_watermark_config(bad, _Config(enforced=True))

    def test_backtick_in_column_rejected(self, adapter):
        bad = {"column": "a`b", "expression": WATERMARK_EXPRESSION}
        with pytest.raises(CompilationError, match="backtick"):
            adapter.validate_watermark_config(bad, _Config(enforced=True))

    def test_unknown_key_rejected(self, adapter):
        with pytest.raises(CompilationError, match="unknown key 'delay'"):
            adapter.validate_watermark_config({**WATERMARK, "delay": 1}, _Config(enforced=True))

    def test_streaming_table_requires_enforced_contract(self, adapter):
        with pytest.raises(CompilationError, match="enforced contract"):
            adapter.validate_watermark_config(WATERMARK, _Config(enforced=False))

    def test_materialized_table_needs_no_contract(self, adapter):
        config = _Config(enforced=False, materialized="materialized_table")
        adapter.validate_watermark_config(WATERMARK, config)


class TestAppendWatermark:
    def test_appended_as_last_entry_of_column_block(self, adapter):
        columns_ddl = "(\n  user_id STRING NOT NULL,\n  event_ts TIMESTAMP_LTZ(3) NOT NULL\n)\n"
        assert adapter.append_watermark(columns_ddl, WATERMARK) == (
            "(\n  user_id STRING NOT NULL,\n  event_ts TIMESTAMP_LTZ(3) NOT NULL,\n"
            f"  WATERMARK FOR `{WATERMARK_COLUMN}` AS {WATERMARK_EXPRESSION}\n)"
        )

    def test_without_columns_is_a_watermark_only_block(self, adapter):
        assert adapter.append_watermark("", WATERMARK) == (
            f"(\n  WATERMARK FOR `{WATERMARK_COLUMN}` AS {WATERMARK_EXPRESSION}\n)"
        )

    def test_unparenthesized_ddl_rejected(self, adapter):
        with pytest.raises(CompilationError, match="parenthesized"):
            adapter.append_watermark("user_id STRING", WATERMARK)
