"""Unit tests for ConfluentAdapter.validate_materialized_table_config.

The validator rejects configs that exist in open-source Flink materialized
tables but not in Confluent's dialect. It sees the model's Jinja config
object (anything with `.get`), so a plain dict stands in for it here.
"""

import pytest
from dbt_common.exceptions import CompilationError

from dbt.adapters.confluent.impl import ConfluentAdapter


@pytest.fixture
def adapter():
    # bypass __init__ — the validator only needs the method dispatch
    return ConfluentAdapter.__new__(ConfluentAdapter)


class TestValidateMaterializedTableConfig:
    def test_empty_config_passes(self, adapter):
        adapter.validate_materialized_table_config({})

    def test_supported_keys_pass(self, adapter):
        adapter.validate_materialized_table_config(
            {
                "distributed_by": {"columns": ["order_id"], "buckets": 4},
                "with": {"key.format": "avro-registry"},
                "start_mode": "FROM_BEGINNING",
            }
        )

    def test_none_valued_unsupported_key_passes(self, adapter):
        """A key set to None counts as unset, matching config.get semantics."""
        adapter.validate_materialized_table_config({"refresh_mode": None})

    @pytest.mark.parametrize("key", ["freshness_interval", "refresh_mode", "partition_by"])
    def test_single_unsupported_key_raises(self, adapter, key):
        with pytest.raises(CompilationError) as excinfo:
            adapter.validate_materialized_table_config({key: "x"})
        message = str(excinfo.value)
        assert f"'{key}' is not supported" in message
        for supported_key in ("distributed_by", "with", "start_mode", "statement_name"):
            assert supported_key in message

    def test_multiple_unsupported_keys_bundled(self, adapter):
        """All offending keys land in one error, in a stable order."""
        with pytest.raises(CompilationError) as excinfo:
            adapter.validate_materialized_table_config(
                {"partition_by": ["x"], "freshness_interval": "1h"}
            )
        assert "'freshness_interval', 'partition_by' are not supported" in str(excinfo.value)
