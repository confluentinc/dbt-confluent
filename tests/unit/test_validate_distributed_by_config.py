"""Unit tests for ConfluentAdapter.validate_distributed_by_config.

The validator is the source of truth for what shapes of `distributed_by`
config the adapter accepts. Downstream consumers (`get_distributed_by_clause`
in Jinja, `_check_distribution_drift` in Python) trust the config after this
runs, so any gap here lands in user-facing tracebacks.
"""

import pytest
from dbt_common.exceptions import CompilationError

from dbt.adapters.confluent.impl import ConfluentAdapter


@pytest.fixture
def adapter():
    # bypass __init__ — the validator only needs the method dispatch
    return ConfluentAdapter.__new__(ConfluentAdapter)


class TestValidateDistributedByConfig:
    def test_none_short_circuits(self, adapter):
        """An unset `distributed_by` is the common case — must be a no-op."""
        adapter.validate_distributed_by_config(None)

    @pytest.mark.parametrize(
        "valid_config",
        [
            {"columns": ["order_id"]},
            {"columns": ["order_id"], "buckets": 4},
            {"columns": ("order_id", "customer_id")},  # tuple is also a sequence
            {"columns": ["a", "b", "c"], "buckets": 1},
        ],
        ids=["columns_only", "columns_and_buckets", "tuple_columns", "many_columns"],
    )
    def test_valid_configs_pass(self, adapter, valid_config):
        adapter.validate_distributed_by_config(valid_config)

    @pytest.mark.parametrize(
        "bad_config, expected_substring",
        [
            # not a mapping
            (["order_id"], "must be a mapping"),
            ("order_id", "must be a mapping"),
            (42, "must be a mapping"),
            # missing or empty columns
            ({}, "non-empty 'columns' list"),
            ({"columns": []}, "non-empty 'columns' list"),
            ({"columns": None}, "non-empty 'columns' list"),
            # columns is a string (would otherwise iterate per-character)
            ({"columns": "order_id"}, "non-empty 'columns' list"),
            # column entries
            ({"columns": [42]}, "non-empty strings"),
            ({"columns": [""]}, "non-empty strings"),
            ({"columns": ["foo`bar"]}, "backtick"),
            # buckets
            ({"columns": ["c"], "buckets": 0}, "must be a positive integer"),
            ({"columns": ["c"], "buckets": -1}, "must be a positive integer"),
            ({"columns": ["c"], "buckets": 1.5}, "must be a positive integer"),
            ({"columns": ["c"], "buckets": "four"}, "must be a positive integer"),
            ({"columns": ["c"], "buckets": True}, "must be a positive integer"),
            # unknown keys
            ({"columns": ["c"], "strategy": "range"}, "unknown key 'strategy'"),
        ],
        ids=[
            "list_not_dict",
            "string_not_dict",
            "int_not_dict",
            "missing_columns",
            "empty_columns",
            "none_columns",
            "string_columns",
            "int_column_entry",
            "empty_string_column_entry",
            "backtick_in_column_name",
            "buckets_zero",
            "buckets_negative",
            "buckets_float",
            "buckets_string",
            "buckets_bool_true",
            "unknown_key",
        ],
    )
    def test_invalid_configs_raise(self, adapter, bad_config, expected_substring):
        with pytest.raises(CompilationError) as excinfo:
            adapter.validate_distributed_by_config(bad_config)
        assert expected_substring in str(excinfo.value), (
            f"Expected error containing {expected_substring!r}, got: {excinfo.value}"
        )

    def test_unknown_key_is_stable_across_runs(self, adapter):
        """Multiple unknown keys → message names the lexicographically first one
        so error output is stable regardless of dict iteration order."""
        with pytest.raises(CompilationError) as excinfo:
            adapter.validate_distributed_by_config({"columns": ["c"], "zeta": 1, "alpha": 2})
        assert "unknown key 'alpha'" in str(excinfo.value)
