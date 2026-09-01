"""Unit tests for ConfluentAdapter.validate_tableflow_config.

The validator is the source of truth for what shapes of `tableflow` config
the adapter accepts. `ensure_tableflow_config` trusts the config after this
runs, so any gap here lands in a confusing driver-level error instead of a
clear compile-time one.
"""

import pytest
from dbt_common.exceptions import CompilationError

from dbt.adapters.confluent.impl import ConfluentAdapter


@pytest.fixture
def adapter():
    # bypass __init__ — the validator only needs the method dispatch
    return ConfluentAdapter.__new__(ConfluentAdapter)


class TestValidateTableflowConfig:
    def test_none_short_circuits(self, adapter):
        adapter.validate_tableflow_config(None)

    @pytest.mark.parametrize(
        "valid_config",
        [
            {"formats": "ICEBERG", "storage": {"type": "managed"}},
            {"formats": ["iceberg"], "storage": {"type": "managed"}},
            {"formats": ["ICEBERG", "DELTA"], "storage": {"type": "managed"}},
            {
                "formats": "ICEBERG",
                "storage": {
                    "type": "byob_aws",
                    "bucket_name": "my-bucket",
                    "provider_integration_id": "cspi-123",
                },
            },
            {
                "formats": "ICEBERG",
                "storage": {
                    "type": "azure_adls",
                    "storage_account_name": "acct",
                    "container_name": "container",
                    "provider_integration_id": "cspi-123",
                },
            },
            {
                "formats": "ICEBERG",
                "storage": {"type": "managed"},
                "retention_ms": 604800000,
                "data_retention_ms": 0,
            },
            {
                "formats": "ICEBERG",
                "storage": {"type": "managed"},
                "retention_ms": "604800000",
            },
            {
                "formats": "ICEBERG",
                "storage": {"type": "managed"},
                "error_handling": {"mode": "suspend"},
            },
            {
                "formats": "ICEBERG",
                "storage": {"type": "managed"},
                "error_handling": {"mode": "skip"},
            },
            {
                "formats": "ICEBERG",
                "storage": {"type": "managed"},
                "error_handling": {"mode": "log", "target": "my_dlq"},
            },
            {
                "formats": "ICEBERG",
                "storage": {"type": "managed"},
                "error_handling": {"mode": "log"},
            },
        ],
        ids=[
            "single_format_managed",
            "lowercase_format",
            "multiple_formats",
            "byob_aws",
            "azure_adls",
            "retention",
            "retention_as_numeric_string",
            "error_handling_suspend",
            "error_handling_skip",
            "error_handling_log_with_target",
            "error_handling_log_default_target",
        ],
    )
    def test_valid_configs_pass(self, adapter, valid_config):
        adapter.validate_tableflow_config(valid_config)

    @pytest.mark.parametrize(
        "bad_config, expected_substring",
        [
            (["ICEBERG"], "must be a mapping"),
            ("ICEBERG", "must be a mapping"),
            ({"storage": {"type": "managed"}, "bogus": 1}, "unknown key(s): bogus"),
            ({"storage": {"type": "managed"}}, "'tableflow.formats' is required"),
            ({"formats": [], "storage": {"type": "managed"}}, "'tableflow.formats' is required"),
            (
                {"formats": ["PARQUET"], "storage": {"type": "managed"}},
                "'tableflow.formats' is invalid",
            ),
            ({"formats": "ICEBERG"}, "'tableflow.storage' is required"),
            ({"formats": "ICEBERG", "storage": "managed"}, "'tableflow.storage' is required"),
            (
                {"formats": "ICEBERG", "storage": {"type": "s3"}},
                "'tableflow.storage.type' must be one of",
            ),
            (
                {"formats": "ICEBERG", "storage": {"type": "byob_aws"}},
                "'tableflow.storage' of type 'byob_aws' is invalid",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {
                        "type": "managed",
                        "bucket_name": "extra",
                    },
                },
                "'tableflow.storage' of type 'managed' is invalid",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {
                        "type": "byob_aws",
                        "bucket_name": 12345,
                        "provider_integration_id": "cspi-1",
                    },
                },
                "'tableflow.storage.bucket_name' must be a string",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {
                        "type": "azure_adls",
                        "storage_account_name": "acct",
                        "container_name": 999,
                        "provider_integration_id": "cspi-1",
                    },
                },
                "'tableflow.storage.container_name' must be a string",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"type": "managed"},
                    "retention_ms": -1,
                },
                "must be a non-negative integer",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"type": "managed"},
                    "retention_ms": True,
                },
                "must be a non-negative integer",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"type": "managed"},
                    "retention_ms": "not-a-number",
                },
                "must be a non-negative integer",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"type": "managed"},
                    "retention_ms": [1, 2, 3],
                },
                "must be a non-negative integer",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"type": "managed"},
                    "error_handling": "suspend",
                },
                "must be a mapping with a 'mode' key",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"type": "managed"},
                    "error_handling": {"mode": "retry"},
                },
                "'tableflow.error_handling.mode' must be one of",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"type": "managed"},
                    "error_handling": {"mode": "suspend", "target": "x"},
                },
                "'tableflow.error_handling' of mode 'suspend' is invalid",
            ),
            (
                {
                    "formats": "ICEBERG",
                    "storage": {"type": "managed"},
                    "error_handling": {"mode": "log", "target": 123},
                },
                "'tableflow.error_handling.target' must be a string",
            ),
        ],
        ids=[
            "list_not_dict",
            "string_not_dict",
            "unknown_top_level_key",
            "missing_formats",
            "empty_formats",
            "unknown_format",
            "missing_storage",
            "storage_not_dict",
            "unknown_storage_type",
            "byob_aws_missing_required_keys",
            "managed_with_extra_key",
            "byob_aws_bucket_name_not_a_string",
            "azure_adls_container_name_not_a_string",
            "negative_retention_ms",
            "bool_retention_ms",
            "non_numeric_string_retention_ms",
            "list_retention_ms",
            "error_handling_not_dict",
            "unknown_error_handling_mode",
            "target_not_allowed_outside_log",
            "target_not_a_string",
        ],
    )
    def test_invalid_configs_raise(self, adapter, bad_config, expected_substring):
        with pytest.raises(CompilationError) as excinfo:
            adapter.validate_tableflow_config(bad_config)
        assert expected_substring in str(excinfo.value), (
            f"Expected error containing {expected_substring!r}, got: {excinfo.value}"
        )
