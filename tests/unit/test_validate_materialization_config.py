"""Unit tests for ConfluentAdapter.validate_materialization_config.

The validator raises a clear compile error when a dbt-confluent-specific
config key is set on a materialization that doesn't consume it (e.g.
`statement_properties` on `table`), instead of silently no-op'ing it. It only
ever inspects the closed set of keys dbt-confluent itself defines
(`all_confluent_config_keys()`), so a user's own custom config -- read by
their own hooks/macros -- is never touched, no matter what key name they pick.
"""

import pytest
from dbt_common.exceptions import CompilationError

from dbt.adapters.confluent.impl import MATERIALIZATION_CONFIG_KEYS, ConfluentAdapter


@pytest.fixture
def adapter():
    # bypass __init__ — the validator only needs the method dispatch
    return ConfluentAdapter.__new__(ConfluentAdapter)


class TestAllConfluentConfigKeys:
    def test_returns_sorted_list(self, adapter):
        keys = adapter.all_confluent_config_keys()
        assert keys == sorted(keys)

    def test_includes_every_materialization_specific_key(self, adapter):
        keys = set(adapter.all_confluent_config_keys())
        for expected in (
            "with",
            "distributed_by",
            "connector",
            "statement_properties",
            "start_mode",
            "tableflow",
        ):
            assert expected in keys


class TestValidateMaterializationConfig:
    def test_unknown_materialization_is_a_no_op(self, adapter):
        """A materialization we don't recognize (e.g. dbt-core's own `seed`,
        `snapshot`) isn't in MATERIALIZATION_CONFIG_KEYS -- never raise for it."""
        adapter.validate_materialization_config("seed", {"anything": "goes"})

    def test_no_observed_config_is_a_no_op(self, adapter):
        adapter.validate_materialization_config("table", {})

    @pytest.mark.parametrize(
        "materialization, observed_config",
        [
            ("table", {"distributed_by": {"columns": ["id"]}}),
            ("table", {"on_schema_drift": "ignore"}),
            ("table", {"statement_name": "custom"}),
            ("table", {"compute_pool_id": "lfcp-1"}),
            ("table", {"tableflow": {"formats": "ICEBERG", "storage": {"type": "managed"}}}),
            ("view", {"statement_name": "custom"}),
            ("streaming_source", {"connector": "faker", "with": {"a": "b"}}),
            (
                "streaming_source",
                {"tableflow": {"formats": "ICEBERG", "storage": {"type": "managed"}}},
            ),
            ("streaming_table", {"with": {"a": "b"}, "statement_properties": {"x": "y"}}),
            (
                "streaming_table",
                {"tableflow": {"formats": "ICEBERG", "storage": {"type": "managed"}}},
            ),
            (
                "materialized_table",
                {
                    "with": {"a": "b"},
                    "distributed_by": {"columns": ["id"]},
                    "start_mode": "FROM_BEGINNING",
                    "statement_properties": {"x": "y"},
                },
            ),
            (
                "materialized_table",
                {"tableflow": {"formats": "ICEBERG", "storage": {"type": "managed"}}},
            ),
        ],
    )
    def test_supported_config_passes(self, adapter, materialization, observed_config):
        adapter.validate_materialization_config(materialization, observed_config)

    @pytest.mark.parametrize(
        "materialization, observed_config",
        [
            ("table", {"statement_properties": {"sql.tables.scan.idle-timeout": "30 s"}}),
            ("table", {"with": {"changelog.mode": "append"}}),
            ("table", {"connector": "faker"}),
            ("view", {"distributed_by": {"columns": ["id"]}}),
            ("view", {"on_schema_drift": "ignore"}),
            ("streaming_source", {"statement_properties": {"x": "y"}}),
            ("streaming_table", {"connector": "faker"}),
            ("materialized_table", {"on_schema_drift": "ignore"}),
            ("materialized_table", {"connector": "faker"}),
            ("view", {"tableflow": {"formats": "ICEBERG", "storage": {"type": "managed"}}}),
        ],
        ids=[
            "statement_properties_on_table",
            "with_on_table",
            "connector_on_table",
            "distributed_by_on_view",
            "on_schema_drift_on_view",
            "statement_properties_on_streaming_source",
            "connector_on_streaming_table",
            "on_schema_drift_on_materialized_table",
            "connector_on_materialized_table",
            "tableflow_on_view",
        ],
    )
    def test_unsupported_config_raises(self, adapter, materialization, observed_config):
        with pytest.raises(CompilationError) as exc_info:
            adapter.validate_materialization_config(materialization, observed_config)
        (offending_key,) = observed_config.keys()
        assert offending_key in str(exc_info.value)
        assert materialization in str(exc_info.value)

    def test_error_lists_every_violation_at_once(self, adapter):
        """Multiple unsupported keys should be reported together, not one at a time."""
        with pytest.raises(CompilationError) as exc_info:
            adapter.validate_materialization_config(
                "table",
                {"statement_properties": {"x": "y"}, "with": {"a": "b"}},
            )
        msg = str(exc_info.value)
        assert "statement_properties" in msg
        assert "with" in msg

    def test_error_names_which_materializations_do_support_it(self, adapter):
        with pytest.raises(CompilationError) as exc_info:
            adapter.validate_materialization_config("table", {"statement_properties": {"x": "y"}})
        msg = str(exc_info.value)
        assert "streaming_table" in msg
        assert "materialized_table" in msg

    def test_ignore_unsupported_config_suppresses_the_error(self, adapter):
        """A model can explicitly opt a specific key out of this check."""
        adapter.validate_materialization_config(
            "table",
            {
                "statement_properties": {"x": "y"},
                "ignore_unsupported_config": ["statement_properties"],
            },
        )

    def test_ignore_unsupported_config_is_scoped_to_listed_keys_only(self, adapter):
        """Ignoring one key must not silently swallow a different violation."""
        with pytest.raises(CompilationError) as exc_info:
            adapter.validate_materialization_config(
                "table",
                {
                    "statement_properties": {"x": "y"},
                    "with": {"a": "b"},
                    "ignore_unsupported_config": ["statement_properties"],
                },
            )
        msg = str(exc_info.value)
        assert "with" in msg
        assert "statement_properties" not in msg

    @pytest.mark.parametrize(
        "bad_value",
        [
            "statement_properties",  # a bare string, not a list
            {"statement_properties": True},  # a dict
            [1, 2],  # list of non-strings
        ],
        ids=["bare_string", "dict", "non_string_items"],
    )
    def test_ignore_unsupported_config_must_be_a_list_of_strings(self, adapter, bad_value):
        with pytest.raises(CompilationError) as exc_info:
            adapter.validate_materialization_config(
                "table",
                {
                    "statement_properties": {"x": "y"},
                    "ignore_unsupported_config": bad_value,
                },
            )
        assert "ignore_unsupported_config" in str(exc_info.value)

    def test_every_materialization_supports_universal_keys(self, adapter):
        """statement_name and compute_pool_id must never be flagged, regardless
        of materialization -- every `statement()` call can read them."""
        for materialization in MATERIALIZATION_CONFIG_KEYS:
            adapter.validate_materialization_config(
                materialization,
                {"statement_name": "custom", "compute_pool_id": "lfcp-1"},
            )
