"""Unit tests for schema drift detection logic in ConfluentAdapter.

The public `check_schema_drift` is a thin orchestrator over four helpers:
- `_partition_drift_catalog` splits the unified UNION ALL agate.Table into
  per-concern dicts.
- `_check_column_drift`, `_check_options_drift`, `_check_distribution_drift`
  raise CompilationError when their concern has drifted.

We test the helpers directly rather than fabricating a unified catalog for
every case — the orchestrator is small enough that a couple of partition
tests cover its glue, while the per-concern logic gets exhaustive coverage.
"""

import agate
import pytest
from dbt_common.exceptions import CompilationError

from dbt.adapters.confluent.impl import ConfluentAdapter

# ---------------------------------------------------------------------------
# _check_column_drift
# ---------------------------------------------------------------------------


class TestCheckColumnDrift:
    def test_no_drift(self):
        existing = {"id": "BIGINT", "value": "STRING"}
        expected = {"id": "BIGINT", "value": "STRING"}
        ConfluentAdapter._check_column_drift("t", existing, expected)

    def test_extra_column_detected(self):
        existing = {"id": "BIGINT"}
        expected = {"id": "BIGINT", "extra": "STRING"}
        with pytest.raises(CompilationError, match="drift detected"):
            ConfluentAdapter._check_column_drift("t", existing, expected)

    def test_removed_column_detected(self):
        existing = {"id": "BIGINT", "value": "STRING"}
        expected = {"id": "BIGINT"}
        with pytest.raises(CompilationError, match="drift detected"):
            ConfluentAdapter._check_column_drift("t", existing, expected)

    def test_renamed_column_detected(self):
        existing = {"id": "BIGINT", "value": "STRING"}
        expected = {"id": "BIGINT", "name": "STRING"}
        with pytest.raises(CompilationError, match="drift detected"):
            ConfluentAdapter._check_column_drift("t", existing, expected)

    def test_type_change_detected(self):
        existing = {"id": "BIGINT"}
        expected = {"id": "INT"}
        with pytest.raises(CompilationError, match="type mismatch"):
            ConfluentAdapter._check_column_drift("t", existing, expected)

    def test_column_order_ignored(self):
        existing = {"a": "BIGINT", "b": "STRING"}
        expected = {"b": "STRING", "a": "BIGINT"}
        ConfluentAdapter._check_column_drift("t", existing, expected)

    def test_case_sensitive_names(self):
        """Flink allows distinct columns differing only by case (when backtick-quoted).
        Both sides come from INFORMATION_SCHEMA which preserves declared casing,
        so a case difference is real drift."""
        existing = {"ID": "BIGINT"}
        expected = {"id": "BIGINT"}
        with pytest.raises(CompilationError, match="drift detected"):
            ConfluentAdapter._check_column_drift("t", existing, expected)


# ---------------------------------------------------------------------------
# _check_options_drift
# ---------------------------------------------------------------------------


class TestCheckOptionsDrift:
    def test_options_drift_detected(self):
        with pytest.raises(CompilationError, match="drift detected"):
            ConfluentAdapter._check_options_drift(
                "t",
                expected_with={"changelog.mode": "append"},
                existing_options={"changelog.mode": "upsert"},
            )

    def test_options_missing_detected(self):
        with pytest.raises(CompilationError, match="drift detected"):
            ConfluentAdapter._check_options_drift(
                "t",
                expected_with={"changelog.mode": "append"},
                existing_options={},
            )

    def test_extra_existing_options_allowed(self):
        """Extra options in the existing table (e.g. connector defaults) are fine."""
        ConfluentAdapter._check_options_drift(
            "t",
            expected_with={"changelog.mode": "upsert"},
            existing_options={"changelog.mode": "upsert", "connector": "faker"},
        )

    def test_no_options_check_when_empty(self):
        ConfluentAdapter._check_options_drift(
            "t",
            expected_with={},
            existing_options={"anything": "here"},
        )

    def test_options_non_string_value_coerced(self):
        """Config values (int, bool) are coerced to str before comparing with I_S strings."""
        # No drift: int 1 should match string "1"
        ConfluentAdapter._check_options_drift(
            "t",
            expected_with={"rows-per-second": 1},
            existing_options={"rows-per-second": "1"},
        )


# ---------------------------------------------------------------------------
# _check_distribution_drift
# ---------------------------------------------------------------------------


class TestCheckDistributionDrift:
    def test_unset_expected_skips_check(self):
        """Confluent assigns a default distribution to most tables, so we only
        verify what the user explicitly requested (mirrors WITH options)."""
        ConfluentAdapter._check_distribution_drift(
            "t",
            expected=None,
            existing={"algorithm": "HASH", "buckets": 6, "columns": ["id"]},
        )

    def test_expected_set_existing_none_drift(self):
        with pytest.raises(CompilationError, match="(?i)distribution drift detected"):
            ConfluentAdapter._check_distribution_drift(
                "t",
                expected={"columns": ["id"], "buckets": 4},
                existing=None,
            )

    def test_column_drift_detected(self):
        with pytest.raises(CompilationError, match="(?i)distribution drift detected"):
            ConfluentAdapter._check_distribution_drift(
                "t",
                expected={"columns": ["id"], "buckets": 4},
                existing={"algorithm": "HASH", "buckets": 4, "columns": ["other"]},
            )

    def test_column_order_drift_detected(self):
        """HASH(a, b) and HASH(b, a) partition differently — order matters."""
        with pytest.raises(CompilationError, match="(?i)distribution drift detected"):
            ConfluentAdapter._check_distribution_drift(
                "t",
                expected={"columns": ["a", "b"]},
                existing={"algorithm": "HASH", "buckets": 4, "columns": ["b", "a"]},
            )

    def test_bucket_drift_detected(self):
        with pytest.raises(CompilationError, match="(?i)distribution drift detected"):
            ConfluentAdapter._check_distribution_drift(
                "t",
                expected={"columns": ["id"], "buckets": 4},
                existing={"algorithm": "HASH", "buckets": 6, "columns": ["id"]},
            )

    def test_buckets_unset_means_unchecked(self):
        """When the user omits `buckets`, Confluent's default is left untouched."""
        ConfluentAdapter._check_distribution_drift(
            "t",
            expected={"columns": ["id"]},
            existing={"algorithm": "HASH", "buckets": 6, "columns": ["id"]},
        )

    def test_no_drift(self):
        ConfluentAdapter._check_distribution_drift(
            "t",
            expected={"columns": ["id"], "buckets": 4},
            existing={"algorithm": "HASH", "buckets": 4, "columns": ["id"]},
        )


# ---------------------------------------------------------------------------
# _partition_drift_catalog
# ---------------------------------------------------------------------------


def _row(
    *,
    section,
    table_name=None,
    col_name=None,
    data_type=None,
    dist_position=None,
    option_key=None,
    option_value=None,
    is_distributed=None,
    dist_algorithm=None,
    dist_buckets=None,
):
    return (
        section,
        table_name,
        col_name,
        data_type,
        dist_position,
        option_key,
        option_value,
        is_distributed,
        dist_algorithm,
        dist_buckets,
    )


_CATALOG_COLUMNS = [
    "section",
    "table_name",
    "col_name",
    "data_type",
    "dist_position",
    "option_key",
    "option_value",
    "is_distributed",
    "dist_algorithm",
    "dist_buckets",
]

# Pin types so agate's inference doesn't coerce "YES" to a boolean (Confluent
# returns it as a string, and the partitioner compares against the literal "YES").
_CATALOG_TYPES = [
    agate.Text(),  # section
    agate.Text(),  # table_name
    agate.Text(),  # col_name
    agate.Text(),  # data_type
    agate.Number(),  # dist_position
    agate.Text(),  # option_key
    agate.Text(),  # option_value
    agate.Text(),  # is_distributed
    agate.Text(),  # dist_algorithm
    agate.Number(),  # dist_buckets
]


def _make_catalog(rows):
    return agate.Table(rows, column_names=_CATALOG_COLUMNS, column_types=_CATALOG_TYPES)


class TestPartitionDriftCatalog:
    def test_splits_columns_by_table_name(self):
        catalog = _make_catalog(
            [
                _row(section="COLUMNS", table_name="existing", col_name="id", data_type="BIGINT"),
                _row(
                    section="COLUMNS", table_name="temp", col_name="id", data_type="BIGINT"
                ),
                _row(
                    section="COLUMNS",
                    table_name="temp",
                    col_name="extra",
                    data_type="STRING",
                ),
            ]
        )
        existing, expected, options, distribution = (
            ConfluentAdapter._partition_drift_catalog(catalog, "existing", "temp")
        )
        assert existing == {"id": "BIGINT"}
        assert expected == {"id": "BIGINT", "extra": "STRING"}
        assert options == {}
        assert distribution is None

    def test_extracts_distribution_from_tables_and_columns(self):
        catalog = _make_catalog(
            [
                _row(
                    section="COLUMNS",
                    table_name="existing",
                    col_name="a",
                    data_type="INT",
                    dist_position=2,
                ),
                _row(
                    section="COLUMNS",
                    table_name="existing",
                    col_name="b",
                    data_type="INT",
                    dist_position=1,
                ),
                _row(
                    section="COLUMNS",
                    table_name="existing",
                    col_name="c",
                    data_type="INT",
                ),
                _row(
                    section="TABLES",
                    table_name="existing",
                    is_distributed="YES",
                    dist_algorithm="HASH",
                    dist_buckets=4,
                ),
            ]
        )
        _, _, _, distribution = ConfluentAdapter._partition_drift_catalog(
            catalog, "existing", "temp"
        )
        # Ordering by DISTRIBUTION_ORDINAL_POSITION: b (pos=1), then a (pos=2)
        assert distribution == {"algorithm": "HASH", "buckets": 4, "columns": ["b", "a"]}

    def test_no_distribution_when_is_distributed_no(self):
        catalog = _make_catalog(
            [
                _row(
                    section="TABLES",
                    table_name="existing",
                    is_distributed="NO",
                    dist_algorithm=None,
                    dist_buckets=None,
                ),
            ]
        )
        _, _, _, distribution = ConfluentAdapter._partition_drift_catalog(
            catalog, "existing", "temp"
        )
        assert distribution is None

    def test_collects_table_options(self):
        catalog = _make_catalog(
            [
                _row(
                    section="TABLE_OPTIONS",
                    table_name="existing",
                    option_key="changelog.mode",
                    option_value="upsert",
                ),
                _row(
                    section="TABLE_OPTIONS",
                    table_name="existing",
                    option_key="connector",
                    option_value="faker",
                ),
            ]
        )
        _, _, options, _ = ConfluentAdapter._partition_drift_catalog(
            catalog, "existing", "temp"
        )
        assert options == {"changelog.mode": "upsert", "connector": "faker"}
