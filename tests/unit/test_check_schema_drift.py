"""Unit tests for schema drift detection logic in ConfluentAdapter.

The public `check_schema_drift` is a thin orchestrator over four helpers:
- `_partition_drift_catalog` splits the unified UNION ALL agate.Table into
  per-concern dicts.
- `_check_column_drift`, `_check_options_drift`, `_check_distribution_drift`
  return a list of one-line violation strings (empty list = no drift). The
  orchestrator collects them all and raises a single CompilationError so the
  user sees every drift in one run instead of fixing them one at a time.

We test the helpers directly rather than fabricating a unified catalog for
every case — the orchestrator is small enough that a couple of partition
tests cover its glue, while the per-concern logic gets exhaustive coverage.
"""

import agate
import pytest
from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.confluent.impl import ConfluentAdapter, ConfluentRelation

# ---------------------------------------------------------------------------
# _check_column_drift
# ---------------------------------------------------------------------------


class TestCheckColumnDrift:
    def test_no_drift(self):
        existing = {"id": "BIGINT", "value": "STRING"}
        expected = {"id": "BIGINT", "value": "STRING"}
        assert ConfluentAdapter._check_column_drift(existing, expected) == []

    def test_extra_column_detected(self):
        existing = {"id": "BIGINT"}
        expected = {"id": "BIGINT", "extra": "STRING"}
        assert ConfluentAdapter._check_column_drift(existing, expected) == [
            "column added: 'extra'"
        ]

    def test_removed_column_detected(self):
        existing = {"id": "BIGINT", "value": "STRING"}
        expected = {"id": "BIGINT"}
        assert ConfluentAdapter._check_column_drift(existing, expected) == [
            "column removed: 'value'"
        ]

    def test_renamed_column_detected(self):
        """Rename surfaces as one removal + one addition."""
        existing = {"id": "BIGINT", "value": "STRING"}
        expected = {"id": "BIGINT", "name": "STRING"}
        assert ConfluentAdapter._check_column_drift(existing, expected) == [
            "column added: 'name'",
            "column removed: 'value'",
        ]

    def test_type_change_detected(self):
        existing = {"id": "BIGINT"}
        expected = {"id": "INT"}
        violations = ConfluentAdapter._check_column_drift(existing, expected)
        assert violations == ["column type: 'id' existing='BIGINT', expected='INT'"]

    def test_collects_all_type_mismatches(self):
        """Multiple type changes are reported together, not just the first."""
        existing = {"a": "BIGINT", "b": "STRING", "c": "DECIMAL(10,2)"}
        expected = {"a": "INT", "b": "STRING", "c": "DECIMAL(10,4)"}
        violations = ConfluentAdapter._check_column_drift(existing, expected)
        assert violations == [
            "column type: 'a' existing='BIGINT', expected='INT'",
            "column type: 'c' existing='DECIMAL(10,2)', expected='DECIMAL(10,4)'",
        ]

    def test_column_order_ignored(self):
        existing = {"a": "BIGINT", "b": "STRING"}
        expected = {"b": "STRING", "a": "BIGINT"}
        assert ConfluentAdapter._check_column_drift(existing, expected) == []

    def test_case_sensitive_names(self):
        """Flink allows distinct columns differing only by case (when backtick-quoted).
        Both sides come from INFORMATION_SCHEMA which preserves declared casing,
        so a case difference is real drift."""
        existing = {"ID": "BIGINT"}
        expected = {"id": "BIGINT"}
        violations = ConfluentAdapter._check_column_drift(existing, expected)
        assert violations == ["column added: 'id'", "column removed: 'ID'"]


# ---------------------------------------------------------------------------
# _check_options_drift
# ---------------------------------------------------------------------------


class TestCheckOptionsDrift:
    def test_options_drift_detected(self):
        violations = ConfluentAdapter._check_options_drift(
            expected_with={"changelog.mode": "append"},
            existing_options={"changelog.mode": "upsert"},
        )
        assert violations == ["option: 'changelog.mode' existing='upsert', expected='append'"]

    def test_options_missing_detected(self):
        violations = ConfluentAdapter._check_options_drift(
            expected_with={"changelog.mode": "append"},
            existing_options={},
        )
        assert violations == ["option: 'changelog.mode' existing='<not set>', expected='append'"]

    def test_extra_existing_options_allowed(self):
        """Extra options in the existing table (e.g. connector defaults) are fine."""
        assert (
            ConfluentAdapter._check_options_drift(
                expected_with={"changelog.mode": "upsert"},
                existing_options={"changelog.mode": "upsert", "connector": "faker"},
            )
            == []
        )

    def test_no_options_check_when_empty(self):
        assert (
            ConfluentAdapter._check_options_drift(
                expected_with={},
                existing_options={"anything": "here"},
            )
            == []
        )

    def test_options_non_string_value_coerced(self):
        """Config values (int, bool) are coerced to str before comparing with I_S strings."""
        # No drift: int 1 should match string "1"
        assert (
            ConfluentAdapter._check_options_drift(
                expected_with={"rows-per-second": 1},
                existing_options={"rows-per-second": "1"},
            )
            == []
        )

    def test_empty_string_existing_not_misreported(self):
        """An empty-string existing value must be shown as '' in the violation,
        not as <not set> (which would falsely imply the option is missing)."""
        violations = ConfluentAdapter._check_options_drift(
            expected_with={"changelog.mode": "append"},
            existing_options={"changelog.mode": ""},
        )
        assert violations == ["option: 'changelog.mode' existing='', expected='append'"]

    def test_collects_all_drifted_options(self):
        """Multiple drifted options are reported together."""
        violations = ConfluentAdapter._check_options_drift(
            expected_with={"changelog.mode": "append", "scan.startup.mode": "earliest"},
            existing_options={"changelog.mode": "upsert", "scan.startup.mode": "latest"},
        )
        assert sorted(violations) == sorted(
            [
                "option: 'changelog.mode' existing='upsert', expected='append'",
                "option: 'scan.startup.mode' existing='latest', expected='earliest'",
            ]
        )


# ---------------------------------------------------------------------------
# _check_distribution_drift
# ---------------------------------------------------------------------------


class TestCheckDistributionDrift:
    def test_unset_expected_skips_check(self):
        """Confluent assigns a default distribution to most tables, so we only
        verify what the user explicitly requested (mirrors WITH options)."""
        assert (
            ConfluentAdapter._check_distribution_drift(
                expected=None,
                existing={"buckets": 6, "columns": ["id"]},
            )
            == []
        )

    def test_expected_set_existing_none_drift(self):
        violations = ConfluentAdapter._check_distribution_drift(
            expected={"columns": ["id"], "buckets": 4},
            existing=None,
        )
        assert violations == [
            "distribution: existing=<none>, expected={'columns': ['id'], 'buckets': 4}"
        ]

    def test_column_drift_detected(self):
        violations = ConfluentAdapter._check_distribution_drift(
            expected={"columns": ["id"], "buckets": 4},
            existing={"buckets": 4, "columns": ["other"]},
        )
        assert violations == ["distribution columns: existing=['other'], expected=['id']"]

    def test_column_order_drift_detected(self):
        """HASH(a, b) and HASH(b, a) partition differently — order matters."""
        violations = ConfluentAdapter._check_distribution_drift(
            expected={"columns": ["a", "b"]},
            existing={"buckets": 4, "columns": ["b", "a"]},
        )
        assert violations == ["distribution columns: existing=['b', 'a'], expected=['a', 'b']"]

    def test_bucket_drift_detected(self):
        violations = ConfluentAdapter._check_distribution_drift(
            expected={"columns": ["id"], "buckets": 4},
            existing={"buckets": 6, "columns": ["id"]},
        )
        assert violations == ["distribution buckets: existing=6, expected=4"]

    def test_buckets_unset_means_unchecked(self):
        """When the user omits `buckets`, Confluent's default is left untouched."""
        assert (
            ConfluentAdapter._check_distribution_drift(
                expected={"columns": ["id"]},
                existing={"buckets": 6, "columns": ["id"]},
            )
            == []
        )

    def test_column_and_bucket_drift_collected_together(self):
        violations = ConfluentAdapter._check_distribution_drift(
            expected={"columns": ["a"], "buckets": 4},
            existing={"buckets": 8, "columns": ["b"]},
        )
        assert violations == [
            "distribution columns: existing=['b'], expected=['a']",
            "distribution buckets: existing=8, expected=4",
        ]

    def test_no_drift(self):
        assert (
            ConfluentAdapter._check_distribution_drift(
                expected={"columns": ["id"], "buckets": 4},
                existing={"buckets": 4, "columns": ["id"]},
            )
            == []
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
    agate.Number(),  # dist_buckets
]


def _make_catalog(rows):
    return agate.Table(rows, column_names=_CATALOG_COLUMNS, column_types=_CATALOG_TYPES)


class TestPartitionDriftCatalog:
    def test_splits_columns_by_table_name(self):
        catalog = _make_catalog(
            [
                _row(section="COLUMNS", table_name="existing", col_name="id", data_type="BIGINT"),
                _row(section="COLUMNS", table_name="temp", col_name="id", data_type="BIGINT"),
                _row(
                    section="COLUMNS",
                    table_name="temp",
                    col_name="extra",
                    data_type="STRING",
                ),
            ]
        )
        existing, expected, options, distribution = ConfluentAdapter._partition_drift_catalog(
            catalog, "existing", "temp"
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
                    dist_buckets=4,
                ),
            ]
        )
        _, _, _, distribution = ConfluentAdapter._partition_drift_catalog(
            catalog, "existing", "temp"
        )
        # Ordering by DISTRIBUTION_ORDINAL_POSITION: b (pos=1), then a (pos=2)
        assert distribution == {"buckets": 4, "columns": ["b", "a"]}

    def test_no_distribution_when_is_distributed_no(self):
        catalog = _make_catalog(
            [
                _row(
                    section="TABLES",
                    table_name="existing",
                    is_distributed="NO",
                    dist_buckets=None,
                ),
            ]
        )
        _, _, _, distribution = ConfluentAdapter._partition_drift_catalog(
            catalog, "existing", "temp"
        )
        assert distribution is None

    def test_is_distributed_case_insensitive(self):
        """Defensive: confluent-sql may someday return 'yes' / 'Yes' / True
        instead of the canonical 'YES'.  Comparison must not silently miss it."""
        catalog = _make_catalog(
            [
                _row(
                    section="COLUMNS",
                    table_name="existing",
                    col_name="id",
                    data_type="BIGINT",
                    dist_position=1,
                ),
                _row(
                    section="TABLES",
                    table_name="existing",
                    is_distributed="yes",
                    dist_buckets=4,
                ),
            ]
        )
        _, _, _, distribution = ConfluentAdapter._partition_drift_catalog(
            catalog, "existing", "temp"
        )
        assert distribution == {"buckets": 4, "columns": ["id"]}

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
        _, _, options, _ = ConfluentAdapter._partition_drift_catalog(catalog, "existing", "temp")
        assert options == {"changelog.mode": "upsert", "connector": "faker"}


# ---------------------------------------------------------------------------
# check_schema_drift (orchestrator)
# ---------------------------------------------------------------------------


def _relation(identifier):
    return ConfluentRelation.create(
        database="env-1", schema="cluster-a", identifier=identifier, type="table"
    )


class TestCheckSchemaDriftOrchestrator:
    """Smoke tests for the public orchestrator. The per-concern helpers are
    exhaustively tested above; here we confirm only that the orchestrator
    accepts Relation objects, partitions the catalog, collects violations
    from every helper, and raises one error containing all of them."""

    # Catalog rows reference tables by their `INFORMATION_SCHEMA` identifier,
    # so the catalog labels must match the relations' identifiers exactly —
    # otherwise the COLUMNS rows go unrouted and column drift silently
    # disappears. We use "my_table" / "tmp_my_table" consistently below.
    EXISTING_ID = "my_table"
    TEMP_ID = "tmp_my_table"

    def test_collects_violations_from_every_concern(self):
        """When column + options + distribution are all drifted, the single
        raised error must mention every category — no fail-fast."""
        catalog = _make_catalog(
            [
                _row(
                    section="COLUMNS",
                    table_name=self.EXISTING_ID,
                    col_name="id",
                    data_type="BIGINT",
                    dist_position=1,
                ),
                _row(
                    section="TABLES",
                    table_name=self.EXISTING_ID,
                    is_distributed="YES",
                    dist_buckets=4,
                ),
                _row(
                    section="TABLE_OPTIONS",
                    table_name=self.EXISTING_ID,
                    option_key="changelog.mode",
                    option_value="upsert",
                ),
                _row(
                    section="COLUMNS",
                    table_name=self.TEMP_ID,
                    col_name="id",
                    data_type="BIGINT",
                ),
                _row(
                    section="COLUMNS",
                    table_name=self.TEMP_ID,
                    col_name="extra",
                    data_type="STRING",
                ),
            ]
        )
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)  # bypass __init__
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                catalog,
                expected_with={"changelog.mode": "append"},
                expected_distribution={"columns": ["other"], "buckets": 8},
            )
        msg = str(excinfo.value)
        assert "Schema drift detected for" in msg
        assert self.EXISTING_ID in msg
        assert "column added: 'extra'" in msg
        assert "option: 'changelog.mode'" in msg
        assert "distribution columns:" in msg
        assert "distribution buckets:" in msg
        assert "Use --full-refresh" in msg

    def test_options_only_drift(self):
        """When only options drift, only options-related violations appear."""
        catalog = _make_catalog(
            [
                _row(
                    section="COLUMNS",
                    table_name=self.EXISTING_ID,
                    col_name="id",
                    data_type="BIGINT",
                ),
                _row(
                    section="COLUMNS",
                    table_name=self.TEMP_ID,
                    col_name="id",
                    data_type="BIGINT",
                ),
                _row(
                    section="TABLE_OPTIONS",
                    table_name=self.EXISTING_ID,
                    option_key="changelog.mode",
                    option_value="upsert",
                ),
            ]
        )
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                catalog,
                expected_with={"changelog.mode": "append"},
            )
        msg = str(excinfo.value)
        assert "option: 'changelog.mode'" in msg
        # No "column" or "distribution" lines should appear in the violation list
        violation_section = msg.split("Schema drift detected for", 1)[1]
        assert "column" not in violation_section.lower()
        assert "distribution" not in violation_section.lower()

    def test_no_drift_returns_silently(self):
        """When nothing has drifted the orchestrator returns without raising."""
        catalog = _make_catalog(
            [
                _row(
                    section="COLUMNS",
                    table_name=self.EXISTING_ID,
                    col_name="id",
                    data_type="BIGINT",
                ),
                _row(
                    section="COLUMNS",
                    table_name=self.TEMP_ID,
                    col_name="id",
                    data_type="BIGINT",
                ),
            ]
        )
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        adapter.check_schema_drift(
            _relation(self.EXISTING_ID),
            _relation(self.TEMP_ID),
            catalog,
            expected_with={},
        )

    def test_empty_expected_columns_raises_distinct_error(self):
        """An empty expected_columns must NOT masquerade as a column-list drift.

        The drift-check temp table coming back with no columns from
        INFORMATION_SCHEMA is almost always a transient Confluent Cloud
        metadata propagation lag, so we surface it as a retriable
        DbtDatabaseError with a distinct, diagnosable message.
        """
        catalog = _make_catalog(
            [
                _row(
                    section="COLUMNS",
                    table_name=self.EXISTING_ID,
                    col_name="id",
                    data_type="BIGINT",
                ),
                _row(
                    section="COLUMNS",
                    table_name=self.EXISTING_ID,
                    col_name="value",
                    data_type="STRING",
                ),
            ]
        )
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(DbtDatabaseError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                catalog,
                expected_with={},
            )
        msg = str(excinfo.value)
        assert "INFORMATION_SCHEMA" in msg
        # Must not look like a regular column-list drift error.
        assert "drift detected" not in msg.lower()
