"""Unit tests for schema drift detection logic in ConfluentAdapter.

The public `check_schema_drift` is a thin orchestrator over four helpers, and
takes the metadata already partitioned:
- `_partition_drift_metadata` (behind `partition_drift_metadata`, which
  check_for_schema_drift calls once) and `_partition_drift_columns` split the
  metadata and columns queries' agate.Tables into per-concern structures.
- `_check_column_drift`, `_check_options_drift`, `_check_distribution_drift`
  return a list of one-line violation strings (empty list = no drift). The
  orchestrator collects them all and raises a single CompilationError so the
  user sees every drift in one run instead of fixing them one at a time.

We test the helpers directly rather than fabricating query results for
every case — the orchestrator is small enough that a couple of partition
tests cover its glue, while the per-concern logic gets exhaustive coverage.
"""

import pytest
from confluent_sql.types import ColumnTypeDefinition
from dbt_common.exceptions import CompilationError, DbtDatabaseError, DbtRuntimeError

from dbt.adapters.confluent.impl import ConfluentAdapter, DriftMetadata, DryRunColumns
from tests.unit._helpers import drift_metadata_row as _row
from tests.unit._helpers import make_drift_columns as _make_columns
from tests.unit._helpers import make_drift_metadata as _make_metadata
from tests.unit._helpers import partitioned_drift_metadata as _partitioned
from tests.unit._helpers import relation as _relation

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

    def test_display_spells_type_values(self):
        """The dry-run path compares non-string types and spells them with display_type."""
        violations = ConfluentAdapter._check_column_drift(
            {"a": 1}, {"a": 2}, display=lambda value: f"<{value}>"
        )
        assert violations == ["column type: 'a' existing='<1>', expected='<2>'"]

    def test_fit_decides_and_spells_type_changes(self):
        """`fit` decides each shared column: the result is compared with the existing value, and
        the message shows the result."""
        violations = ConfluentAdapter._check_column_drift(
            {"a": 1, "b": 2},
            {"a": 3, "b": 5},
            fit=lambda existing, expected: (
                existing if existing % 2 == expected % 2 else expected * 10
            ),
        )
        assert violations == ["column type: 'b' existing='2', expected='50'"]


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
# _partition_drift_metadata / _partition_drift_columns
# ---------------------------------------------------------------------------


class TestPartitionDriftMetadata:
    def test_tables_row_sets_found(self):
        metadata = _make_metadata(
            [_row(section="TABLES", is_distributed="NO", is_materialized="NO")]
        )
        assert ConfluentAdapter._partition_drift_metadata(metadata) == DriftMetadata(
            found=True, is_materialized=False, options={}, distribution=None
        )

    def test_no_tables_row_is_not_found(self):
        """Metadata lag (or a table dropped outside dbt): TABLE_OPTIONS rows alone don't count
        as finding the table."""
        metadata = _make_metadata(
            [_row(section="TABLE_OPTIONS", option_key="changelog.mode", option_value="upsert")]
        )
        partitioned = ConfluentAdapter._partition_drift_metadata(metadata)
        assert partitioned.found is False
        assert partitioned.options == {"changelog.mode": "upsert"}

    def test_extracts_distribution_from_distribution_rows(self):
        metadata = _make_metadata(
            [
                _row(section="TABLES", is_distributed="YES", dist_buckets=4),
                _row(section="DISTRIBUTION", col_name="a", dist_position=2),
                _row(section="DISTRIBUTION", col_name="b", dist_position=1),
            ]
        )
        distribution = ConfluentAdapter._partition_drift_metadata(metadata).distribution
        # Ordering by DISTRIBUTION_ORDINAL_POSITION: b (pos=1), then a (pos=2)
        assert distribution == {"buckets": 4, "columns": ["b", "a"]}

    def test_distributed_without_distribution_rows_has_no_columns(self):
        """A keyless table's default `DISTRIBUTED INTO 6 BUCKETS` has no distribution columns
        (probe e3cbfffb), and the macro skips the DISTRIBUTION section when distributed_by isn't
        checked. Both read as no columns."""
        metadata = _make_metadata([_row(section="TABLES", is_distributed="YES", dist_buckets=6)])
        distribution = ConfluentAdapter._partition_drift_metadata(metadata).distribution
        assert distribution == {"buckets": 6, "columns": []}

    def test_no_distribution_when_is_distributed_no(self):
        metadata = _make_metadata([_row(section="TABLES", is_distributed="NO", dist_buckets=None)])
        assert ConfluentAdapter._partition_drift_metadata(metadata).distribution is None

    def test_is_distributed_case_insensitive(self):
        """Defensive: confluent-sql may someday return 'yes' / 'Yes' / True
        instead of the canonical 'YES'.  Comparison must not silently miss it."""
        metadata = _make_metadata(
            [
                _row(section="TABLES", is_distributed="yes", dist_buckets=4),
                _row(section="DISTRIBUTION", col_name="id", dist_position=1),
            ]
        )
        distribution = ConfluentAdapter._partition_drift_metadata(metadata).distribution
        assert distribution == {"buckets": 4, "columns": ["id"]}

    def test_collects_table_options(self):
        metadata = _make_metadata(
            [
                _row(section="TABLES", is_distributed="NO"),
                _row(section="TABLE_OPTIONS", option_key="changelog.mode", option_value="upsert"),
                _row(section="TABLE_OPTIONS", option_key="connector", option_value="faker"),
            ]
        )
        options = ConfluentAdapter._partition_drift_metadata(metadata).options
        assert options == {"changelog.mode": "upsert", "connector": "faker"}

    def test_is_materialized_detected(self):
        """IS_MATERIALIZED='YES' in the TABLES section flags a materialized
        table; the default distribution flags ('NO') must not mask it."""
        metadata = _make_metadata(
            [_row(section="TABLES", is_distributed="NO", is_materialized="YES")]
        )
        assert ConfluentAdapter._partition_drift_metadata(metadata).is_materialized is True

    def test_is_materialized_false_for_regular_table(self):
        metadata = _make_metadata(
            [_row(section="TABLES", is_distributed="YES", dist_buckets=4, is_materialized="NO")]
        )
        assert ConfluentAdapter._partition_drift_metadata(metadata).is_materialized is False

    def test_available_to_jinja(self):
        """check_for_schema_drift calls partition_drift_metadata from Jinja, and the macro
        harness's mock adapter wouldn't notice it missing @available."""
        assert "partition_drift_metadata" in ConfluentAdapter._available_
        metadata = _make_metadata([_row(section="TABLES", is_distributed="NO")])
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        assert adapter.partition_drift_metadata(metadata) == (
            ConfluentAdapter._partition_drift_metadata(metadata)
        )


class TestPartitionDriftColumns:
    def test_splits_columns_by_table_name(self):
        columns = _make_columns(
            [
                ("existing", "id", "BIGINT"),
                ("temp", "id", "BIGINT"),
                ("temp", "extra", "STRING"),
            ]
        )
        existing, expected = ConfluentAdapter._partition_drift_columns(columns, "existing", "temp")
        assert existing == {"id": "BIGINT"}
        assert expected == {"id": "BIGINT", "extra": "STRING"}

    def test_skips_and_warns_on_an_unexpected_table(self, mocker):
        warning = mocker.patch("dbt.adapters.confluent.impl.logger").warning
        columns = _make_columns(
            [
                ("existing", "id", "BIGINT"),
                ("other", "x", "INT"),
                ("temp", "id", "BIGINT"),
            ]
        )
        existing, expected = ConfluentAdapter._partition_drift_columns(columns, "existing", "temp")
        assert existing == {"id": "BIGINT"}
        assert expected == {"id": "BIGINT"}
        warning.assert_called_once()
        assert "other" in warning.call_args.args[0]


# ---------------------------------------------------------------------------
# check_schema_drift (orchestrator)
# ---------------------------------------------------------------------------


def _type(data: dict) -> ColumnTypeDefinition:
    """A driver ColumnTypeDefinition, parsed from dry-run-shaped JSON."""
    return ColumnTypeDefinition.from_response(data)


BIGINT = _type({"type": "BIGINT", "nullable": True})
STRING = _type({"type": "VARCHAR", "nullable": True, "length": 2147483647})
DECIMAL_10_2 = _type({"type": "DECIMAL", "nullable": True, "precision": 10, "scale": 2})


class TestCheckSchemaDriftOrchestrator:
    """Smoke tests for the public orchestrator. The per-concern helpers are
    exhaustively tested above; here we confirm only that the orchestrator
    accepts Relation objects, partitions the query results, collects
    violations from every helper, and raises one error containing all of
    them."""

    # get_drift_columns rows reference tables by their `INFORMATION_SCHEMA`
    # identifier, so the labels must match the relations' identifiers exactly —
    # otherwise the rows go unrouted and column drift silently disappears. We
    # use "my_table" / "tmp_my_table" consistently below.
    EXISTING_ID = "my_table"
    TEMP_ID = "tmp_my_table"

    def test_collects_violations_from_every_concern(self):
        """When column + options + distribution are all drifted, the single
        raised error must mention every category — no fail-fast."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)  # bypass __init__
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                self._metadata(
                    _row(
                        section="TABLE_OPTIONS",
                        option_key="changelog.mode",
                        option_value="upsert",
                    ),
                    _row(section="DISTRIBUTION", col_name="id", dist_position=1),
                    is_distributed="YES",
                    dist_buckets=4,
                ),
                expected_with={"changelog.mode": "append"},
                expected_distribution={"columns": ["other"], "buckets": 8},
                drift_columns=self._columns({"id": "BIGINT"}, {"id": "BIGINT", "extra": "STRING"}),
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
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                self._metadata(
                    _row(
                        section="TABLE_OPTIONS", option_key="changelog.mode", option_value="upsert"
                    )
                ),
                expected_with={"changelog.mode": "append"},
                drift_columns=self._columns({"id": "BIGINT"}, {"id": "BIGINT"}),
            )
        msg = str(excinfo.value)
        assert "option: 'changelog.mode'" in msg
        # No "column" or "distribution" lines should appear in the violation list
        violation_section = msg.split("Schema drift detected for", 1)[1]
        assert "column" not in violation_section.lower()
        assert "distribution" not in violation_section.lower()

    def test_connector_change_detected(self):
        """streaming_source's `connector` config is rendered into the DDL's
        WITH clause but lives outside the `with` config — the orchestrator
        must merge it into the expected options so changing it is caught."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                self._metadata(
                    _row(section="TABLE_OPTIONS", option_key="connector", option_value="faker")
                ),
                expected_with={},
                expected_connector="datagen",
                drift_columns=self._columns({"id": "BIGINT"}, {"id": "BIGINT"}),
            )
        msg = str(excinfo.value)
        assert "option: 'connector' existing='faker', expected='datagen'" in msg

    def test_matching_connector_passes(self):
        """A connector that matches the existing table's option is no drift."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        adapter.check_schema_drift(
            _relation(self.EXISTING_ID),
            _relation(self.TEMP_ID),
            self._metadata(
                _row(section="TABLE_OPTIONS", option_key="connector", option_value="faker")
            ),
            expected_with={},
            expected_connector="faker",
            drift_columns=self._columns({"id": "BIGINT"}, {"id": "BIGINT"}),
        )

    def test_no_drift_returns_silently(self):
        """When nothing has drifted the orchestrator returns without raising."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        adapter.check_schema_drift(
            _relation(self.EXISTING_ID),
            _relation(self.TEMP_ID),
            self._metadata(),
            expected_with={},
            drift_columns=self._columns({"id": "BIGINT"}, {"id": "BIGINT"}),
        )

    def test_materialized_table_raises_dedicated_error(self):
        """A reverse materialization switch (the model's name is held by a
        Flink materialized table) must raise its own error, not a drift list —
        the MT can't be managed by drop-and-recreate at all, so per-concern
        guidance would be noise. It takes precedence over other violations."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                self._metadata(is_materialized="YES"),
                expected_with={},
                # A drifted column that must NOT surface: the MT error wins.
                drift_columns=self._columns({"id": "BIGINT"}, {"renamed": "BIGINT"}),
            )
        msg = str(excinfo.value)
        assert "materialized table" in msg
        assert "materialized='materialized_table'" in msg
        assert "--full-refresh" in msg
        assert "Schema drift detected" not in msg

    def test_materialized_table_raises_even_under_enforce_columns(self):
        """enforce='columns' (the streaming restart path under
        on_schema_drift='ignore') must still reject a materialized table:
        the restart would submit an INSERT against it."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                self._metadata(is_materialized="YES"),
                expected_with={},
                drift_columns=self._columns({"id": "BIGINT"}, {"id": "BIGINT"}),
                enforce="columns",
            )
        assert "materialized table" in str(excinfo.value)

    def test_distribution_added_drift_through_orchestrator(self):
        """When the existing table is not distributed (IS_DISTRIBUTED='NO') but
        the model requests `distributed_by`, the orchestrator surfaces the
        'distribution: existing=<none>' violation.

        This is the one distribution-violation string that does not start with
        'distribution columns:'/'distribution buckets:', and the only path that
        drives `_partition_drift_metadata` to return `distribution=None`
        while `expected` is set — so we exercise it end-to-end (partition →
        orchestrator) rather than only at the `_check_distribution_drift` helper.
        """
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                # IS_DISTRIBUTED='NO' → partition yields distribution=None
                self._metadata(),
                expected_with={},
                expected_distribution={"columns": ["id"], "buckets": 4},
                drift_columns=self._columns({"id": "BIGINT"}, {"id": "BIGINT"}),
            )
        msg = str(excinfo.value)
        assert "distribution: existing=<none>" in msg
        # Columns and options match, so distribution is the only bullet.
        bullets = [line for line in msg.splitlines() if line.strip().startswith("- ")]
        assert len(bullets) == 1

    def test_empty_expected_columns_raises_distinct_error(self):
        """An empty expected_columns must NOT masquerade as a column-list drift.

        The drift-check temp table coming back with no columns from
        INFORMATION_SCHEMA is almost always a transient Confluent Cloud
        metadata propagation lag, so we surface it as a retriable
        DbtDatabaseError with a distinct, diagnosable message.
        """
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(DbtDatabaseError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                self._metadata(),
                expected_with={},
                drift_columns=self._columns({"id": "BIGINT", "value": "STRING"}, {}),
            )
        msg = str(excinfo.value)
        assert "expected schema" in msg
        assert "INFORMATION_SCHEMA" in msg
        # Must not look like a regular column-list drift error.
        assert "drift detected" not in msg.lower()

    def test_empty_existing_columns_raises_distinct_error(self):
        """An empty existing_columns must NOT masquerade as a column-list drift.

        The drift check only runs when dbt's cache says the relation exists,
        so the existing table coming back with zero COLUMNS rows is the same
        metadata propagation lag as the temp-table case (or an external drop
        mid-run) — not "every column was added". It must surface as a
        retriable DbtDatabaseError, not a CompilationError advising a
        destructive --full-refresh.
        """
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(DbtDatabaseError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                self._metadata(),
                expected_with={},
                drift_columns=self._columns({}, {"id": "BIGINT", "value": "STRING"}),
            )
        msg = str(excinfo.value)
        assert "existing schema" in msg
        assert "INFORMATION_SCHEMA" in msg
        # Must not look like a regular column-list drift error.
        assert "drift detected" not in msg.lower()

    def test_enforce_columns_ignores_options_and_distribution_drift(self):
        """With enforce='columns', only column violations should raise."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        # Options and distribution differ, but enforce='columns' suppresses
        # those; columns match so no error is raised.
        adapter.check_schema_drift(
            _relation(self.EXISTING_ID),
            _relation(self.TEMP_ID),
            self._metadata(
                _row(section="TABLE_OPTIONS", option_key="changelog.mode", option_value="upsert"),
                _row(section="DISTRIBUTION", col_name="id", dist_position=1),
                is_distributed="YES",
                dist_buckets=4,
            ),
            expected_with={"changelog.mode": "append"},
            expected_distribution={"columns": ["other"], "buckets": 8},
            drift_columns=self._columns({"id": "BIGINT"}, {"id": "BIGINT"}),
            enforce="columns",
        )

    def test_enforce_columns_still_raises_on_column_drift(self):
        """enforce='columns' must still surface real column drift."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                self._metadata(),
                expected_with={},
                drift_columns=self._columns({"id": "BIGINT"}, {"id": "BIGINT", "extra": "STRING"}),
                enforce="columns",
            )
        assert "column added: 'extra'" in str(excinfo.value)

    @staticmethod
    def _metadata(*rows, **tables_row):
        """The partitioned metadata for a TABLES row for the existing table (not distributed and
        not materialized, unless tables_row says otherwise), then `rows`."""
        tables = {"is_distributed": "NO", "is_materialized": "NO", **tables_row}
        return _partitioned([_row(section="TABLES", **tables), *rows])

    def _columns(self, existing, temp):
        """`_get_drift_columns`' result, from {column_name: data_type} maps for the existing and
        temp tables."""
        return _make_columns(
            [(self.EXISTING_ID, name, data_type) for name, data_type in existing.items()]
            + [(self.TEMP_ID, name, data_type) for name, data_type in temp.items()]
        )

    def test_dry_run_columns_replace_temp_rows(self):
        """Dry-run path: both column maps arrive resolved, the metadata holds only the existing
        table, and drift is still detected."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                None,
                self._metadata(),
                expected_with={},
                dry_run_columns=DryRunColumns(
                    existing={"id": BIGINT}, expected={"id": BIGINT, "extra": STRING}
                ),
            )
        assert "column added: 'extra'" in str(excinfo.value)

    def test_dry_run_columns_no_drift_returns_silently(self):
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        adapter.check_schema_drift(
            _relation(self.EXISTING_ID),
            None,
            self._metadata(),
            expected_with={},
            dry_run_columns=DryRunColumns(
                existing={"price": DECIMAL_10_2}, expected={"price": DECIMAL_10_2}
            ),
        )

    def test_dry_run_columns_with_enforce_columns(self):
        """The streaming restart path under on_schema_drift='ignore' dry-runs too:
        options/distribution drift is ignored, column drift still raises."""
        metadata = self._metadata(
            _row(section="TABLE_OPTIONS", option_key="changelog.mode", option_value="upsert"),
        )
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        adapter.check_schema_drift(
            _relation(self.EXISTING_ID),
            None,
            metadata,
            expected_with={"changelog.mode": "append"},
            enforce="columns",
            dry_run_columns=DryRunColumns(existing={"id": BIGINT}, expected={"id": BIGINT}),
        )
        with pytest.raises(CompilationError, match="column added: 'extra'"):
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                None,
                metadata,
                expected_with={"changelog.mode": "append"},
                enforce="columns",
                dry_run_columns=DryRunColumns(
                    existing={"id": BIGINT}, expected={"id": BIGINT, "extra": BIGINT}
                ),
            )

    def test_dry_run_columns_missing_tables_row_raises(self):
        """The TABLES-row guard runs on the dry-run path too: without it, options and
        distribution would read as empty and be reported as drift."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(DbtDatabaseError, match="could not find") as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                None,
                _partitioned([]),
                expected_with={},
                dry_run_columns=DryRunColumns(existing={"id": BIGINT}, expected={"id": BIGINT}),
            )
        assert "INFORMATION_SCHEMA.TABLES" in str(excinfo.value)

    def test_empty_expected_columns_is_a_bug_not_lag(self):
        """The resolver returns None (fall back), never an empty expected map, which must
        not reach the temp-table guard's "metadata propagation lag, retry" advice."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(DbtRuntimeError, match="no expected columns"):
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                None,
                _partitioned([]),
                expected_with={},
                dry_run_columns=DryRunColumns(existing={"id": BIGINT}, expected={}),
            )

    @pytest.mark.parametrize("both", [True, False])
    def test_requires_exactly_one_expected_source(self, both):
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(DbtRuntimeError, match="exactly one") as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID) if both else None,
                _partitioned([]),
                expected_with={},
                dry_run_columns=(
                    DryRunColumns(existing={"id": BIGINT}, expected={"id": BIGINT})
                    if both
                    else None
                ),
            )
        assert "dbt-confluent" in str(excinfo.value)

    @pytest.mark.parametrize("path", ["temp_without_columns", "dry_run_with_columns"])
    def test_drift_columns_pair_with_temp_relation(self, path):
        """drift_columns is the fallback's COLUMNS read: required with temp_relation, and a bug
        without it."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        if path == "temp_without_columns":
            arguments = {"temp_relation": _relation(self.TEMP_ID)}
        else:
            arguments = {
                "temp_relation": None,
                "dry_run_columns": DryRunColumns(existing={"id": BIGINT}, expected={"id": BIGINT}),
                "drift_columns": self._columns({"id": "BIGINT"}, {}),
            }
        with pytest.raises(DbtRuntimeError, match="drift_columns") as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                drift_metadata=self._metadata(),
                expected_with={},
                **arguments,
            )
        assert "dbt-confluent bug" in str(excinfo.value)

    def test_missing_tables_row_raises_on_fallback(self):
        """No TABLES row on the fallback path must not read as "no options, no distribution"
        and report drift against them: it's the retriable lag error."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(DbtDatabaseError, match="could not find") as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                _partitioned([]),
                expected_with={"changelog.mode": "append"},
                expected_distribution={"columns": ["id"], "buckets": 4},
                drift_columns=self._columns({"id": "BIGINT"}, {"id": "BIGINT"}),
            )
        msg = str(excinfo.value)
        assert "INFORMATION_SCHEMA.TABLES" in msg
        assert "drift detected" not in msg.lower()

    def test_missing_tables_row_checked_before_temp_columns(self):
        """Guard order on the fallback: materialized, TABLES row, temp columns, existing
        columns."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(DbtDatabaseError, match="could not find"):
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                _relation(self.TEMP_ID),
                _partitioned([]),
                expected_with={},
                drift_columns=self._columns({}, {}),
            )

    def test_distributed_table_without_distribution_columns_reports_drift(self):
        """A keyless table's default distribution (IS_DISTRIBUTED='YES', no distribution
        columns) gaining a distributed_by is real drift, not a lag error (design Q3)."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                None,
                self._metadata(is_distributed="YES", dist_buckets=6),
                expected_with={},
                expected_distribution={"columns": ["id"]},
                dry_run_columns=DryRunColumns(existing={"id": BIGINT}, expected={"id": BIGINT}),
            )
        assert "distribution columns: existing=[], expected=['id']" in str(excinfo.value)

    @pytest.mark.parametrize("existing_nullable", [True, False])
    def test_top_level_nullability_is_ignored(self, existing_nullable):
        """A yml not_null column fed by a nullable source column (or the reverse) is no drift,
        as it wasn't when FULL_DATA_TYPE strings were compared."""
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        adapter.check_schema_drift(
            _relation(self.EXISTING_ID),
            None,
            self._metadata(),
            expected_with={},
            dry_run_columns=DryRunColumns(
                existing={"id": _type({"type": "BIGINT", "nullable": not existing_nullable})},
                expected={"id": _type({"type": "BIGINT", "nullable": existing_nullable})},
            ),
        )

    def test_nested_nullability_drifts(self):
        """NOT NULL inside a composite type is part of the stored type; the message spells
        both sides the way FULL_DATA_TYPE does."""
        element = {"type": "VARCHAR", "length": 2147483647}
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                None,
                self._metadata(),
                expected_with={},
                dry_run_columns=DryRunColumns(
                    existing={
                        "tags": _type(
                            {
                                "type": "ARRAY",
                                "nullable": True,
                                "element_type": {**element, "nullable": False},
                            }
                        )
                    },
                    expected={
                        "tags": _type(
                            {
                                "type": "ARRAY",
                                "nullable": True,
                                "element_type": {**element, "nullable": True},
                            }
                        )
                    },
                ),
            )
        assert (
            "column type: 'tags' existing='ARRAY<VARCHAR(2147483647) NOT NULL>', "
            "expected='ARRAY<VARCHAR(2147483647)>'"
        ) in str(excinfo.value)

    def test_map_keys_fit_an_avro_stored_key(self):
        """map['a', 1] has a CHAR(1) key in the model's dry-run, which fits in the
        VARCHAR(2147483647) NOT NULL an Avro table stores: no drift."""
        value = {"type": "INTEGER", "nullable": False}
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        adapter.check_schema_drift(
            _relation(self.EXISTING_ID),
            None,
            self._metadata(),
            expected_with={},
            dry_run_columns=DryRunColumns(
                existing={
                    "m": _type(
                        {
                            "type": "MAP",
                            "nullable": True,
                            "key_type": {
                                "type": "VARCHAR",
                                "nullable": False,
                                "length": 2147483647,
                            },
                            "value_type": value,
                        }
                    )
                },
                expected={
                    "m": _type(
                        {
                            "type": "MAP",
                            "nullable": False,
                            "key_type": {"type": "CHAR", "nullable": False, "length": 1},
                            "value_type": value,
                        }
                    )
                },
            ),
        )

    @staticmethod
    def _protobuf_map(key: dict, value_type: str = "INTEGER") -> ColumnTypeDefinition:
        return _type(
            {
                "type": "MAP",
                "nullable": True,
                "key_type": key,
                "value_type": {"type": value_type, "nullable": True},
            }
        )

    def _check_protobuf_map_key(self, length: int) -> None:
        # A Protobuf table keeps the declared key type, here VARCHAR(5).
        existing = self._protobuf_map({"type": "VARCHAR", "nullable": True, "length": 5})
        expected = self._protobuf_map({"type": "VARCHAR", "nullable": True, "length": length})
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        adapter.check_schema_drift(
            _relation(self.EXISTING_ID),
            None,
            self._metadata(),
            expected_with={},
            dry_run_columns=DryRunColumns(existing={"m": existing}, expected={"m": expected}),
        )

    @pytest.mark.parametrize("length", [5, 3], ids=["same", "shorter"])
    def test_protobuf_map_key_that_fits(self, length):
        """A key that fits in the Protobuf table's declared key type isn't drift."""
        self._check_protobuf_map_key(length)

    def test_protobuf_longer_map_key_drifts(self):
        """A key longer than the Protobuf table's declared key type is drift."""
        with pytest.raises(CompilationError) as excinfo:
            self._check_protobuf_map_key(10)
        assert (
            "column type: 'm' existing='MAP<VARCHAR(5), INT>', expected='MAP<VARCHAR(10), INT>'"
        ) in str(excinfo.value)

    def test_protobuf_map_value_change_keeps_the_tables_key(self):
        """The key fits but the value changed: drift, and the message spells the table's key on
        both sides, so only the value reads differently."""
        existing = self._protobuf_map({"type": "VARCHAR", "nullable": True, "length": 5})
        expected = self._protobuf_map({"type": "CHAR", "nullable": False, "length": 1}, "BIGINT")
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        with pytest.raises(CompilationError) as excinfo:
            adapter.check_schema_drift(
                _relation(self.EXISTING_ID),
                None,
                self._metadata(),
                expected_with={},
                dry_run_columns=DryRunColumns(existing={"m": existing}, expected={"m": expected}),
            )
        assert (
            "column type: 'm' existing='MAP<VARCHAR(5), INT>', expected='MAP<VARCHAR(5), BIGINT>'"
        ) in str(excinfo.value)
