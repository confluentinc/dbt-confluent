"""Functional tests for the `watermark` config (#261).

A watermark's job is to make a column an event-time attribute, which Flink requires for
event-time windows in streaming mode (batch/snapshot mode doesn't, so a `table` model
couldn't tell the difference). So the test is a tumbling-window aggregation, as a
`streaming_table`, over a watermarked materialized_table, read via `ref()`:

- With `watermark` configured, the aggregation plans and emits windows. A window is only
  emitted once the watermark passes its end, so seeing any proves the watermark advances.
- Without it (same source, same MT, same window model -- only the `watermark` config differs),
  Flink rejects the window aggregate because `event_ts` isn't a time attribute. This is what
  shows the positive case works *because of* the watermark.
"""

import pytest

from dbt.tests.util import run_dbt, set_model_file
from tests.functional.adapter._helpers import assert_drift_error, relation, wait_for_snapshot_rows
from tests.functional.adapter.fixtures import ConfluentFixtures
from tests.functional.adapter.isolation import physical_name

ROW_COUNT = 100
EVENT_TIME_COLUMN = "event_ts"
WINDOW_SECONDS = 10
WATERMARK_CONFIG = (
    f"watermark={{'column': '{EVENT_TIME_COLUMN}', "
    f"'expression': \"`{EVENT_TIME_COLUMN}` - INTERVAL '5' SECOND\"}},"
)

# Event times spread over the last minute, so many windows lie behind the latest event time.
SOURCE = f"""
{{{{ config(
    materialized='streaming_source',
    connector='faker',
    with={{
        'rows-per-second': '50',
        'number-of-rows': '{ROW_COUNT}',
        'changelog.mode': 'append',
        'fields.{EVENT_TIME_COLUMN}.expression': "#{{date.past '60','SECONDS'}}",
    }}
) }}}}
order_id BIGINT,
{EVENT_TIME_COLUMN} TIMESTAMP(3)
"""

# The "narrowing" stage: contract-enforced so the columns (and watermark) are explicit.
MT = """
{{ config(
    materialized='__MT_MATERIALIZATION__',
    __MT_CONTRACT__
    __MT_EXTRA__
    __WATERMARK__
) }}
select order_id, __EVENT_TIME__ from {{ ref('__SOURCE__') }}
"""

# Config only some of the upstream materializations accept.
MT_EXTRA_CONFIG = {"materialized_table": "start_mode=\"RESUME_OR_FROM_NOW(INTERVAL '7' DAY)\","}

MT_CONTRACT_CONFIG = "contract={'enforced': true},"

MT_CONTRACT_YML = """- name: __MT__
    columns:
      - name: order_id
        data_type: bigint
      - name: __EVENT_TIME__
        data_type: timestamp(3)"""

MT_YML = """
models:
  __MT_YML__
  - name: __WINDOWED__
    columns:
      - name: window_start
        data_type: timestamp(3)
      - name: window_end
        data_type: timestamp(3)
      - name: order_count
        data_type: bigint
"""

# The stateful stage: a tumbling window over the upstream model's event time.
WINDOWED = """
{{ config(materialized='streaming_table', contract={'enforced': true}) }}
select window_start, window_end, count(*) as order_count
from TABLE(
    TUMBLE(TABLE {{ ref('__MT__') }}, DESCRIPTOR(__EVENT_TIME__), INTERVAL '__WINDOW_SECONDS__' SECOND)
)
group by window_start, window_end
"""


def _models(
    src, mt, windowed, watermark_config, mt_contract=True, mt_materialization="materialized_table"
):
    def fill(text):
        return (
            text.replace("__MT_CONTRACT__", MT_CONTRACT_CONFIG if mt_contract else "")
            .replace("__MT_MATERIALIZATION__", mt_materialization)
            .replace("__MT_EXTRA__", MT_EXTRA_CONFIG.get(mt_materialization, ""))
            .replace("__MT_YML__", MT_CONTRACT_YML if mt_contract else "")
            .replace("__WATERMARK__", watermark_config)
            .replace("__EVENT_TIME__", EVENT_TIME_COLUMN)
            .replace("__SOURCE__", src)
            .replace("__MT__", mt)
            .replace("__WINDOWED__", windowed)
            .replace("__WINDOW_SECONDS__", str(WINDOW_SECONDS))
        )

    return {
        f"{src}.sql": SOURCE,
        f"{mt}.sql": fill(MT),
        f"{windowed}.sql": fill(WINDOWED),
        "models.yml": fill(MT_YML),
    }


class _WatermarkFixtures(ConfluentFixtures):
    SRC: str
    MT: str
    WINDOWED: str

    def _run_upstream_until_populated(self, project):
        """Build the source, wait for it to hold all its rows, then build the upstream model
        (a `table` is a one-off snapshot, so it needs the source complete first) and wait for
        it to have them too."""
        run_dbt(["run", "--select", self.SRC])
        wait_for_snapshot_rows(
            project,
            f"select order_id from {physical_name(self.SRC)}",
            min_rows=ROW_COUNT,
            timeout=120,
        )
        run_dbt(["run", "--select", self.MT])
        rows = wait_for_snapshot_rows(
            project,
            f"select order_id from {physical_name(self.MT)}",
            min_rows=ROW_COUNT,
            timeout=120,
        )
        assert len(rows) == ROW_COUNT


class _WatermarkedWindowTests(_WatermarkFixtures):
    """The shared positive scenario; subclasses choose whether the MT has a contract."""

    MT_CONTRACT = True
    MT_MATERIALIZATION = "materialized_table"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield _models(
            self.SRC,
            self.MT,
            self.WINDOWED,
            WATERMARK_CONFIG,
            mt_contract=self.MT_CONTRACT,
            mt_materialization=self.MT_MATERIALIZATION,
        )

    def test_tumbling_window_over_watermarked_mt(self, project):
        self._run_upstream_until_populated(project)

        results = run_dbt(["run", "--select", self.WINDOWED])
        assert results[0].status.name == "Success"

        # Windows only appear once the watermark has passed their end.
        windows = wait_for_snapshot_rows(
            project,
            f"select window_start, window_end, order_count from {physical_name(self.WINDOWED)}",
            min_rows=1,
            timeout=120,
        )
        assert len(windows) >= 1
        for window_start, window_end, order_count in windows:
            assert (window_end - window_start).total_seconds() == WINDOW_SECONDS
            assert 1 <= order_count <= ROW_COUNT
        assert sum(row[2] for row in windows) <= ROW_COUNT

        # The watermark is on the upstream table itself (not only inferred): the server reports it.
        kind = "MATERIALIZED TABLE" if self.MT_MATERIALIZATION == "materialized_table" else "TABLE"
        ddl = project.run_sql(f"SHOW CREATE {kind} {physical_name(self.MT)}", fetch="all")[0][0]
        assert f"WATERMARK FOR `{EVENT_TIME_COLUMN}`" in ddl


class TestWatermarkEnablesEventTimeWindows(_WatermarkedWindowTests):
    NAME = "wmwindow"
    SRC = "src_wmwindow"
    MT = "mt_wmwindow"
    WINDOWED = "win_wmwindow"


class TestWatermarkWithoutContractOnMaterializedTable(_WatermarkedWindowTests):
    """A materialized_table's watermark needs no contract: Flink infers the other columns."""

    NAME = "wmnocontract"
    SRC = "src_wmnocontract"
    MT = "mt_wmnocontract"
    WINDOWED = "win_wmnocontract"
    MT_CONTRACT = False


class TestWatermarkOnTable(_WatermarkedWindowTests):
    """A `table` (no contract) takes a watermark too."""

    NAME = "wmtable"
    SRC = "src_wmtable"
    MT = "tbl_wmtable"
    WINDOWED = "win_wmtable"
    MT_CONTRACT = False
    MT_MATERIALIZATION = "table"


class TestWithoutWatermarkWindowsAreRejected(_WatermarkFixtures):
    """Control: identical to the class above except for the absent `watermark` config."""

    NAME = "wmcontrol"
    SRC = "src_wmcontrol"
    MT = "mt_wmcontrol"
    WINDOWED = "win_wmcontrol"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield _models(self.SRC, self.MT, self.WINDOWED, watermark_config="")

    def test_tumbling_window_requires_time_attribute(self, project):
        self._run_upstream_until_populated(project)

        results = run_dbt(["run", "--select", self.WINDOWED], expect_pass=False)
        assert results[0].status.name == "Error"
        assert "time attribute" in results[0].message


class TestWatermarkDriftOnTable(ConfluentFixtures):
    """An existing `table`'s watermark is compared with the config on every run, so a changed
    one fails with drift instead of being silently skipped (the watermark is part of the
    CREATE TABLE, so only --full-refresh can change it).

    The unchanged re-run skipping also proves the comparison tolerates how Flink reports the
    watermark back (backticked identifiers, upper-cased keywords)."""

    NAME = "wmdrift"
    MODEL = "tbl_wmdrift"
    # Written the way a user would, not the way Flink reports it back.
    BASELINE_EXPRESSION = f"{EVENT_TIME_COLUMN} - interval '5' second"
    CHANGED_EXPRESSION = f"{EVENT_TIME_COLUMN} - interval '30' second"

    @staticmethod
    def _model(expression):
        watermark = (
            f"watermark={{'column': '{EVENT_TIME_COLUMN}', 'expression': \"{expression}\"}}"
            if expression
            else ""
        )
        return f"""
{{{{ config(materialized='table', {watermark}) }}}}
select 1 as order_id, cast('2024-01-01 00:00:00' as timestamp(3)) as {EVENT_TIME_COLUMN}
"""

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {"models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class")
    def models(self):
        return {f"{self.MODEL}.sql": self._model(self.BASELINE_EXPRESSION)}

    def test_watermark_drift(self, project):
        run_dbt(["run", "--full-refresh"])

        results = run_dbt(["run"])
        assert results[0].message == "SKIP", results[0].message

        set_model_file(
            project, relation(project, self.MODEL), self._model(self.CHANGED_EXPRESSION)
        )
        results = run_dbt(["run"], expect_pass=False)
        assert_drift_error(results, self.MODEL)
        assert "watermark expression" in results[0].message

        # Dropping the config isn't checked: the watermark may come from a contract's
        # constraints, which can't be told apart from here.
        set_model_file(project, relation(project, self.MODEL), self._model(None))
        results = run_dbt(["run"])
        assert results[0].message == "SKIP", results[0].message
