"""Functional tests for rebuilding `view` models (#74).

Flink has no `CREATE OR REPLACE VIEW`, so an existing view is updated in place with
`ALTER VIEW ... AS`. A rerun keeps the view, and new SQL the server rejects, or an enforced contract
that no longer matches, leaves the previous view serving instead of dropping it first.
"""

from functools import wraps
from unittest.mock import patch

import pytest

from dbt.adapters.confluent.impl import ConfluentAdapter
from dbt.tests.util import (
    check_relation_types,
    relation_from_name,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)
from tests.functional.adapter._helpers import snapshot_query
from tests.functional.adapter.fixtures import ConfluentFixtures
from tests.functional.adapter.isolation import physical_name

# ConfluentFixtures sets `+full_refresh: True` for every model, which would send each rerun down
# the --full-refresh path (drop and create). `full_refresh=false` keeps these models on the plain
# run path, which ALTERs an existing view.
BASE_VIEW = """
{{ config(materialized='view', full_refresh=false) }}
select cast(1 as int) as id, cast('v1' as string) as rev
"""

CHANGED_VIEW = """
{{ config(materialized='view', full_refresh=false) }}
select cast(1 as int) as id, cast('v2' as string) as rev, cast('x' as string) as extra
"""

# References a column that doesn't exist, so the server rejects the statement.
BROKEN_VIEW = """
{{ config(materialized='view', full_refresh=false) }}
select cast(1 as int) as id, missing_col as rev
"""

CHILD_VIEW = """
{{ config(materialized='view', full_refresh=false) }}
select id, rev from {{ ref('base_view') }}
"""

# Renames `rev`, so it no longer matches the contract's columns.
DRIFTED_CONTRACT_VIEW = """
{{ config(materialized='view', full_refresh=false) }}
select cast(1 as int) as id, cast('v1' as string) as rev2
"""

# No `full_refresh` config, and its test class drops ConfluentFixtures' `+full_refresh: True`, so
# only the --full-refresh flag can send it down the drop path.
REFRESHED_VIEW = """
{{ config(materialized='view') }}
select cast(1 as int) as id, cast('v1' as string) as rev
"""

# Same columns and types as CONTRACT_YML declares, different values.
MATCHING_CONTRACT_VIEW_V2 = """
{{ config(materialized='view', full_refresh=false) }}
select cast(1 as int) as id, cast('v2' as string) as rev
"""

# The same model name as a table, then as a view.
SWITCHING_TABLE = """
{{ config(materialized='table', full_refresh=false) }}
select cast(1 as int) as id, cast('a' as string) as name
"""

SWITCHING_VIEW = """
{{ config(materialized='view', full_refresh=false) }}
select cast(1 as int) as id, cast('a' as string) as name
"""

# Renames `rev` like DRIFTED_CONTRACT_VIEW, but with no `full_refresh` config, so only the
# --full-refresh flag decides between ALTER and drop-and-create.
DRIFTED_CONTRACT_REFRESHED_VIEW = """
{{ config(materialized='view') }}
select cast(1 as int) as id, cast('v1' as string) as rev2
"""

CONTRACT_YML = """
models:
  - name: contract_view
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
      - name: rev
        data_type: string
"""


class ViewFixtures(ConfluentFixtures):
    """Reads back the views a test class builds. ConfluentFixtures namespaces each model's
    relation and sweeps the class's views when the class finishes."""

    def read(self, project, name, columns):
        """The rows `columns` select from the view of the model `name`, as tuples."""
        view = relation_from_name(project.adapter, physical_name(name))
        rows = snapshot_query(project, f"select {columns} from {view}", fetch="all")
        return [tuple(row) for row in rows]


class TestViewRerunAltersInPlace(ViewFixtures):
    @pytest.fixture(scope="class")
    def models(self):
        return {"base_view.sql": BASE_VIEW, "child_view.sql": CHILD_VIEW}

    def test_rerun_alters_existing_views(self, project):
        run_dbt(["run"])
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert "alter view" in logs.lower()
        assert "drop view" not in logs.lower()
        check_relation_types(
            project.adapter,
            {physical_name("base_view"): "view", physical_name("child_view"): "view"},
        )

    def test_changed_sql_takes_effect(self, project):
        run_dbt(["run"])
        write_file(CHANGED_VIEW, project.project_root, "models", "base_view.sql")
        run_dbt(["run"])
        assert self.read(project, "base_view", "id, rev, extra") == [(1, "v2", "x")]
        assert self.read(project, "child_view", "id, rev") == [(1, "v2")]


class TestRejectedSqlKeepsView(ViewFixtures):
    @pytest.fixture(scope="class")
    def models(self):
        return {"base_view.sql": BASE_VIEW, "child_view.sql": CHILD_VIEW}

    def test_rejected_sql_keeps_previous_view(self, project):
        run_dbt(["run"])
        write_file(BROKEN_VIEW, project.project_root, "models", "base_view.sql")
        results = run_dbt(["run", "--select", "base_view"], expect_pass=False)
        assert "missing_col" in results[0].message
        assert self.read(project, "base_view", "id, rev") == [(1, "v1")]
        assert self.read(project, "child_view", "id, rev") == [(1, "v1")]


class TestContractFailureKeepsView(ViewFixtures):
    @pytest.fixture(scope="class")
    def models(self):
        return {"contract_view.sql": BASE_VIEW, "schema.yml": CONTRACT_YML}

    def test_contract_failure_keeps_previous_view(self, project):
        run_dbt(["run"])
        write_file(DRIFTED_CONTRACT_VIEW, project.project_root, "models", "contract_view.sql")
        results = run_dbt(["run"], expect_pass=False)
        assert "contract" in results[0].message.lower()
        assert self.read(project, "contract_view", "id, rev") == [(1, "v1")]


class TestMatchingContractView(ViewFixtures):
    """An enforced contract the SQL satisfies must not get in the way: the first run creates the
    view (CREATE VIEW checks the contract itself), and a rerun passes the early check and alters it."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"contract_view.sql": BASE_VIEW, "schema.yml": CONTRACT_YML}

    def test_matching_contract_creates_then_alters(self, project):
        run_dbt(["run"])
        assert self.read(project, "contract_view", "id, rev") == [(1, "v1")]

        write_file(MATCHING_CONTRACT_VIEW_V2, project.project_root, "models", "contract_view.sql")
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert "alter view" in logs.lower()
        assert "drop view" not in logs.lower()
        assert self.read(project, "contract_view", "id, rev") == [(1, "v2")]


class FlagOnlyFullRefreshViewFixtures(ViewFixtures):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        """ConfluentFixtures' project config without `+full_refresh: True`, so the --full-refresh
        flag alone decides between ALTER and drop-and-create."""
        return {
            "tests": {"+schema": unique_schema},
            "models": {"+schema": unique_schema},
            "seeds": {"+schema": unique_schema},
        }


class TestFullRefreshRecreatesView(FlagOnlyFullRefreshViewFixtures):
    @pytest.fixture(scope="class")
    def models(self):
        return {"refreshed_view.sql": REFRESHED_VIEW}

    def test_full_refresh_drops_and_creates(self, project):
        run_dbt(["run"])
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert "alter view" in logs.lower()
        assert "drop view" not in logs.lower()
        _, logs = run_dbt_and_capture(["--debug", "run", "--full-refresh"])
        assert "drop view if exists" in logs.lower()
        assert "create view" in logs.lower()
        assert "alter view" not in logs.lower()
        assert self.read(project, "refreshed_view", "id, rev") == [(1, "v1")]


class TestTableToViewSwitch(ViewFixtures):
    """A model switching from `table` to `view` without --full-refresh drops the table and creates
    the view (no ALTER, nothing to alter). The view's statement can't exist yet, so the stale
    statement cleanup must not expect it (a pool-scoped 403 there would warn loudly); once the
    view exists, a rerun does expect it."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"switching.sql": SWITCHING_TABLE}

    @staticmethod
    def run_recording_delete_statement_expectations():
        """Run dbt, returning the `expect_exists` of each adapter.delete_statement call made."""
        expectations = []
        original = ConfluentAdapter.delete_statement

        @wraps(original)  # keeps the `@available` marker dbt's jinja wrapper looks for
        def spy(self, statement_name, expect_exists=True):
            expectations.append(expect_exists)
            return original(self, statement_name, expect_exists=expect_exists)

        with patch.object(ConfluentAdapter, "delete_statement", spy):
            run_dbt(["run"])
        return expectations

    def test_switch_drops_table_and_creates_view(self, project):
        run_dbt(["run"])
        check_relation_types(project.adapter, {physical_name("switching"): "table"})

        write_file(SWITCHING_VIEW, project.project_root, "models", "switching.sql")
        expectations = self.run_recording_delete_statement_expectations()
        check_relation_types(project.adapter, {physical_name("switching"): "view"})
        assert self.read(project, "switching", "id, name") == [(1, "a")]
        assert expectations == [False]

        # Now it is a view, so the rerun alters it, and its statement is expected to exist.
        expectations = self.run_recording_delete_statement_expectations()
        check_relation_types(project.adapter, {physical_name("switching"): "view"})
        assert expectations == [True]


class TestFullRefreshContractFailureKeepsView(FlagOnlyFullRefreshViewFixtures):
    """--full-refresh drops and creates the view, but the enforced contract is checked before the
    drop, so a mismatch fails the model with the previous view still in place."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"contract_view.sql": REFRESHED_VIEW, "schema.yml": CONTRACT_YML}

    def test_full_refresh_contract_failure_keeps_previous_view(self, project):
        run_dbt(["run"])
        write_file(
            DRIFTED_CONTRACT_REFRESHED_VIEW, project.project_root, "models", "contract_view.sql"
        )
        results, logs = run_dbt_and_capture(
            ["--debug", "run", "--full-refresh"], expect_pass=False
        )
        assert "contract" in results[0].message.lower()
        assert "drop view" not in logs.lower()
        assert self.read(project, "contract_view", "id, rev") == [(1, "v1")]
