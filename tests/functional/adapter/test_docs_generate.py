"""Tests for `dbt docs generate`'s catalog queries."""

import time

import pytest

from dbt.adapters.confluent.impl import ConfluentAdapter
from dbt.tests.util import run_dbt
from tests.functional.adapter.fixtures import ConfluentFixtures

_RUN_TAG = format(int(time.time()), "08x")

CATALOG_MACROS = {"get_catalog_relations"}

PROJECT_TABLE = f"dbttest_docs_model_{_RUN_TAG}"

# A table that lives in the same schema as the project's but isn't part of it, standing in for
# whatever else shares the environment (e.g. another test session's relations).
UNRELATED_TABLE = f"dbttest_docs_unrelated_{_RUN_TAG}"

PROJECT_TABLE_SQL = """
{{ config(materialized='table') }}
select 1 as id
"""


def capture_catalog_rows(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """Record every row the catalog macros hand back to the adapter, i.e. what the
    INFORMATION_SCHEMA queries actually returned, before any filtering by dbt."""
    captured: list[dict] = []
    original_execute_macro = ConfluentAdapter.execute_macro

    def execute_macro_and_capture(self, macro_name, *args, **kwargs):
        result = original_execute_macro(self, macro_name, *args, **kwargs)
        if macro_name in CATALOG_MACROS:
            captured.extend(result)
        return result

    monkeypatch.setattr(ConfluentAdapter, "execute_macro", execute_macro_and_capture)
    return captured


class TestDocsGenerateIsScopedToProject(ConfluentFixtures):
    """`docs generate` must only read catalog metadata for the project's own relations.

    Querying the whole environment's INFORMATION_SCHEMA made the result depend on every
    other relation concurrently being created and dropped there, and the query often timed out.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {f"{PROJECT_TABLE}.sql": PROJECT_TABLE_SQL}

    @pytest.fixture(scope="class", autouse=True)
    def setup_and_teardown(self, project):
        project.run_sql(f"create table {UNRELATED_TABLE} (id int)")
        yield
        project.run_sql(f"drop table if exists {UNRELATED_TABLE}")
        project.run_sql(f"drop table if exists {PROJECT_TABLE}")

    def test_catalog_queries_exclude_unrelated_relations(self, project, monkeypatch):
        run_dbt(["run"])
        catalog_rows = capture_catalog_rows(monkeypatch)

        catalog = run_dbt(["docs", "generate"])

        queried_tables = {row["table_name"] for row in catalog_rows}
        assert PROJECT_TABLE in queried_tables
        assert UNRELATED_TABLE not in queried_tables
        assert set(catalog.nodes) == {f"model.test.{PROJECT_TABLE}"}

    def test_no_compile_catalog_is_scoped_to_the_projects_dag(self, project, monkeypatch):
        """Without compiling dbt passes no selection, so the catalog is scoped from the
        project's DAG instead."""
        run_dbt(["run"])
        catalog_rows = capture_catalog_rows(monkeypatch)

        run_dbt(["docs", "generate", "--no-compile"])

        queried_tables = {row["table_name"] for row in catalog_rows}
        assert PROJECT_TABLE in queried_tables
        assert UNRELATED_TABLE not in queried_tables
