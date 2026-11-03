"""Tests for `dbt docs generate`'s catalog queries."""

import pytest

from dbt.adapters.confluent.impl import ConfluentAdapter
from dbt.tests.util import run_dbt
from tests.functional.adapter.fixtures import ConfluentFixtures
from tests.functional.adapter.isolation import physical_name

CATALOG_MACROS = {"get_catalog_relations"}

PROJECT_MODEL = "docs_model"

# A table that lives in the same schema as the project's but isn't part of it, standing in for
# whatever else shares the environment (e.g. another test session's relations).
UNRELATED_TABLE = "docs_unrelated"

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
        return {f"{PROJECT_MODEL}.sql": PROJECT_TABLE_SQL}

    @pytest.fixture(scope="class")
    def project_relation(self, relation_namespace):
        """The project model's name in the shared schema (models are namespaced per class)."""
        return physical_name(PROJECT_MODEL)

    @pytest.fixture(scope="class")
    def unrelated_relation(self, relation_namespace):
        """Namespaced like the model's so the class's cleanup sweep also reclaims it."""
        return physical_name(UNRELATED_TABLE)

    @pytest.fixture(scope="class", autouse=True)
    def create_unrelated_table(self, project, unrelated_relation):
        project.run_sql(f"create table {unrelated_relation} (id int)")

    def test_catalog_queries_exclude_unrelated_relations(
        self, project, monkeypatch, project_relation, unrelated_relation
    ):
        run_dbt(["run"])
        catalog_rows = capture_catalog_rows(monkeypatch)

        catalog = run_dbt(["docs", "generate"])

        queried_tables = {row["table_name"] for row in catalog_rows}
        assert project_relation in queried_tables
        assert unrelated_relation not in queried_tables
        assert set(catalog.nodes) == {f"model.test.{PROJECT_MODEL}"}

    def test_no_compile_catalog_is_scoped_to_the_projects_dag(
        self, project, monkeypatch, project_relation, unrelated_relation
    ):
        """Without compiling dbt passes no selection, so the catalog is scoped from the
        project's DAG instead."""
        run_dbt(["run"])
        catalog_rows = capture_catalog_rows(monkeypatch)

        run_dbt(["docs", "generate", "--no-compile"])

        queried_tables = {row["table_name"] for row in catalog_rows}
        assert project_relation in queried_tables
        assert unrelated_relation not in queried_tables
