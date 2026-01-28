import pytest

from dbt.adapters.base.relation import BaseRelation
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic
from dbt.tests.util import (
    assert_message_in_logs,
    get_model_file,
    run_dbt,
    run_dbt_and_capture,
    set_model_file,
)
from tests.functional.adapter.fixtures import ConfluentFixtures


class TestConfluentMaterializedViewsBasic(ConfluentFixtures, MaterializedViewBasic):
    """
    Confluent Flink SQL implements materialized views as CTAS tables.
    The materialized_view materialization works, but INFORMATION_SCHEMA
    reports them as 'BASE TABLE' rather than 'MATERIALIZED VIEW'.
    """

    @pytest.fixture(scope="class", autouse=True)
    def setup_seed(self, project, my_materialized_view):
        run_dbt(["seed"])

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, my_materialized_view):
        run_dbt(["run", "--models", my_materialized_view.identifier, "--full-refresh"])
        initial_model = get_model_file(project, my_materialized_view)
        yield
        set_model_file(project, my_materialized_view, initial_model)

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> str | None:
        """Query the relation type from INFORMATION_SCHEMA."""
        sql = f"""
          SELECT TABLE_TYPE
          FROM INFORMATION_SCHEMA.`TABLES`
          WHERE TABLE_CATALOG_ID = '{relation.database}'
            AND TABLE_SCHEMA = '{relation.schema}'
            AND TABLE_NAME = '{relation.identifier}'
        """
        result = project.run_sql(sql, fetch="one")
        if result is None:
            return None

        table_type = result[0]
        type_mapping = {
            "BASE TABLE": "table",
            "VIEW": "view",
        }
        return type_mapping.get(table_type, table_type.lower().replace(" ", "_"))

    # -- Override tests to expect "table" for materialized views --
    # Confluent implements materialized views as tables (CTAS)

    def test_materialized_view_create(self, project, my_materialized_view):
        # In Confluent, materialized views are tables
        assert self.query_relation_type(project, my_materialized_view) == "table"

    def test_materialized_view_create_idempotent(self, project, my_materialized_view):
        assert self.query_relation_type(project, my_materialized_view) == "table"
        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "table"

    def test_materialized_view_full_refresh(self, project, my_materialized_view):
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--models", my_materialized_view.identifier, "--full-refresh"]
        )
        assert self.query_relation_type(project, my_materialized_view) == "table"
        # This message doesn't show up because our materialization strategy
        # never calls the default replace_sql macro.
        # assert_message_in_logs(f"Applying REPLACE to: {my_materialized_view}", logs)

    def test_materialized_view_replaces_table(self, project, my_table):
        run_dbt(["run", "--models", my_table.identifier])
        assert self.query_relation_type(project, my_table) == "table"

        self.swap_table_to_materialized_view(project, my_table)

        run_dbt(["run", "--models", my_table.identifier])
        # Both regular tables and materialized views show as "table"
        assert self.query_relation_type(project, my_table) == "table"

    def test_materialized_view_replaces_view(self, project, my_view):
        run_dbt(["run", "--models", my_view.identifier])
        assert self.query_relation_type(project, my_view) == "view"

        self.swap_view_to_materialized_view(project, my_view)

        run_dbt(["run", "--models", my_view.identifier])
        # Materialized view shows as "table" in Confluent
        assert self.query_relation_type(project, my_view) == "table"

    def test_table_replaces_materialized_view(self, project, my_materialized_view):
        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "table"

        self.swap_materialized_view_to_table(project, my_materialized_view)

        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "table"

    def test_view_replaces_materialized_view(self, project, my_materialized_view):
        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "table"

        self.swap_materialized_view_to_view(project, my_materialized_view)

        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "view"

    @pytest.mark.skip("Confluent CTAS tables are continuously updated - no manual refresh needed")
    def test_materialized_view_only_updates_after_refresh(
        self, project, my_materialized_view, my_seed
    ):
        pass
