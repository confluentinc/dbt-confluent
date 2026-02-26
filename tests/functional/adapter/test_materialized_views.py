import pytest

from dbt.adapters.base.relation import BaseRelation
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic
from dbt.tests.util import (
    get_model_file,
    run_dbt,
    run_dbt_and_capture,
    set_model_file,
)
from tests.functional.adapter.fixtures import ConfluentFixtures


class TestConfluentMaterializedViewsBasic(ConfluentFixtures, MaterializedViewBasic):
    """
    Confluent Flink SQL implements materialized views as CTAS tables.
    INFORMATION_SCHEMA reports them as 'BASE TABLE' rather than 'MATERIALIZED VIEW'.

    This means some upstream tests are redundant (table-to-MV swap is invisible
    since both are 'table' in Confluent), and the refresh test doesn't apply
    because CTAS tables are continuously updated by Flink.
    """

    @pytest.fixture(scope="class", autouse=True)
    def cleanup(self, project):
        run_dbt(["seed"])
        yield
        project.run_sql(f"drop table if exists my_seed")
        project.run_sql(f"drop table if exists my_table")
        project.run_sql(f"drop table if exists my_view")

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, my_materialized_view):
        run_dbt(["run", "--models", my_materialized_view.identifier, "--full-refresh"])
        initial_model = get_model_file(project, my_materialized_view)
        yield
        set_model_file(project, my_materialized_view, initial_model)
        project.run_sql(f"drop table if exists {my_materialized_view}")

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

        table_type = result[0][0]
        type_mapping = {
            "BASE TABLE": "table",
            "VIEW": "view",
        }
        return type_mapping.get(table_type, table_type.lower().replace(" ", "_"))

    # -- Tests that are meaningful for Confluent --

    def test_materialized_view_create(self, project, my_materialized_view):
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

    def test_materialized_view_replaces_view(self, project, my_view):
        """A real view is replaced by a CTAS table when switching to materialized_view."""
        run_dbt(["run", "--models", my_view.identifier])
        assert self.query_relation_type(project, my_view) == "view"

        self.swap_view_to_materialized_view(project, my_view)

        run_dbt(["run", "--models", my_view.identifier])
        assert self.query_relation_type(project, my_view) == "table"

    def test_view_replaces_materialized_view(self, project, my_materialized_view):
        """A CTAS table is replaced by a real view when switching to view."""
        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "table"

        self.swap_materialized_view_to_view(project, my_materialized_view)

        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "view"

    # -- Skipped: redundant because both table and materialized_view are 'table' in Confluent --

    @pytest.mark.skip(
        "Redundant: both table and materialized_view are reported as 'table' in Confluent"
    )
    def test_materialized_view_replaces_table(self, project, my_table):
        pass

    @pytest.mark.skip(
        "Redundant: both table and materialized_view are reported as 'table' in Confluent"
    )
    def test_table_replaces_materialized_view(self, project, my_materialized_view):
        pass

    @pytest.mark.skip("CTAS tables are continuously updated by Flink - no manual refresh needed")
    def test_materialized_view_only_updates_after_refresh(
        self, project, my_materialized_view, my_seed
    ):
        pass
