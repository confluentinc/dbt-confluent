"""Functional tests for the profile's resource_name_prefix.

The rest of the suite runs unprefixed (see ConfluentFixtures.RESOURCE_NAME_PREFIX);
this class sets a prefix and checks it reaches topic and statement names in
their two forms: hyphens in statement names, underscores in topic names.
"""

import pytest

from dbt.tests.util import run_dbt
from tests.functional.adapter._helpers import delete_statements_by_label
from tests.functional.adapter.fixtures import ConfluentFixtures

PREFIX = "dbt-adapter-test-"
# confluent__generate_alias_name turns the prefix's hyphens into underscores
TOPIC_PREFIX = PREFIX.replace("-", "_")

# Model names are unique to this file: the whole suite shares one cluster and
# other tests create unprefixed relations (e.g. my_streaming_table), so the
# "bare name does not exist" checks below only hold for names nobody else uses.

SOURCE_SQL = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
        'changelog.mode': 'append',
    }
) }}
order_id BIGINT,
price DECIMAL(10, 2),
order_time TIMESTAMP(3),
WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,
PRIMARY KEY(`order_id`) NOT ENFORCED
"""

TABLE_SQL = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
) }}
select order_id, price, order_time from {{ ref('prefix_test_source') }}
"""

MODELS_YML = """
models:
  - name: prefix_test_table
    columns:
      - name: order_id
        data_type: bigint
        constraints:
          - type: not_null
          - type: primary_key
            expression: "not enforced"
      - name: price
        data_type: decimal(10,2)
      - name: order_time
        data_type: timestamp(3)
"""

EPHEMERAL_SQL = """
{{ config(materialized='ephemeral') }}
select order_id, price from {{ ref('prefix_test_source') }}
"""

VIEW_SQL = """
{{ config(materialized='view') }}
select * from {{ ref('prefix_test_ephemeral') }}
"""


class TestResourceNamePrefix(ConfluentFixtures):
    NAME = "prefix"
    RESOURCE_NAME_PREFIX = PREFIX
    TABLES = ["prefix_test_table", "prefix_test_source"]
    VIEWS = ["prefix_test_view"]

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "prefix_test_source.sql": SOURCE_SQL,
            "prefix_test_table.sql": TABLE_SQL,
            "prefix_test_ephemeral.sql": EPHEMERAL_SQL,
            "prefix_test_view.sql": VIEW_SQL,
            "models.yml": MODELS_YML,
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project, dbt_profile_data):
        # Suppress the per-test statement cleanup; class_clean_up handles teardown.
        yield

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        delete_statements_by_label(project, label)
        for name in self.VIEWS:
            project.run_sql(f"drop view if exists `{TOPIC_PREFIX}{name}`")
        for name in self.TABLES:
            project.run_sql(f"drop table if exists `{TOPIC_PREFIX}{name}`")

    @pytest.fixture(scope="class")
    def run_dbt_results(self, project):
        return run_dbt(["run"])

    def _get_relation(self, project, identifier):
        with project.adapter.connection_named("get_relation"):
            return project.adapter.get_relation(project.database, project.test_schema, identifier)

    def test_all_models_succeed(self, run_dbt_results):
        # Includes prefix_test_view, whose SQL inlines the ephemeral CTE
        # __dbt__cte__dbt_adapter_test_prefix_test_ephemeral
        assert {r.node.name: r.status.name for r in run_dbt_results} == {
            "prefix_test_source": "Success",
            "prefix_test_table": "Success",
            "prefix_test_view": "Success",
        }

    @pytest.mark.parametrize("model", ["prefix_test_source", "prefix_test_table"])
    def test_topic_names_use_underscore_prefix(self, project, run_dbt_results, model):
        assert self._get_relation(project, f"{TOPIC_PREFIX}{model}") is not None
        assert self._get_relation(project, model) is None

    def test_view_name_uses_underscore_prefix(self, project, run_dbt_results):
        assert self._get_relation(project, f"{TOPIC_PREFIX}prefix_test_view") is not None

    def test_statement_name_uses_hyphen_prefix(self, project, run_dbt_results):
        """The streaming_table INSERT keeps running, so its statement is still there."""
        adapter = project.adapter
        name = adapter.get_statement_name(model_name="prefix_test_table", project_name=self.NAME)
        assert name.startswith(PREFIX)
        with adapter.connection_named("check_statement"):
            conn = adapter.connections.get_thread_connection()
            assert conn.handle.get_statement(name) is not None
