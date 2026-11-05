"""name_prefix isolation against a live cluster; name rules are unit-tested
in tests/unit/test_name_prefix_matrix.py."""

import pytest

from dbt.tests.util import run_dbt
from tests.functional.adapter.fixtures import ConfluentFixtures
from tests.functional.adapter.isolation import physical_name

# Statement names start with this prefix, so the sweep's name-prefix match misses them;
# it still deletes them by the class's statement_label.
PREFIX = "dbt-adapter-test-ns-"
TOPIC_PREFIX = PREFIX.replace("-", "_")

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


class TestNamePrefix(ConfluentFixtures):
    NAME = "prefix"
    NAME_PREFIX = PREFIX

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "prefix_test_source.sql": SOURCE_SQL,
            "prefix_test_table.sql": TABLE_SQL,
            "prefix_test_ephemeral.sql": EPHEMERAL_SQL,
            "prefix_test_view.sql": VIEW_SQL,
            "models.yml": MODELS_YML,
        }

    @pytest.fixture(scope="class")
    def run_dbt_results(self, project):
        return run_dbt(["run"])

    def _get_relation(self, project, identifier):
        with project.adapter.connection_named("get_relation"):
            return project.adapter.get_relation(project.database, project.test_schema, identifier)

    def test_all_models_succeed(self, run_dbt_results):
        # Includes prefix_test_view, whose SQL inlines the ephemeral CTE
        # __dbt__cte__<class namespace>dbt_adapter_test_ns_prefix_test_ephemeral
        assert {r.node.name: r.status.name for r in run_dbt_results} == {
            "prefix_test_source": "Success",
            "prefix_test_table": "Success",
            "prefix_test_view": "Success",
        }

    @pytest.mark.parametrize("model", ["prefix_test_source", "prefix_test_table"])
    def test_topic_names_use_underscore_prefix(self, project, run_dbt_results, model):
        assert self._get_relation(project, physical_name(f"{TOPIC_PREFIX}{model}")) is not None
        assert self._get_relation(project, physical_name(model)) is None

    def test_view_name_uses_underscore_prefix(self, project, run_dbt_results):
        assert (
            self._get_relation(project, physical_name(f"{TOPIC_PREFIX}prefix_test_view"))
            is not None
        )

    def test_statement_name_uses_hyphen_prefix(self, project, run_dbt_results):
        """The streaming_table INSERT keeps running, so its statement still exists."""
        adapter = project.adapter
        statement_name_prefix = adapter.config.credentials.statement_name_prefix
        name = adapter.get_statement_name(model_name="prefix_test_table", project_name=self.NAME)
        assert name.startswith(f"{PREFIX}{statement_name_prefix}")
        with adapter.connection_named("check_statement"):
            conn = adapter.connections.get_thread_connection()
            assert conn.handle.get_statement(name) is not None
