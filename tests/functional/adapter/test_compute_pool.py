"""Functional tests for per-model `compute_pool_id` config (#37).

A model can override the connection-default compute pool via
`config(compute_pool_id='lfcp-...')`. The override flows through the custom
`statement` macro -> adapter.execute() -> add_query() -> cursor.execute().

These tests need a SECOND compute pool, distinct from the one used as the
profile default (CONFLUENT_COMPUTE_POOL_ID). Set CONFLUENT_COMPUTE_POOL_ID_2
to a different pool in the same environment+region, or the module is skipped.
"""

import os

import pytest
from confluent_sql.exceptions import StatementNotFoundError

from dbt.tests.util import run_dbt
from tests.functional.adapter.fixtures import ClassScopedCleanup

DEFAULT_POOL = os.getenv("CONFLUENT_COMPUTE_POOL_ID")
SECOND_POOL = os.getenv("CONFLUENT_COMPUTE_POOL_ID_2")

pytestmark = pytest.mark.skipif(
    not SECOND_POOL or SECOND_POOL == DEFAULT_POOL,
    reason="CONFLUENT_COMPUTE_POOL_ID_2 must be set to a pool different from "
    "CONFLUENT_COMPUTE_POOL_ID to exercise per-model compute pool override",
)

MY_STREAMING_SOURCE = """
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

# Overrides the connection-default pool with the second pool.
MY_POOLED_TABLE = """
{{{{ config(
    materialized='streaming_table',
    compute_pool_id='{pool}',
    with={{'changelog.mode': 'append'}},
) }}}}
select order_id, price from {{{{ ref('my_streaming_source') }}}}
""".format(pool=SECOND_POOL or "")

MODELS_YML = """
models:
  - name: my_pooled_table
    columns:
      - name: order_id
        data_type: bigint
      - name: price
        data_type: decimal(10,2)
"""


class TestPerModelComputePool(ClassScopedCleanup):
    NAME = "computepool"
    TABLES = ["my_pooled_table", "my_streaming_source"]

    @pytest.fixture(scope="class")
    def run_dbt_results(self, project):
        return run_dbt(["run"])

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_pooled_table.sql": MY_POOLED_TABLE,
            "models.yml": MODELS_YML,
        }

    def test_model_runs_on_overridden_pool(self, project, run_dbt_results):
        """The streaming_table INSERT lands on the per-model second pool, not
        the connection default."""
        adapter = project.adapter
        name = adapter.get_statement_name(
            model_name="my_pooled_table",
            project_name=self.NAME,
        )

        with adapter.connection_named("check_pool"):
            conn = adapter.connections.get_thread_connection()
            try:
                stmt = conn.handle.get_statement(name)
            except StatementNotFoundError:
                pytest.fail(f"Expected Flink statement '{name}' to exist but it was not found")

        assert stmt.compute_pool_id == SECOND_POOL, (
            f"Expected statement '{name}' on overridden pool {SECOND_POOL}, "
            f"but it ran on {stmt.compute_pool_id}"
        )
        assert stmt.compute_pool_id != DEFAULT_POOL, (
            "Override did not take effect: statement ran on the connection-default pool"
        )
