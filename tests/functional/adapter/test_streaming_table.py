from textwrap import dedent

import pytest

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationType
from dbt.tests.adapter.materialized_view import files
from dbt.tests.util import (
    assert_message_in_logs,
    get_model_file,
    relation_from_name,
    run_dbt,
    run_dbt_and_capture,
    set_model_file,
)
from tests.functional.adapter.fixtures import ConfluentFixtures

MY_STREAMING_TABLE = """
{{ config(
    materialized='streaming_table',
    options={'changelog.mode': 'append'}
) }}
select order_id, price, order_time from {{ ref('my_streaming_source') }}
"""

MY_STREAMING_SOURCE = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    options={
        'rows-per-second': '1',
        'number-of-rows': '100',
        'changelog.mode': 'append',
    }
) }}
(
    order_id BIGINT,
    price DECIMAL(10, 2),
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,
    PRIMARY KEY(`order_id`) NOT ENFORCED
)
"""

MODELS_YML = """
models:
  - name: my_streaming_table
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

class TestStreamingTable(ConfluentFixtures):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_streaming_table.sql": MY_STREAMING_TABLE,
            "models.yml": MODELS_YML
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        project.run_sql(f"drop table my_streaming_source")
        project.run_sql(f"drop table my_streaming_table")


    def test_materialized_source(self, project):
        results = run_dbt(["run"])
        assert results[0].node.name == "my_streaming_source"
        assert results[1].node.name == "my_streaming_table"
        relation = relation_from_name(project.adapter, "my_streaming_table")
        result = project.run_sql(f"select * from {relation}", fetch="one")
        assert len(result) == 3

        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 2
        assert len(catalog.sources) == 0


