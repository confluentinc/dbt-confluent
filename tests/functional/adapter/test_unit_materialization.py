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
    with={'changelog.mode': 'append'}
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
order_id BIGINT,
price DECIMAL(10, 2),
order_time TIMESTAMP(3),
WATERMARK FOR order_time AS order_time + INTERVAL '10' SECONDS,
PRIMARY KEY(`order_id`) NOT ENFORCED
"""

SCHEMA_YML = """
unit_tests:
  - name: test_streaming_table
    model: my_streaming_table
    config:
      watermarks:
        my_streaming_source:
          column: order_time
          expression: "order_time + INTERVAL '10' SECONDS"
    given:
      - input: ref('my_streaming_source')
        rows:
          - order_id: 1
            price: 10.0
            order_time: '2024-01-01 10:00:00'
          - order_id: 1
            price: 10.0
            order_time: '2024-01-01 10:05:00'
          - order_id: 2
            price: 10.0
            order_time: '2024-01-01 10:10:00'
    expect:
      rows:
        - order_id: 1
          price: 10.0
          order_time: '2024-01-01 10:00:00'
        - order_id: 1
          price: 10.0
          order_time: '2024-01-01 10:05:00'
        - order_id: 2
          price: 10.0
          order_time: '2024-01-01 10:10:00'

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


@pytest.mark.skip("Testing strategy are still TODO with streaming tables")
class TestStreamingTests(ConfluentFixtures):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_streaming_table.sql": MY_STREAMING_TABLE,
            "schema.yml": SCHEMA_YML
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        project.run_sql(f"drop table my_streaming_source")
        project.run_sql(f"drop table my_streaming_table")


    def test_streamint_tests(self, project):
        results = run_dbt(["run"])
        run_dbt(["test"])


