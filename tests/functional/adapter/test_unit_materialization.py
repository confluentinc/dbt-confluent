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
) }}
select order_id, price, order_time from {{ ref('my_streaming_source') }}
"""

MY_STREAMING_SOURCE = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
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
    given:
      - input: ref('my_streaming_source')
        rows:
          - order_id: 1
            price: 10.0
            order_time: '2024-01-01 10:00:00'
          - order_id: 2
            price: 20.0
            order_time: '2024-01-01 10:05:00'
          - order_id: 3
            price: 30.0
            order_time: '2024-01-01 10:10:00'
    expect:
      rows:
        - order_id: 1
          price: 10.0
          order_time: '2024-01-01 10:00:00'
        - order_id: 2
          price: 20.0
          order_time: '2024-01-01 10:05:00'
        - order_id: 3
          price: 30.0
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


JOIN_SOURCE_A = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
    }
) }}
id BIGINT,
event_time TIMESTAMP(3),
WATERMARK FOR event_time AS event_time + INTERVAL '10' SECONDS
"""

JOIN_SOURCE_B = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
    }
) }}
id BIGINT,
event_time TIMESTAMP(3),
WATERMARK FOR event_time AS event_time + INTERVAL '10' SECONDS
"""

JOIN_MODEL = """
{{ config(
    materialized='streaming_table',
) }}
SELECT
  a.event_time AS a_time,
  b.event_time AS b_time,
  a.id AS a_id
FROM {{ ref('join_source_a') }} a
INNER JOIN {{ ref('join_source_b') }} b
  ON a.id = b.id
WHERE
  a.event_time BETWEEN b.event_time AND b.event_time + INTERVAL '10' SECONDS
"""

JOIN_SCHEMA_YML = """
unit_tests:
  - name: test_join_on_event_time
    model: join_model
    given:
      - input: ref('join_source_a')
        rows:
          - id: 1
            event_time: '2024-01-01 10:00:00'
      - input: ref('join_source_b')
        rows:
          - id: 1
            event_time: '2024-01-01 10:00:00'
    expect:
      rows:
        - a_time: '2024-01-01 10:00:00'
          b_time: '2024-01-01 10:00:00'
          a_id: 1

models:
  - name: join_model
    columns:
      - name: a_time
        data_type: timestamp(3)
      - name: b_time
        data_type: timestamp(3)
      - name: a_id
        data_type: bigint
"""


class TestJoinOnExplicitTimestamp(ConfluentFixtures):
    """Test that joining two streaming sources on explicit timestamp columns
    works correctly in unit tests (as opposed to $rowtime which is uncontrollable)."""

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "join_source_a.sql": JOIN_SOURCE_A,
            "join_source_b.sql": JOIN_SOURCE_B,
            "join_model.sql": JOIN_MODEL,
            "schema.yml": JOIN_SCHEMA_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def custom_clean_up(self, project):
        yield
        project.run_sql("drop table if exists join_source_a")
        project.run_sql("drop table if exists join_source_b")
        project.run_sql("drop table if exists join_model")

    def test_join_on_explicit_timestamp(self, project):
        run_dbt(["run"])
        run_dbt(["test"])


class TestStreamingTests(ConfluentFixtures):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_streaming_table.sql": MY_STREAMING_TABLE,
            "schema.yml": SCHEMA_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def custom_clean_up(self, project):
        yield
        project.run_sql(f"drop table if exists my_streaming_source")
        project.run_sql(f"drop table if exists my_streaming_table")

    def test_streaming_tests(self, project):
        results = run_dbt(["run"])
        run_dbt(["test"])
