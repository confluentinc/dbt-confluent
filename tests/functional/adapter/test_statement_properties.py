"""Functional tests for the `statement_properties` config (#76).

A model can set Flink SET-style statement properties (e.g.
`sql.tables.scan.idle-timeout`) via `config(statement_properties={...})`. The
value flows through the custom `statement` macro -> adapter.execute() ->
add_query() -> cursor.execute(properties=...), and is wired into every
materialization's `'main'` statement (never `streaming_table`'s separate
`'ddl'` statement, which has no query to tune).
"""

import pytest
from confluent_sql.exceptions import StatementNotFoundError

from dbt.tests.util import relation_from_name, run_dbt
from tests.functional.adapter._helpers import get_result_by_name
from tests.functional.adapter.fixtures import ClassScopedCleanup

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

IDLE_TIMEOUT_PROPERTY = "sql.tables.scan.idle-timeout"
IDLE_TIMEOUT_VALUE = "30 s"

RESERVED_PROPERTY = "sql.snapshot.mode"
RESERVED_PROPERTY_VALUE = "NOW"

MY_TUNED_STREAMING_TABLE = f"""
{{{{ config(
    materialized='streaming_table',
    with={{'changelog.mode': 'append'}},
    statement_properties={{'{IDLE_TIMEOUT_PROPERTY}': '{IDLE_TIMEOUT_VALUE}'}},
) }}}}
select order_id, price from {{{{ ref('my_streaming_source') }}}}
"""

MY_PLAIN_STREAMING_TABLE = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
) }}
select order_id, price from {{ ref('my_streaming_source') }}
"""

MY_BAD_PROPERTIES_TABLE = f"""
{{{{ config(
    materialized='streaming_table',
    with={{'changelog.mode': 'append'}},
    statement_properties={{'{RESERVED_PROPERTY}': '{RESERVED_PROPERTY_VALUE}'}},
) }}}}
select order_id, price from {{{{ ref('my_streaming_source') }}}}
"""

MODELS_YML = """
models:
  - name: my_tuned_streaming_table
    columns:
      - name: order_id
        data_type: bigint
      - name: price
        data_type: decimal(10,2)
  - name: my_plain_streaming_table
    columns:
      - name: order_id
        data_type: bigint
      - name: price
        data_type: decimal(10,2)
"""

BAD_PROPERTIES_MODELS_YML = """
models:
  - name: my_bad_properties_table
    columns:
      - name: order_id
        data_type: bigint
      - name: price
        data_type: decimal(10,2)
"""


class TestStatementProperties(ClassScopedCleanup):
    NAME = "stmtprops"
    TABLES = ["my_tuned_streaming_table", "my_plain_streaming_table", "my_streaming_source"]

    @pytest.fixture(scope="class")
    def run_dbt_results(self, project):
        return run_dbt(["run"])

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_tuned_streaming_table.sql": MY_TUNED_STREAMING_TABLE,
            "my_plain_streaming_table.sql": MY_PLAIN_STREAMING_TABLE,
            "models.yml": MODELS_YML,
        }

    def _get_statement(self, project, model_name):
        adapter = project.adapter
        name = adapter.get_statement_name(model_name=model_name, project_name=self.NAME)
        with adapter.connection_named(f"check_{model_name}"):
            conn = adapter.connections.get_thread_connection()
            try:
                return conn.handle.get_statement(name)
            except StatementNotFoundError:
                pytest.fail(f"Expected Flink statement '{name}' to exist but it was not found")

    def test_statement_properties_land_on_the_insert_statement(self, project, run_dbt_results):
        """The INSERT statement (not the DDL) carries the configured property."""
        stmt = self._get_statement(project, "my_tuned_streaming_table")

        assert stmt.properties.get(IDLE_TIMEOUT_PROPERTY) == IDLE_TIMEOUT_VALUE, (
            f"Expected '{IDLE_TIMEOUT_PROPERTY}' to be set on statement '{stmt.name}', "
            f"got properties: {stmt.properties}"
        )

        relation = relation_from_name(project.adapter, "my_tuned_streaming_table")
        result = project.run_sql(f"select * from {relation}", fetch="one")
        assert len(result[0]) == 2

    def test_unconfigured_model_does_not_have_the_property(self, project, run_dbt_results):
        """Control: a model that never sets `statement_properties` must NOT show
        our configured value for the same key. Without this, the previous test
        could pass by coincidence if Flink's default idle-timeout ever happened
        to equal IDLE_TIMEOUT_VALUE, rather than because our plumbing actually
        threaded the configured value through."""
        stmt = self._get_statement(project, "my_plain_streaming_table")

        assert stmt.properties.get(IDLE_TIMEOUT_PROPERTY) != IDLE_TIMEOUT_VALUE, (
            f"Statement '{stmt.name}' (no statement_properties configured) reports "
            f"{IDLE_TIMEOUT_PROPERTY}={IDLE_TIMEOUT_VALUE!r}, the same value the other "
            f"test configures explicitly -- that test wouldn't actually be proving "
            f"our config took effect. Got properties: {stmt.properties}"
        )


class TestReservedStatementPropertyRejected(ClassScopedCleanup):
    """A user-supplied reserved key (driver-owned) fails loudly rather than
    being silently ignored or overridden -- confirms dbt-confluent needs no
    adapter-side validation of its own for this; confluent_sql's own
    `validate_properties_dict()` already rejects it with `InterfaceError`,
    which the adapter's `exception_handler` wraps into a `DbtDatabaseError`.
    """

    NAME = "stmtpropsbad"
    # my_bad_properties_table's DDL (CREATE TABLE) succeeds -- only the INSERT
    # is rejected for the reserved property -- so it leaves a real table behind
    # and must be dropped too.
    TABLES = ["my_streaming_source", "my_bad_properties_table"]

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_bad_properties_table.sql": MY_BAD_PROPERTIES_TABLE,
            "models.yml": BAD_PROPERTIES_MODELS_YML,
        }

    def test_reserved_property_key_fails_the_run(self, project):
        results = run_dbt(["run"], expect_pass=False)
        result = get_result_by_name(results, "my_bad_properties_table")
        assert result is not None, "my_bad_properties_table not found in results"
        assert result.status.name == "Error", (
            f"Expected status 'Error' but got '{result.status.name}'"
        )
        assert RESERVED_PROPERTY in result.message, (
            f"Expected the reserved-property rejection to name the offending key, "
            f"got: {result.message}"
        )
