"""Functional test for contract and drift checks accepting CHAR(n)/BINARY(n) literals.

Flink types a string literal like 'abc' as CHAR(3) and a binary literal like x'0102' as BINARY(2).
Before the fix, dbt's contract check compared the literal's type name with the yml column's
VARCHAR/VARBINARY and failed, requiring CAST(... AS STRING). After the fix, a CHAR(n)/BINARY(n)
SELECT column is accepted for a yml STRING/BYTES (bare VARCHAR/VARBINARY) column, in both the
contract check (every materialization that runs it) and the drift check (streaming_table; table
with enforced contract).
"""

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture
from tests.functional.adapter.fixtures import ConfluentFixtures

LITERAL_STREAMING_TABLE = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
) }}
select 'abc' as s, 'n/a' as v, x'0102' as b
"""

LITERAL_TABLE = """
{{ config(
    materialized='table',
    contract={'enforced': true},
) }}
select 'abc' as s, 'n/a' as v, x'0102' as b
"""

LITERAL_MATERIALIZED_TABLE = """
{{ config(
    materialized='materialized_table',
    contract={'enforced': true},
) }}
select 'abc' as s, 'n/a' as v, x'0102' as b
"""

LITERAL_VIEW = """
{{ config(
    materialized='view',
    contract={'enforced': true},
) }}
select 'abc' as s, 'n/a' as v, x'0102' as b
"""

LITERAL_MODELS_YML = """
models:
  - name: literal_streaming_table
    columns:
      - name: s
        data_type: string
      - name: v
        data_type: varchar
      - name: b
        data_type: bytes
  - name: literal_table
    columns:
      - name: s
        data_type: string
      - name: v
        data_type: varchar
      - name: b
        data_type: bytes
  - name: literal_materialized_table
    columns:
      - name: s
        data_type: string
      - name: v
        data_type: varchar
      - name: b
        data_type: bytes
  - name: literal_view
    columns:
      - name: s
        data_type: string
      - name: v
        data_type: varchar
      - name: b
        data_type: bytes
"""


class TestContractLiterals(ConfluentFixtures):
    """CHAR(n)/BINARY(n) literals pass the contract check and the drift check for STRING/BYTES
    columns. Run twice: the first run creates each model, and the second run re-runs them (the
    streaming_table restarts its completed INSERT, the table skips, the materialized_table
    re-asserts, and the view alters). Run 2 is a plain `dbt run`, so it takes the re-run paths.
    Both runs succeed."""

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {"models": {"+schema": unique_schema}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "literal_streaming_table.sql": LITERAL_STREAMING_TABLE,
            "literal_table.sql": LITERAL_TABLE,
            "literal_materialized_table.sql": LITERAL_MATERIALIZED_TABLE,
            "literal_view.sql": LITERAL_VIEW,
            "models.yml": LITERAL_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["run", "--full-refresh"])

    def test_rerun_passes_contract_and_drift(self, project):
        results, logs = run_dbt_and_capture(["run"])
        messages = {r.node.name: r.message for r in results}
        assert len(messages) == 4
        # The table's drift check (dry-run path) found no drift.
        assert messages["literal_table"] == "SKIP"
        # The streaming_table's bounded INSERT has usually completed, so it restarts; if not, it
        # skips. Either way its drift check passed and it wasn't recreated.
        assert (
            messages["literal_streaming_table"] == "SKIP"
            or "Re-submitting (no full refresh required)." in logs
        )
