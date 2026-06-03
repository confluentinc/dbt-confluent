"""Unit tests for ConfluentAdapter.drop_materialized_table.

A materialized table must be dropped with DROP MATERIALIZED TABLE (DROP TABLE
reports "not a regular table"), and Flink has no IF EXISTS form. The adapter
swallows a "does not exist" error so a stale relation cache or an externally
dropped table doesn't fail the run, mirroring drop_relation_if_exists. Any other
error propagates.
"""

from unittest.mock import MagicMock

import pytest
from confluent_sql.exceptions import OperationalError
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.confluent.impl import ConfluentAdapter


class TestDropMaterializedTable:
    @pytest.fixture
    def adapter(self):
        a = ConfluentAdapter.__new__(ConfluentAdapter)
        a.execute = MagicMock()
        return a

    def test_issues_drop_materialized_table_as_streaming_ddl(self, adapter):
        adapter.drop_materialized_table("my_mt")
        adapter.execute.assert_called_once()
        sql = adapter.execute.call_args.args[0]
        assert "DROP MATERIALIZED TABLE" in sql
        assert "my_mt" in sql
        assert adapter.execute.call_args.kwargs.get("execution_mode") == "streaming_ddl"

    def test_swallows_does_not_exist_operational_error(self, adapter):
        adapter.execute.side_effect = OperationalError(
            "Materialized Table with identifier 'env.db.my_mt' does not exist."
        )
        # Must not raise — a missing table is a no-op (matches drop_relation_if_exists).
        adapter.drop_materialized_table("my_mt")

    def test_swallows_does_not_exist_database_error(self, adapter):
        adapter.execute.side_effect = DbtDatabaseError("table 'my_mt' does not exist")
        adapter.drop_materialized_table("my_mt")

    def test_reraises_other_errors(self, adapter):
        err = OperationalError("Could not execute DropTable in path env.db.my_mt")
        adapter.execute.side_effect = err
        with pytest.raises(OperationalError) as exc_info:
            adapter.drop_materialized_table("my_mt")
        assert exc_info.value is err
