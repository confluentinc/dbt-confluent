"""Unit tests for ConfluentAdapter.drop_materialized_table.

A materialized table must be dropped with DROP MATERIALIZED TABLE (DROP TABLE
reports "not a regular table"). The adapter uses the IF EXISTS form so a stale
relation cache or an externally dropped table doesn't fail the run, mirroring
drop_relation_if_exists. Errors propagate unmodified.
"""

from unittest.mock import MagicMock

import pytest
from confluent_sql.exceptions import OperationalError

from dbt.adapters.confluent.impl import ConfluentAdapter


class TestDropMaterializedTable:
    @pytest.fixture
    def adapter(self):
        a = ConfluentAdapter.__new__(ConfluentAdapter)
        a.execute = MagicMock()
        return a

    def test_issues_drop_materialized_table_if_exists_as_streaming_ddl(self, adapter):
        adapter.drop_materialized_table("my_mt")
        adapter.execute.assert_called_once()
        sql = adapter.execute.call_args.args[0]
        assert "DROP MATERIALIZED TABLE IF EXISTS" in sql
        assert "my_mt" in sql
        assert adapter.execute.call_args.kwargs.get("execution_mode") == "streaming_ddl"

    def test_errors_propagate(self, adapter):
        err = OperationalError("Could not execute DropTable in path env.db.my_mt")
        adapter.execute.side_effect = err
        with pytest.raises(OperationalError) as exc_info:
            adapter.drop_materialized_table("my_mt")
        assert exc_info.value is err
