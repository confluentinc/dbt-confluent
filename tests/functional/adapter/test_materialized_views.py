import pytest

from dbt.tests.util import run_dbt
from tests.functional.adapter.fixtures import ConfluentFixtures

MY_MV_MODEL = """
{{ config(materialized='materialized_view') }}
select 1 as id
"""


class TestMaterializedViewNotSupported(ConfluentFixtures):
    """materialized_view should raise a compilation error directing users to use table instead."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_mv.sql": MY_MV_MODEL}

    def test_materialized_view_raises_error(self, project):
        result = run_dbt(["run"], expect_pass=False)
        assert len(result) == 1
        assert result[0].status == "error"
