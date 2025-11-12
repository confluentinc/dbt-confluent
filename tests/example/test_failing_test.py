import os
import pytest
from dbt.tests.util import run_dbt


# seeds/my_seed.csv
my_seed_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
""".lstrip()

# models/my_model.sql
my_model_sql = """
select * from {{ ref('my_seed') }}
union all
select null as id, null as name, null as some_date
"""

# models/my_model.yml
my_model_yml = """
version: 2
models:
  - name: my_model
    columns:
      - name: id
        data_tests:
          - unique
          - not_null  # this test will fail
"""


class TestExample:
    """
    Methods in this class will be of two types:
    1. Fixtures defining the dbt "project" for this test case.
       These are scoped to the class, and reused for all tests in the class.
    2. Actual tests, whose names begin with 'test_'.
       These define sequences of dbt commands and 'assert' statements.
    """


    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix):
        """
        Overrides the dbt-tests `schema` fixture.
        We don't want a unique schema, we want to use
        our pre-configured 'dbt_adapter' schema.
        """
        return "dbt_adapter"

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # return {
        #     "name": "example",
        #     "models": {"+materialized": "view"},
        #     # "seeds": {"full_refresh": True},
        # }

        return {
            "name": "example",
            "models": {
                "+materialized": "view",
                "+schema": unique_schema,  # <-- 2. Apply the schema to models
            },
            "seeds": {
                "+schema": unique_schema,  # <-- 2. Apply the schema to seeds
            },
        }

    # everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": my_seed_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model.yml": my_model_yml,
        }

    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_run_seed_test(self, project, caplog):
        """
        Seed, then run, then test. We expect one of the tests to fail
        An alternative pattern is to use pytest "xfail" (see below)
        """
        caplog.set_level("INFO")
        # seed seeds
        print("RUNNING SEED")
        results = run_dbt(["seed"])
        assert len(results) == 1
        # run models
        print("RUNNING RUN")
        results = run_dbt(["run"])
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=False)  # expect failing test
        assert len(results) == 2
        # validate that the results include one pass and one failure
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["fail", "pass"]

    # @pytest.mark.xfail
    # def test_build(self, project):
    #     """Expect a failing test"""
    #     # do it all
    #     _results = run_dbt(["build"])
