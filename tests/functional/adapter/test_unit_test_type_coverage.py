"""Functional coverage proving the unit test materialization's tested-model
dry-run column resolution (get_tested_model_columns -> dry_run.get_raw_columns
+ dry_run.try_get_castable_type) works end to end through a real `dbt test`
invocation - not just in isolation against a bare dry run, which is all
test_dry_run_type_translation.py can prove on its own.

Reuses that file's COLUMN_TYPES manifest (restricted to the types a YAML unit
test fixture can actually target - TypeCase.is_yaml_castable) to build a real
dbt project, with two source/tested-model pairs - one all-nullable, one
all-NOT-NULL - and four unit tests crossing {representative values, all
NULL} x {nullable, NOT NULL}:

  - test_nullable_values: the baseline - every column nullable, every value
    the manifest's representative non-null literal.
  - test_nullable_nulls: same nullable columns, every value NULL instead -
    proves a NULL fixture value casts fine into any nullable type.
  - test_not_null_values: every column declared NOT NULL, values are the
    same non-null literals as the baseline - proves try_get_castable_type's
    nullability stripping (dry_run.py) doesn't accidentally reject a real,
    ordinary NOT NULL column (e.g. a PRIMARY KEY) the way it would if it
    left `NOT NULL` in the CAST target.
  - test_not_null_nulls_expect_failure: every column NOT NULL, but every
    value is NULL anyway - expected to fail, to prove the NOT NULL
    constraint (declared on the source table, not on the CAST) still gets
    enforced somewhere, rather than the missing-constraint CAST silently
    letting a NULL fixture value through.

In every case, the tested model (a bare `select *`) is deliberately never
`dbt run` - only its source is.
"""

import pytest
import yaml

from dbt.tests.util import run_dbt
from tests.functional.adapter.fixtures import ConfluentFixtures
from tests.functional.adapter.test_dry_run_type_translation import COLUMN_TYPES

# INTERVAL/RAW/TIMESTAMP WITH TIME ZONE aren't in COLUMN_TYPES at all (see
# that file), and the constructed types (ARRAY/MULTISET/MAP/ROW) are in it
# but not YAML-castable - a real dbt-confluent user could never write a
# passing unit test fixture for any of these regardless, so there's nothing
# for this test to exercise them with.
CASTABLE_COLUMN_TYPES = {
    name: case for name, case in COLUMN_TYPES.items() if case.is_yaml_castable
}


def _sql_literal(value) -> str:
    """A SQL literal for `value`, for this file's own source-model DDL
    (`CAST(<literal> AS type) AS name`) - distinct from, and not a stand-in
    for, dbt-core's own fixture-value rendering (format_row), which the
    unit tests below exercise for real. Just needs *some* concrete, non-null
    literal per type, so CTAS infers a NOT NULL column from it."""
    if isinstance(value, str):
        return "'{}'".format(value.replace("'", "''"))
    return str(value)


def _source_model_sql(*, not_null: bool) -> str:
    if not_null:
        # A CAST of a concrete, non-null literal is itself non-null -
        # confirmed live earlier in this file's own history - so CTAS
        # infers NOT NULL columns from these without an explicit
        # `NOT NULL` keyword (table materialization here is a plain CTAS,
        # which has no syntax for declaring one anyway).
        projection = ",\n    ".join(
            f"CAST({_sql_literal(case.yaml_value)} AS {case.ddl_type}) AS {name}"
            for name, case in CASTABLE_COLUMN_TYPES.items()
        )
    else:
        projection = ",\n    ".join(
            f"CAST(NULL AS {case.ddl_type}) AS {name}"
            for name, case in CASTABLE_COLUMN_TYPES.items()
        )
    return "{{ config(materialized='table') }}\nselect\n    " + projection


NULLABLE_SOURCE_MODEL = _source_model_sql(not_null=False)
NOT_NULL_SOURCE_MODEL = _source_model_sql(not_null=True)

NULLABLE_TESTED_MODEL = """
{{ config(materialized='table') }}
select * from {{ ref('my_all_types_source_nullable') }}
"""

NOT_NULL_TESTED_MODEL = """
{{ config(materialized='table') }}
select * from {{ ref('my_all_types_source_not_null') }}
"""

# A passthrough model's given input must equal its expected output - each
# spells every castable type's value under its column name, exactly as a
# real dbt-confluent user would write it in a unit test YAML file.
_VALUES_ROW = {name: case.yaml_value for name, case in CASTABLE_COLUMN_TYPES.items()}
_NULL_ROW = dict.fromkeys(CASTABLE_COLUMN_TYPES, None)

UNIT_TEST_YML = yaml.safe_dump(
    {
        "unit_tests": [
            {
                "name": "test_nullable_values",
                "model": "my_all_types_table_nullable",
                "given": [{"input": "ref('my_all_types_source_nullable')", "rows": [_VALUES_ROW]}],
                "expect": {"rows": [_VALUES_ROW]},
            },
            {
                "name": "test_nullable_nulls",
                "model": "my_all_types_table_nullable",
                "given": [{"input": "ref('my_all_types_source_nullable')", "rows": [_NULL_ROW]}],
                "expect": {"rows": [_NULL_ROW]},
            },
            {
                "name": "test_not_null_values",
                "model": "my_all_types_table_not_null",
                "given": [{"input": "ref('my_all_types_source_not_null')", "rows": [_VALUES_ROW]}],
                "expect": {"rows": [_VALUES_ROW]},
            },
            {
                "name": "test_not_null_nulls_expect_failure",
                "model": "my_all_types_table_not_null",
                "given": [{"input": "ref('my_all_types_source_not_null')", "rows": [_NULL_ROW]}],
                "expect": {"rows": [_NULL_ROW]},
            },
        ]
    },
    sort_keys=False,
)


class TestUnitTestAllSupportedTypes(ConfluentFixtures):
    """The tested models (my_all_types_table_nullable/_not_null) are never
    `dbt run` - only their sources are, matching TestUnitTestWithoutPriorRun
    in test_unit_materialization.py."""

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_all_types_source_nullable.sql": NULLABLE_SOURCE_MODEL,
            "my_all_types_table_nullable.sql": NULLABLE_TESTED_MODEL,
            "my_all_types_source_not_null.sql": NOT_NULL_SOURCE_MODEL,
            "my_all_types_table_not_null.sql": NOT_NULL_TESTED_MODEL,
            "unit_test.yml": UNIT_TEST_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def build_sources(self, project):
        run_dbt(
            ["run", "--select", "my_all_types_source_nullable", "my_all_types_source_not_null"]
        )

    @pytest.fixture(scope="class", autouse=True)
    def custom_clean_up(self, project):
        yield
        project.run_sql("drop table if exists my_all_types_source_nullable")
        project.run_sql("drop table if exists my_all_types_source_not_null")

    @pytest.mark.parametrize(
        ("unit_test_name", "expect_pass"),
        [
            ("test_nullable_values", True),
            ("test_nullable_nulls", True),
            ("test_not_null_values", True),
            ("test_not_null_nulls_expect_failure", False),
        ],
    )
    def test_castability_across_nullability(self, project, unit_test_name, expect_pass):
        run_dbt(["test", "--select", unit_test_name], expect_pass=expect_pass)
