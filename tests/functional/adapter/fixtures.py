from textwrap import dedent
from uuid import uuid4

import pytest

from tests.functional.adapter.isolation import TestIsolation


class ConfluentFixtures(TestIsolation):
    @pytest.fixture(scope="class")
    def macros(self, alias_macros: dict[str, str]) -> dict[str, str]:
        return alias_macros

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self,
        unique_schema,
        dbt_profile_target,
        profiles_config_update,
        statement_name_prefix,
        relation_namespace,
    ):
        # A unique `statement_label` per class lets us clean up all created statements.
        profile = {
            "test": {
                "outputs": {
                    "default": {
                        **dbt_profile_target,
                        "statement_name_prefix": statement_name_prefix,
                        "statement_label": f"dbt-test-{uuid4()}",
                    },
                },
                "target": "default",
            },
        }

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        if name := getattr(self, "NAME", None):
            config = {"name": name}
        else:
            config = {}

        # Here we need to specify the schema for both models
        # and seeds, or tests will receive a Relation with an
        # empty string as default.
        return {
            **config,
            "tests": {
                "+schema": unique_schema,
            },
            "models": {
                "+schema": unique_schema,
                "+full_refresh": True,
            },
            "seeds": {
                "+schema": unique_schema,
                "+full_refresh": True,
            },
        }
