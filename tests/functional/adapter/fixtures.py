import logging
from textwrap import dedent
from uuid import uuid4

import pytest

logger = logging.getLogger(__name__)


class ConfluentFixtures:
    @pytest.fixture(scope="class")
    def dbt_profile_data(self, unique_schema, dbt_profile_target, profiles_config_update):
        # We add a unique `statement_name_prefix` to each test class
        # so that we can cleanup all created statements at the end.
        profile = {
            "test": {
                "outputs": {
                    "default": {
                        **dbt_profile_target,
                        "statement_name_prefix": f"dbt-test-{uuid4()}-"
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
            },
            "seeds": {
                "+schema": unique_schema,
                "+full_refresh": True,
            },
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project, dbt_profile_data):
        """
        This adapter does not support creating and dropping whole schemas.
        After each test, we clean up any lingering statements (e.g. CTAS
        streaming jobs) created during the test to free compute pool resources.
        """
        yield
        prefix = dbt_profile_data["test"]["outputs"]["default"]["statement_name_prefix"]
        if not prefix:
            logger.warning(
                "No statement_name_prefix set in profile target. "
                "Skipping statement cleanup — lingering statements may remain."
            )
            return
        with project.adapter.connection_named("cleanup"):
            conn = project.adapter.connections.get_thread_connection()
            conn.handle.cleanup_statements(prefix=prefix)
