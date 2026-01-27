from textwrap import dedent

import pytest


class ConfluentFixtures:
    @pytest.fixture(scope="class")
    def project_config_update(request, unique_schema):
        if name := getattr(request, "NAME", None):
            config = {"name": name}
        else:
            config = {}

        # Here we need to specify the schema for both models
        # and seeds, or tests will receive a Relation with an
        # empty string as default.
        return {
            **config,
            "models": {
                "+schema": unique_schema,
                "+full_refresh": True,
            },
            "seeds": {
                "+schema": unique_schema,
                "+full_refresh": True,
            },
        }

    @pytest.fixture(autouse=True)
    def clean_up(project):
        """
        This adapter does not support creating and dropping whole schemas.
        For now this is a no-op.
        We might need to add a way to cleanup specific relations in a test.
        """
        yield
