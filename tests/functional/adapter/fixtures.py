from textwrap import dedent

import pytest


class ConfluentFixtures:
    """
    Use this class to replace dbt tests' fixtures with custom ones that work with this adapter.
    """

    def get_project_config_update(self, name, schema):
        return {
            "name": name,
            "models": {
                # "+materialized": "view",
                # Here we need to specify the schema, or tests will
                # receive a Relation with an empty string as default.
                "+schema": schema,
            },
            "seeds": {
                # Here we need to specify the schema, or tests will
                # receive a Relation with an empty string as default.
                "+schema": schema,
                "+full_refresh": True,
            },
        }

    def _drop_relation_if_exists(self, project, identifier):
        """Helper to drop a relation if it exists."""
        with project.adapter.connection_named("__test_cleanup"):
            existing = project.adapter.get_relation(
                database=project.database, schema=project.test_schema, identifier=identifier
            )
            if existing:
                try:
                    project.adapter.drop_relation(existing)
                except Exception as e:
                    print(f"Failed to drop {identifier}: {e}")

    def get_relations_to_cleanup(self):
        return []

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        """
        Since we can't drop schemas, we need to manually drop specific tables/views
        created by each test to ensure a clean state for the next test.

        Each test class should override get_relations_to_cleanup() to specify
        what it creates.
        """
        yield
        # Drop known test relations - each test can override this list
        for identifier in self.get_relations_to_cleanup():
            self._drop_relation_if_exists(project, identifier)


    @pytest.fixture(scope="class")
    def schema_yml(self, unique_schema):
        return dedent(f"""
            version: 2
            sources:
              - name: raw
                schema: "{unique_schema}"
                tables:
                  - name: seed
                    identifier: "{{{{ var('seed_name', 'base') }}}}"
        """)
