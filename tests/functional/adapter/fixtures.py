import logging
import os
import re
import time
from textwrap import dedent
from uuid import uuid4

import pytest

from tests.functional.adapter._helpers import (
    ISOLATION_SLUG_ENV_VAR,
    delete_statements_by_label,
    drop_tables,
)

logger = logging.getLogger(__name__)

# Test modules run concurrently (pytest-xdist, --dist=loadfile) against one shared
# schema, so two modules that both define e.g. `my_streaming_table` would clobber each
# other's relations and statements. Every module therefore gets its own namespace,
# derived from the module name, applied centrally (no per-test naming needed):
# - relations: this macro prefixes every model's alias; env var set by `relation_namespace`
# - statements: `statement_name_prefix` in the profile (see dbt_profile_data)
# Tests that refer to a relation by literal name use `_helpers.physical_name`.
GENERATE_ALIAS_NAME_MACRO = f"""
{{% macro generate_alias_name(custom_alias_name=none, node=none) -%}}
    {{{{- env_var('{ISOLATION_SLUG_ENV_VAR}', '') -}}}}
    {{{{- adapter.dispatch('generate_alias_name', 'dbt')(custom_alias_name, node) | trim -}}}}
{{%- endmacro %}}
"""

# Names are never reused across sessions: Kafka topic deletion is asynchronous, so a table
# dropped by the previous run can still be half-deleted ("found, but its Kafka topic does not
# exist") when a new run recreates the same name. Computed per worker process, which is fine:
# a module only ever runs on one worker.
_SESSION_TAG = format(int(time.time()), "08x")


def _module_tag(request: pytest.FixtureRequest) -> str:
    """`tests.functional.adapter.test_schema_drift` -> `test_schema_drift`."""
    name = request.module.__name__.rsplit(".", 1)[-1]
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


class ConfluentFixtures:
    # Set False in classes whose relation names are already globally unique (or are fixed
    # by dbt-tests-adapter base classes); they keep their literal model names.
    NAMESPACE_RELATIONS = True

    @pytest.fixture(scope="class")
    def module_tag(self, request: pytest.FixtureRequest) -> str:
        return _module_tag(request)

    @pytest.fixture(scope="class")
    def macros(self) -> dict[str, str]:
        return {"generate_alias_name.sql": GENERATE_ALIAS_NAME_MACRO}

    @pytest.fixture(scope="class")
    def relation_namespace(self, module_tag: str):
        """Publish this module's relation prefix for the generate_alias_name macro.

        Required by `dbt_profile_data`, hence by `project`, so the prefix is in place before
        anything (including a class-scoped autouse fixture) first invokes dbt."""
        previous = os.environ.get(ISOLATION_SLUG_ENV_VAR)
        os.environ[ISOLATION_SLUG_ENV_VAR] = (
            f"dbt_{module_tag}__{_SESSION_TAG}__" if self.NAMESPACE_RELATIONS else ""
        )
        yield
        if previous is None:
            os.environ.pop(ISOLATION_SLUG_ENV_VAR, None)
        else:
            os.environ[ISOLATION_SLUG_ENV_VAR] = previous

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self,
        unique_schema,
        dbt_profile_target,
        profiles_config_update,
        module_tag,
        relation_namespace,
    ):
        # The test-suite-reserved `statement_name_prefix` carries the module tag, so concurrent
        # modules never share a statement name. It must start with `dbt-adapter-test-` (what the
        # sweepers match on), which every module tag's `test_` provides; hyphens only, since the
        # driver uses it verbatim in statement names. A unique `statement_label` per class lets
        # us clean up all created statements.
        profile = {
            "test": {
                "outputs": {
                    "default": {
                        **dbt_profile_target,
                        "statement_name_prefix": f"dbt-adapter-{module_tag.replace('_', '-')}-",
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

    @pytest.fixture(autouse=True)
    def clean_up(self, project, dbt_profile_data):
        """
        This adapter does not support creating and dropping whole schemas.
        After each test, we clean up any lingering statements (e.g. CTAS
        streaming jobs) created during the test to free compute pool resources.
        """
        yield

        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        if not label:
            logger.warning(
                "No statement_label set in profile target. "
                "Skipping statement cleanup — lingering statements may remain."
            )
            return
        delete_statements_by_label(project, label)


class ClassScopedCleanup(ConfluentFixtures):
    """For tests that manipulate statements inside the test body and want
    teardown once at class scope rather than after every test.

    Subclasses set TABLES to the model relation names to drop. The default
    per-test clean_up only deletes statements (it can't drop schemas); these
    tests additionally need their tables dropped, which class_clean_up does.
    """

    TABLES: list[str] = []

    @pytest.fixture(autouse=True)
    def clean_up(self, project, dbt_profile_data):
        # Suppress the per-test statement cleanup; class_clean_up handles teardown.
        yield

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        delete_statements_by_label(project, label)
        drop_tables(project, *self.TABLES)
