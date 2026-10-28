"""Per-class namespacing so concurrent functional-test classes never collide.

`TestIsolation` is mixed into `ConfluentFixtures`: it namespaces every relation and statement
a test class creates, and cleans up everything in that namespace afterwards. Tests that refer
to a relation by literal name go through `physical_name`.
"""

import hashlib
import os
import re
import time

import pytest
from confluent_sql import ArtifactNotFoundError

from tests.functional.adapter._helpers import (
    drop_any_relation,
)

# Option (registered in tests/conftest.py) that leaves a class's resources in place when any of
# its tests failed, so the failure can be inspected on the cluster.
KEEP_FAILED_OPTION = "--keep-failed-resources"

KEEP_FAILED_HINT = (
    "This class's relations and statements are swept when the class finishes. To keep them\n"
    "for debugging when a test fails, rerun with:\n"
    f"    pytest {KEEP_FAILED_OPTION} <this test>\n"
    "A later session's sweep reclaims them once they are older than the stale threshold."
)

# Test classes with at least one failed test (in this process: a class runs on one xdist worker).
_FAILED_CLASSES = pytest.StashKey[set]()


def record_failure(item: pytest.Item, report: pytest.TestReport) -> None:
    """Note that `item`'s class has a failed test, and say how to keep its resources.
    Called from the `pytest_runtest_makereport` hook in tests/functional/conftest.py."""
    if not report.failed or item.cls is None:
        return
    item.config.stash.setdefault(_FAILED_CLASSES, set()).add(item.cls)
    if not item.config.getoption("keep_failed_resources"):
        report.sections.append(("Cleanup", KEEP_FAILED_HINT))


# Leftovers younger than this may belong to a session still running on the
# shared cluster (a full suite run takes minutes; two hours is a generous
# margin), so the sweepers below leave them for a later sweep.
SWEEP_MIN_AGE_SECONDS = 2 * 60 * 60

# Env var carrying the per-class relation prefix; see GENERATE_ALIAS_NAME_MACRO.
ISOLATION_SLUG_ENV_VAR = "TEST_WORKER_ISOLATION_SLUG"

# Test modules run concurrently (pytest-xdist, --dist=loadscope) against one shared
# schema, so two classes that both define e.g. `my_streaming_table` would clobber each
# other's relations and statements. Every class therefore gets its own namespace,
# derived from its module and class name, applied centrally (no per-test naming needed):
# - relations: this macro prefixes every model's alias; env var set by `relation_namespace`
# - statements: `statement_name_prefix` in the profile (see dbt_profile_data)
# Tests that refer to a relation by literal name use `physical_name`.
GENERATE_ALIAS_NAME_MACRO = f"""
{{% macro generate_alias_name(custom_alias_name=none, node=none) -%}}
    {{{{- env_var('{ISOLATION_SLUG_ENV_VAR}', '') -}}}}
    {{{{- adapter.dispatch('generate_alias_name', 'dbt')(custom_alias_name, node) | trim -}}}}
{{%- endmacro %}}
"""

# Names are never reused across sessions: Kafka topic deletion is asynchronous, so a table
# dropped by the previous run can still be half-deleted ("found, but its Kafka topic does not
# exist") when a new run recreates the same name. Computed per worker process, which is fine:
# a class only ever runs on one worker.
SESSION_TAG = format(int(time.time()), "08x")

# Length, in hex digits, of the class hash in every relation and statement name.
CLASS_HASH_LENGTH = 6

# Every test relation and statement is named `<reserved prefix><session tag><sep><class hash><sep>`
# followed by the name the test asked for. The reserved prefixes are what the sweep matches on, so
# they must never overlap a name a human or another tool would plausibly pick. The two templates
# are kept side by side so it is easy to see they follow the same pattern. Render them with
# `relation_prefix` / `statement_prefix` and parse them with `STALE_RELATION_RE` /
# `STALE_STATEMENT_RE` (built from the templates), never by formatting them by hand.
RELATION_NAME_PREFIX = "dbt_test_"
STATEMENT_NAME_PREFIX = "dbt-test-"  # hyphens only: the driver uses it verbatim in statement names
RELATION_PREFIX_TEMPLATE = RELATION_NAME_PREFIX + "{session_tag}_{class_hash}_"
STATEMENT_PREFIX_TEMPLATE = STATEMENT_NAME_PREFIX + "{session_tag}-{class_hash}-"


def relation_prefix(*, class_hash: str, session_tag: str = SESSION_TAG) -> str:
    """The prefix of every relation the class `class_hash` creates in session `session_tag`."""
    return RELATION_PREFIX_TEMPLATE.format(class_hash=class_hash, session_tag=session_tag)


def statement_prefix(*, class_hash: str, session_tag: str = SESSION_TAG) -> str:
    """The prefix of every statement the class `class_hash` creates in session `session_tag`."""
    return STATEMENT_PREFIX_TEMPLATE.format(class_hash=class_hash, session_tag=session_tag)


def _prefix_pattern(template: str) -> re.Pattern:
    """A regex matching any rendering of `template`, with the session tag in group `session`."""
    placeholders = {
        "class_hash": f"[0-9a-f]{{{CLASS_HASH_LENGTH}}}",
        "session_tag": "(?P<session>[0-9a-f]{8})",
    }
    literals_and_names = re.split(r"\{(\w+)\}", template)
    return re.compile(
        "^"
        + "".join(
            placeholders[part] if index % 2 else re.escape(part)
            for index, part in enumerate(literals_and_names)
        )
    )


STALE_RELATION_RE = _prefix_pattern(RELATION_PREFIX_TEMPLATE)
STALE_STATEMENT_RE = _prefix_pattern(STATEMENT_PREFIX_TEMPLATE)


def _drop_view(project, name: str) -> None:
    """Views need DROP VIEW, which drop_any_relation (tables / MTs) does not issue."""
    try:
        project.run_sql(f"drop view if exists `{name}`")
    except Exception:
        pass  # give up; a later session's sweep reclaims it


class TestIsolation:
    """Per-class namespacing so concurrent test classes never share a relation or statement.

    Mixed into `ConfluentFixtures`, which consumes `statement_name_prefix` (profile) and
    `alias_macros` (project macros), and requires `relation_namespace` via `dbt_profile_data`."""

    # Set False in classes whose relation names are already globally unique (or are fixed
    # by dbt-tests-adapter base classes); they keep their literal model names.
    NAMESPACE_RELATIONS = True

    @pytest.fixture(scope="class")
    def class_hash(self, request: pytest.FixtureRequest) -> str:
        """Stable hash (`CLASS_HASH_LENGTH` hex digits) of the test class's module and class
        name (`hash()` is salted per process, and xdist workers must agree)."""
        qualified_name = f"{request.module.__name__}.{request.cls.__name__}"
        return hashlib.sha1(qualified_name.encode()).hexdigest()[:CLASS_HASH_LENGTH]

    @pytest.fixture(scope="class")
    def relation_namespace(self, class_hash: str):
        """Publish this class's relation prefix for the generate_alias_name macro, and yield
        it (empty for classes that opt out).

        Required by `dbt_profile_data`, hence by `project`, so the prefix is in place before
        anything (including a class-scoped autouse fixture) first invokes dbt."""
        previous = os.environ.get(ISOLATION_SLUG_ENV_VAR)
        prefix = relation_prefix(class_hash=class_hash) if self.NAMESPACE_RELATIONS else ""
        os.environ[ISOLATION_SLUG_ENV_VAR] = prefix
        yield prefix
        if previous is None:
            os.environ.pop(ISOLATION_SLUG_ENV_VAR, None)
        else:
            os.environ[ISOLATION_SLUG_ENV_VAR] = previous

    @pytest.fixture(scope="class")
    def statement_name_prefix(self, class_hash: str) -> str:
        """The test-suite-reserved `statement_name_prefix`, carrying the class hash so
        concurrent classes never share a statement name, and the session tag so the sweep can
        tell a finished session's statements from a live one's. It must start with
        `STATEMENT_NAME_PREFIX` (what the sweep matches on); hyphens only, since the driver
        uses it verbatim in statement names."""
        return statement_prefix(class_hash=class_hash)

    @pytest.fixture(autouse=True)
    def clean_up(self):
        """Override the builtin cleanup fixture from dbt that attempts to drop schema"""
        pass

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, request, project, dbt_profile_data, class_hash):
        """Once the class is done, sweep this class's resources and any stale ones, unless
        `KEEP_FAILED_OPTION` was given and a test in the class failed.
        Nothing is cleaned between tests: sweeping is slow (it lists the shared catalog), and
        tests in a class may build on each other's relations and statements."""
        yield
        config = request.config
        if config.getoption("keep_failed_resources") and request.cls in config.stash.get(
            _FAILED_CLASSES, ()
        ):
            return
        sweep_test_resources(project, class_hash, _statement_label(dbt_profile_data))

    @pytest.fixture(scope="class")
    def alias_macros(self) -> dict[str, str]:
        """The project macros that apply the relation namespace to every model alias."""
        return {"generate_alias_name.sql": GENERATE_ALIAS_NAME_MACRO}


def physical_name(name: str) -> str:
    """The name of a model's relation in the shared schema: the model name plus the
    current test class's namespace prefix (empty for classes that opt out)."""
    return os.environ.get(ISOLATION_SLUG_ENV_VAR, "") + name


def _statement_label(dbt_profile_data: dict) -> str | None:
    return dbt_profile_data["test"]["outputs"]["default"]["statement_label"]


def sweep_test_resources(
    project,
    class_id: str,
    label: str | None = None,
    include_stale: bool = True,
    min_age: int = SWEEP_MIN_AGE_SECONDS,
) -> None:
    """Drop test relations and delete test statements. A resource is swept if EITHER

    - it is stale (only with `include_stale`), whatever its class: left by a previous session
      (a different session tag) and older than `min_age`, so a failed teardown or
      hard-killed run can't leak it forever (younger ones may belong to a concurrently
      running session); OR
    - it belongs to the test class `class_id` (its hash) in THIS session, whatever its age:
      relations named `relation_prefix(class_hash=class_id)...`, statements named
      `statement_prefix(class_hash=class_id)...` or carrying `label`.

    With `include_stale=False` only the class's own resources are swept.

    Relations are tables, materialized tables and views. Materialized tables go first
    (waiting out their transient establishing state), then views, then the other tables.
    Statements are deleted afterwards, then function artifacts.
    """
    now = time.time()
    class_relation_prefix = relation_prefix(class_hash=class_id)
    class_statement_prefix = statement_prefix(class_hash=class_id)

    def is_stale(pattern: re.Pattern, name: str) -> bool:
        match = pattern.match(name)
        return (
            include_stale
            and match is not None
            and match.group("session") != SESSION_TAG
            and now - int(match.group("session"), 16) >= min_age
        )

    # A schema-drift check's temp table is the namespaced model name behind this prefix.
    schema_check_prefix = project.adapter.generate_schema_check_temp_name("")

    def relation_selected(name: str) -> bool:
        name = name.removeprefix(schema_check_prefix)
        return name.startswith(class_relation_prefix) or is_stale(STALE_RELATION_RE, name)

    views = [
        row[0] for row in project.run_sql("show views", fetch="all") if relation_selected(row[0])
    ]
    tables = [
        row[0]
        for row in project.run_sql("show tables", fetch="all")
        if relation_selected(row[0]) and row[0] not in views
    ]
    for view in views:
        _drop_view(project, view)
    for table in tables:
        drop_any_relation(project, table)

    def statement_selected(name: str) -> bool:
        return name.startswith(class_statement_prefix) or is_stale(STALE_STATEMENT_RE, name)

    with project.adapter.connection_named("sweep"):
        handle = project.adapter.connections.get_thread_connection().handle
        # name_contains is a server-side substring filter; anchor client-side.
        selected = {
            statement.name
            for statement in handle.list_statements(name_contains=STATEMENT_NAME_PREFIX)
            if statement_selected(statement.name)
        }
        if label:
            selected.update(statement.name for statement in handle.list_statements(label=label))
        for name in selected:
            # The adapter helper swallows missing-statement / pool-scoped 403s. Deletion is
            # async and not awaited; cleanup doesn't reuse names.
            project.adapter.delete_statement(name)

    # Function artifacts are named after the dbt project, which carries the relation prefix
    # (see `FunctionFixtures`), so the relation selection applies to them too.
    with project.adapter.connection_named("sweep_artifacts"):
        api = project.adapter.connections.get_thread_handle().get_artifacts_api()
        for artifact in api.list():
            if relation_selected(artifact.display_name):
                try:
                    api.delete(artifact.id)
                except ArtifactNotFoundError:
                    pass  # another sweeper got there first
