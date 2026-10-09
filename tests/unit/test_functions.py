"""Unit tests for the `function` materialization's pure logic in `functions`.

`validate_function_config` gates what reaches `create function` DDL, so every malformed shape must
fail with a readable CompilationError rather than a server-side statement failure. The
describe-parsing and diffing decide whether a re-run is a no-op or a drop and re-create, so a
false "changed" (needless drop) or a false "unchanged" (stale function) both matter.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.confluent import functions
from dbt.adapters.confluent.functions import FunctionState, QualifiedName
from dbt.adapters.confluent.impl import FUNCTION_MATERIALIZATION_FLAG, ConfluentAdapter

CATALOG = "env-1"
DATABASE = "cluster-a"
ARTIFACT_ID = "cfa-abc123"
CLASS_NAME = "com.example.my.TShirtSizingIsSmaller"


class FakeConfig(dict):
    """Stands in for the Jinja config object: anything with `.get`."""


def java_config(**overrides) -> FakeConfig:
    return FakeConfig(
        {"language": "java", "artifact_id": ARTIFACT_ID, "class": CLASS_NAME} | overrides
    )


def validate(config: FakeConfig) -> dict:
    return functions.validate_function_config(config, CATALOG, DATABASE)


def conn(name: str, database: str = DATABASE, catalog: str = CATALOG) -> QualifiedName:
    return QualifiedName(catalog, database, name)


class TestValidateFunctionConfig:
    def test_minimal_java_config(self):
        assert validate(java_config()) == {
            "language": "java",
            "artifact_id": ARTIFACT_ID,
            "class_name": CLASS_NAME,
            "connections": [],
            "connection_names": [],
        }

    def test_python_with_connections(self):
        config = java_config(
            language="PYTHON", connections=["my_external_service"], **{"class": "pkg.mod.fn"}
        )
        assert validate(config) == {
            "language": "python",
            "artifact_id": ARTIFACT_ID,
            "class_name": "pkg.mod.fn",
            "connections": [conn("my_external_service")],
            "connection_names": ["`my_external_service`"],
        }

    def test_explicit_scalar_type_is_accepted(self):
        validate(java_config(type="scalar"))

    @pytest.mark.parametrize(
        "overrides, expected_substring",
        [
            ({"language": None}, "'language' must be one of"),
            ({"language": "sql"}, "'language' must be one of"),
            ({"artifact_id": None}, "'artifact_id' must be"),
            ({"artifact_id": "abc123"}, "'artifact_id' must be"),
            ({"artifact_id": "cfa-"}, "'artifact_id' must be"),
            ({"class": None}, "'class' must be a non-empty string"),
            ({"class": "  "}, "'class' must be a non-empty string"),
            ({"connections": "my_external_service"}, "'connections' must be a list"),
            ({"connections": [""]}, "'connections' must be a list"),
            ({"connections": ["a.b.c.d"]}, "not a valid"),
            ({"connections": ["a..c"]}, "not a valid"),
            ({"type": "table"}, "only scalar functions are supported"),
            ({"type": "aggregate"}, "only scalar functions are supported"),
        ],
    )
    def test_invalid_configs_raise(self, overrides, expected_substring):
        with pytest.raises(CompilationError, match=expected_substring):
            validate(java_config(**overrides))

    def test_collects_every_problem(self):
        with pytest.raises(CompilationError) as exc:
            validate(FakeConfig())
        message = str(exc.value)
        assert "'language'" in message
        assert "'artifact_id'" in message
        assert "'class'" in message


class TestRenderIdentifier:
    @pytest.mark.parametrize(
        "text, expected",
        [
            ("conn", "`conn`"),
            ("other-db.conn", "`other-db`.`conn`"),
            ("`env-1`.`cluster-a`.`conn`", "`env-1`.`cluster-a`.`conn`"),
            ("`odd``name`", "`odd``name`"),
        ],
    )
    def test_render(self, text, expected):
        assert functions.render_identifier(text) == expected

    def test_ddl_gets_identifiers_as_configured_not_qualified(self):
        udf = validate(java_config(connections=["svc", "other-db.svc2"]))
        assert udf["connection_names"] == ["`svc`", "`other-db`.`svc2`"]


class TestParseQualifiedName:
    @pytest.mark.parametrize(
        "text, expected",
        [
            ("conn", conn("conn")),
            ("other-db.conn", conn("conn", database="other-db")),
            ("other-env.other-db.conn", conn("conn", database="other-db", catalog="other-env")),
            ("`env-1`.`cluster-a`.`conn`", conn("conn")),
            ("`odd.name`", conn("odd.name")),
            ("`odd``name`", conn("odd`name")),
        ],
    )
    def test_parse(self, text, expected):
        assert functions.parse_qualified_name(text, CATALOG, DATABASE) == expected

    def test_render_round_trips_awkward_names(self):
        name = conn("odd.`name")
        assert functions.parse_qualified_name(name.render(), "x", "y") == name


def describe_rows(**overrides) -> list[tuple[str, str]]:
    """Rows as `DESCRIBE FUNCTION` returns them, `connections` omitted unless given."""
    info = {
        "is system function": "false",
        "class name": CLASS_NAME,
        "function language": "JAVA",
        "plugin id": ARTIFACT_ID,
        "version id": "latest",
        "return type": "BOOLEAN",
    } | overrides
    return list(info.items())


class TestParseDescribeFunction:
    def test_no_connections_row_means_none(self):
        assert functions.parse_describe_function(describe_rows(), CATALOG, DATABASE) == (
            FunctionState(CLASS_NAME, "java", ARTIFACT_ID, frozenset())
        )

    def test_connections_row(self):
        rows = describe_rows(
            connections="[`env-1`.`cluster-a`.`one`, `env-1`.`cluster-a`.`two`]",
        )
        state = functions.parse_describe_function(rows, CATALOG, DATABASE)
        assert state.connections == {conn("one"), conn("two")}


def configured(**overrides) -> FunctionState:
    return functions.desired_function_state(validate(java_config(**overrides)))


class TestDiffFunctionState:
    def existing(self, **overrides) -> FunctionState:
        return functions.parse_describe_function(describe_rows(**overrides), CATALOG, DATABASE)

    def test_identical_is_empty(self):
        assert functions.diff_function_state(self.existing(), configured()) == []

    def test_bare_connection_name_matches_qualified_describe_output(self):
        existing = self.existing(connections="[`env-1`.`cluster-a`.`svc`]")
        assert functions.diff_function_state(existing, configured(connections=["svc"])) == []

    def test_connection_order_is_irrelevant(self):
        existing = self.existing(connections="[`env-1`.`cluster-a`.`a`, `env-1`.`cluster-a`.`b`]")
        assert functions.diff_function_state(existing, configured(connections=["b", "a"])) == []

    def test_language_case_is_irrelevant(self):
        assert functions.diff_function_state(self.existing(), configured(language="JAVA")) == []

    def test_reports_every_difference(self):
        new_artifact, new_class = "cfa-new999", "com.example.Other"
        changes = functions.diff_function_state(
            self.existing(),
            configured(artifact_id=new_artifact, connections=["svc"], **{"class": new_class}),
        )
        assert len(changes) == 3
        assert any(c.startswith("class_name:") and new_class in c for c in changes)
        assert any(c.startswith("artifact_id:") and new_artifact in c for c in changes)
        assert any(c.startswith("connections:") and "`svc`" in c for c in changes)

    def test_connection_in_another_database_is_a_change(self):
        existing = self.existing(connections="[`env-1`.`cluster-a`.`svc`]")
        changes = functions.diff_function_state(
            existing, configured(connections=["cluster-b.svc"])
        )
        assert [c.split(":")[0] for c in changes] == ["connections"]


def test_adapter_delegates_validation_to_functions_module():
    # bypass __init__ — the wrapper only needs the method dispatch
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)

    class Relation:
        database, schema = CATALOG, DATABASE

    assert adapter.validate_function_config(java_config(), Relation()) == validate(java_config())


class TestPlanFunctionChange:
    """`plan_function_change` infers existence from DESCRIBE's outcome, not a catalog query."""

    FUNCTION_NAME = "is_smaller"
    RELATION = SimpleNamespace(
        database=CATALOG,
        schema=DATABASE,
        identifier=FUNCTION_NAME,
        render=lambda: f"`{CATALOG}`.`{DATABASE}`.`{TestPlanFunctionChange.FUNCTION_NAME}`",
    )

    def execute_whose_describe(self, *, returns=None, raises=None) -> MagicMock:
        return MagicMock(side_effect=raises, return_value=(None, SimpleNamespace(rows=returns)))

    def plan(self, execute: MagicMock):
        return functions.plan_function_change(execute, self.RELATION, validate(java_config()))

    def test_missing_function_is_none(self):
        error = DbtDatabaseError(
            f"Statement 'x' submission failed: Function with the identifier "
            f"'`{self.FUNCTION_NAME}`' doesn't exist."
        )
        assert self.plan(self.execute_whose_describe(raises=error)) is None

    def test_other_describe_failures_are_not_mistaken_for_absence(self):
        error = DbtDatabaseError("Statement 'x' failed: compute pool unavailable")
        with pytest.raises(DbtDatabaseError, match="compute pool unavailable"):
            self.plan(self.execute_whose_describe(raises=error))

    def test_doesnt_exist_about_another_object_is_not_absence(self):
        error = DbtDatabaseError("Catalog 'env-1' doesn't exist.")
        with pytest.raises(DbtDatabaseError):
            self.plan(self.execute_whose_describe(raises=error))

    def test_existing_function_returns_differences(self):
        execute = self.execute_whose_describe(returns=describe_rows())
        assert self.plan(execute) == []
        execute.assert_called_once()


CHANGES = ["artifact_id: 'cfa-old' -> 'cfa-new'"]
FUNCTION = "`env-1`.`cluster-a`.`is_smaller`"


class TestPlanFunctionAction:
    @pytest.mark.parametrize("mode", ["apply", "continue", "fail"])
    def test_absent_function_is_created_whatever_the_mode(self, mode):
        assert functions.plan_function_action(FUNCTION, None, mode) == ("create", None)

    @pytest.mark.parametrize("mode", ["apply", "continue", "fail"])
    def test_unchanged_function_is_left_alone_whatever_the_mode(self, mode):
        assert functions.plan_function_action(FUNCTION, [], mode) == ("unchanged", None)

    @pytest.mark.parametrize(
        "mode, expected_action",
        [("apply", "replace"), ("continue", "keep"), ("fail", "fail")],
    )
    def test_changed_function_follows_on_configuration_change(self, mode, expected_action):
        plan = functions.plan_function_action(FUNCTION, CHANGES, mode)
        assert plan.action == expected_action
        # every mode tells the user what differs and which function it concerns
        assert CHANGES[0] in plan.message
        assert FUNCTION in plan.message

    def test_unknown_mode_raises(self):
        with pytest.raises(CompilationError, match="on_configuration_change"):
            functions.plan_function_action(FUNCTION, CHANGES, "sometimes")


class TestFunctionMaterializationFlag:
    """The `function` materialization is refused unless its behavior flag is enabled."""

    def adapter_with_flag(self, enabled: bool) -> ConfluentAdapter:
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        adapter._behavior = SimpleNamespace(
            **{FUNCTION_MATERIALIZATION_FLAG: SimpleNamespace(no_warn=enabled)}
        )
        return adapter

    def test_flag_is_declared_and_off_by_default(self):
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        (flag,) = [
            f for f in adapter._behavior_flags if f["name"] == FUNCTION_MATERIALIZATION_FLAG
        ]
        assert flag["default"] is False

    def test_disabled_raises_naming_the_flag(self):
        with pytest.raises(CompilationError, match=FUNCTION_MATERIALIZATION_FLAG):
            self.adapter_with_flag(enabled=False).require_function_materialization_enabled()

    def test_enabled_passes(self):
        self.adapter_with_flag(enabled=True).require_function_materialization_enabled()
