"""Unit tests for `functions.validate_function_config`.

The validator gates what reaches `create function` DDL for the `function`
materialization, so every malformed shape must fail with a readable
CompilationError rather than a server-side statement failure.
"""

import pytest
from dbt_common.exceptions import CompilationError

from dbt.adapters.confluent import functions
from dbt.adapters.confluent.impl import ConfluentAdapter

ARTIFACT_ID = "cfa-abc123"
CLASS_NAME = "com.example.my.TShirtSizingIsSmaller"


class FakeConfig(dict):
    """Stands in for the Jinja config object: anything with `.get`."""


def java_config(**overrides) -> FakeConfig:
    return FakeConfig(
        {"language": "java", "artifact_id": ARTIFACT_ID, "class": CLASS_NAME} | overrides
    )


class TestValidateFunctionConfig:
    def test_minimal_java_config(self):
        assert functions.validate_function_config(java_config()) == {
            "language": "java",
            "artifact_id": ARTIFACT_ID,
            "class_name": CLASS_NAME,
            "connections": [],
        }

    def test_python_with_connections(self):
        config = java_config(
            language="PYTHON", connections=["my_external_service"], **{"class": "pkg.mod.fn"}
        )
        assert functions.validate_function_config(config) == {
            "language": "python",
            "artifact_id": ARTIFACT_ID,
            "class_name": "pkg.mod.fn",
            "connections": ["my_external_service"],
        }

    def test_explicit_scalar_type_is_accepted(self):
        functions.validate_function_config(java_config(type="scalar"))

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
            ({"type": "table"}, "only scalar functions are supported"),
            ({"type": "aggregate"}, "only scalar functions are supported"),
        ],
    )
    def test_invalid_configs_raise(self, overrides, expected_substring):
        with pytest.raises(CompilationError, match=expected_substring):
            functions.validate_function_config(java_config(**overrides))

    def test_collects_every_problem(self):
        with pytest.raises(CompilationError) as exc:
            functions.validate_function_config(FakeConfig())
        message = str(exc.value)
        assert "'language'" in message
        assert "'artifact_id'" in message
        assert "'class'" in message


def test_adapter_delegates_to_functions_module():
    # bypass __init__ — the wrapper only needs the method dispatch
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    assert adapter.validate_function_config(java_config()) == functions.validate_function_config(
        java_config()
    )
