"""Suite-wide pytest setup. Fixtures and hooks specific to one suite live in tests/unit/ or
tests/functional/ (see tests/functional/conftest.py); only what must be global is here:
`pytest_plugins` and command line options (pytest only honors those at the root), and the hook
that marks each test `unit` or `functional` by directory."""

import pytest

# Import the dbt functional fixtures as a plugin (only functional tests use them).
# Note: fixtures with session scope need to be local, and `pytest_plugins` is only allowed here.

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--keep-failed-resources",
        action="store_true",
        default=False,
        help="Skip the end-of-class sweep of relations and statements for functional test "
        "classes with a failed test, so the failure can be inspected on the cluster.",
    )


_SUITE_MARKERS = {"unit": pytest.mark.unit, "functional": pytest.mark.functional}


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Mark each test `unit` or `functional` by the top-level tests/ directory it lives in, so
    `pytest -m unit` / `pytest -m functional` select a suite. A test outside both is an error
    rather than silently escaping both selections."""
    tests_dir = config.rootpath / "tests"
    for item in items:
        suite = item.path.relative_to(tests_dir).parts[0]
        if suite not in _SUITE_MARKERS:
            raise pytest.UsageError(
                f"{item.nodeid} must live under tests/unit/ or tests/functional/ "
                "so that it is marked `unit` or `functional`."
            )
        item.add_marker(_SUITE_MARKERS[suite])
