import os

import pytest

from tests.functional.adapter.isolation import record_failure

SERIAL_ONLY_MESSAGE = (
    "{nodeid} is marked `serial`: it cannot run under pytest-xdist (it observes the whole "
    "shared environment, so other workers' relations make it fail, or it uses fixed "
    "relation names that concurrent classes would clobber).\n"
    "To run the parallel-safe tests in parallel:\n"
    '    pytest tests/functional -n auto -m "not serial"\n'
    "To run just the serial-only tests (no -n):\n"
    "    pytest tests/functional -m serial"
)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    """Fail `serial` tests immediately, before any fixture or cluster work, when this
    process is an xdist worker."""
    if item.get_closest_marker("serial") and hasattr(item.config, "workerinput"):
        pytest.fail(SERIAL_ONLY_MESSAGE.format(nodeid=item.nodeid), pytrace=False)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    """Track failed classes so `class_clean_up` can honor `--keep-failed-resources`, and
    print how to use it with every failure."""
    report = (yield).get_result()
    record_failure(item, report)


@pytest.fixture(scope="session")
def skip_unless_oauth_tests_enabled() -> None:
    """Gate for tests that need an `auth: oauth` login, which opens a browser and wants a human
    to sign in: skipped unless `RUN_OAUTH_TESTS` is set. Request this (directly, or from a
    fixture that must not run before the gate) in every OAuth test."""
    if not os.getenv("RUN_OAUTH_TESTS"):
        pytest.skip("OAuth tests open a browser to sign in; set RUN_OAUTH_TESTS=1 to run them")
