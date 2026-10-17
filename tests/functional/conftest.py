import pytest

SERIAL_ONLY_MESSAGE = (
    "{nodeid} is marked `serial`: it cannot run under pytest-xdist (it observes the whole "
    "shared environment, so other workers' relations make it fail).\n"
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
