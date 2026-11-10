"""Unit tests for option_values: how the drift check compares a WITH option value the model sets
with the one INFORMATION_SCHEMA.TABLE_OPTIONS reports."""

import pytest

from dbt.adapters.confluent.option_values import option_values_match
from tests.flink_option_values import FLINK_REPORTED_VALUES


@pytest.mark.parametrize(
    ("expected", "existing"),
    [(written, reported) for _, written, reported in FLINK_REPORTED_VALUES],
)
def test_value_as_flink_reports_it_matches(expected, existing):
    assert option_values_match(expected, existing)


@pytest.mark.parametrize(
    ("expected", "existing"),
    [
        # 'm' is minutes in a duration and mebibytes in a size
        ("60m", "1 h"),
        ("1m", "1 mb"),
        # Whitespace around and inside, as Flink allows
        (" 7  days ", "7 d"),
        # Kept as written
        ("90 min", "90 min"),
        ("#{Name.firstName}", "#{Name.firstName}"),
        # Leading zeros, as Flink reads them
        ("007 d", "7 d"),
        # Below a millisecond is dropped, not rounded to the nearest
        ("999999 ns", "0 ms"),
        ("1999 µs", "1 ms"),
    ],
)
def test_same_value_matches(expected, existing):
    assert option_values_match(expected, existing)


@pytest.mark.parametrize(
    ("expected", "existing"),
    [
        ("8 days", "7 d"),
        ("2 gb", "1 gb"),
        ("delete", "compact"),
        ("forever", "7 d"),  # not a duration
        ("1 gb", "1 h"),  # a size against a duration
        ("1 h", "1 gb"),  # a duration against a size
        ("100 us", "100 ms"),  # not a Flink unit (the micro sign is µ)
        ("2000 µs", "1 ms"),  # a whole millisecond more
        ("#{Name.lastName}", "#{Name.firstName}"),
        # Flink rejects padded enum values
        (" compact", "compact"),
    ],
)
def test_changed_value_does_not_match(expected, existing):
    assert not option_values_match(expected, existing)


@pytest.mark.parametrize(
    ("expected", "existing"),
    [
        ("7.5 d", "7 d"),  # not a whole number
        ("7 d ays", "7 d"),  # space inside the unit
        ("-1", "0 ms"),  # sign
        ("+7 d", "7 d"),  # sign
        ("²", "2 ms"),  # a digit int() rejects
        # Empty
        ("", "0 ms"),
    ],
)
def test_malformed_value_does_not_match(expected, existing):
    """The unit lookup is what rejects these, since anything malformed ends up in the unit."""
    assert not option_values_match(expected, existing)


def test_case_only_change_to_free_text_matches():
    """Known blind spot: case is ignored for any value that isn't a duration or size, so a change
    to free text that only alters case isn't drift. Pinned so a change to it is deliberate."""
    assert option_values_match("#{name.firstName}", "#{Name.firstName}")


def test_duration_or_size_shaped_free_text_change_matches():
    """Known blind spot: a free-text value in the form Flink reports a duration or size in is read
    as one, so changing a faker expression from '1 h' to '60 min' isn't drift. Pinned so a change
    to it is deliberate."""
    assert option_values_match("60 min", "1 h")
    assert option_values_match("1024 kb", "1 mb")
