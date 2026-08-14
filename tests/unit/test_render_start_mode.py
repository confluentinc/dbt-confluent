"""Unit tests for ConfluentAdapter.render_start_mode.

The helper is the single validate-and-render step for the `start_mode` config
of the materialized_table materialization: it accepts the eight documented
Confluent forms as plain strings, normalizes the keyword to uppercase and
enforces its arity, and passes any parenthesized argument through verbatim
after a lexical check (quoted literals and bare words only — the server
validates the argument's structure, e.g. that FROM_NOW gets an interval
literal like INTERVAL '7' DAY). The materialization trusts its output, so a
gap here renders broken (or injectable) DDL.
"""

import pytest
from dbt_common.exceptions import CompilationError

from dbt.adapters.confluent.impl import ConfluentAdapter


@pytest.fixture
def adapter():
    # bypass __init__ — the helper only needs the method dispatch
    return ConfluentAdapter.__new__(ConfluentAdapter)


class TestRenderStartMode:
    def test_none_renders_empty(self, adapter):
        """An unset `start_mode` is the common case — renders nothing."""
        assert adapter.render_start_mode(None) == ""

    @pytest.mark.parametrize(
        "value, expected",
        [
            # the eight documented forms
            ("FROM_BEGINNING", "FROM_BEGINNING"),
            ("FROM_NOW", "FROM_NOW"),
            ("RESUME_OR_FROM_BEGINNING", "RESUME_OR_FROM_BEGINNING"),
            ("RESUME_OR_FROM_NOW", "RESUME_OR_FROM_NOW"),
            ("FROM_NOW(INTERVAL '1' HOUR)", "FROM_NOW(INTERVAL '1' HOUR)"),
            ("RESUME_OR_FROM_NOW(INTERVAL '7' DAY)", "RESUME_OR_FROM_NOW(INTERVAL '7' DAY)"),
            (
                "FROM_TIMESTAMP('2024-01-01 00:00:00')",
                "FROM_TIMESTAMP('2024-01-01 00:00:00')",
            ),
            (
                "RESUME_OR_FROM_TIMESTAMP('2024-01-01 00:00:00')",
                "RESUME_OR_FROM_TIMESTAMP('2024-01-01 00:00:00')",
            ),
        ],
        ids=[
            "from_beginning",
            "from_now",
            "resume_or_from_beginning",
            "resume_or_from_now",
            "from_now_interval",
            "resume_or_from_now_interval",
            "from_timestamp",
            "resume_or_from_timestamp",
        ],
    )
    def test_documented_forms_render_verbatim(self, adapter, value, expected):
        assert adapter.render_start_mode(value) == expected

    @pytest.mark.parametrize(
        "value, expected",
        [
            # keyword case and surrounding whitespace are normalized
            ("from_beginning", "FROM_BEGINNING"),
            ("  resume_or_from_now  ", "RESUME_OR_FROM_NOW"),
            ("from_timestamp('2024-01-01 00:00:00')", "FROM_TIMESTAMP('2024-01-01 00:00:00')"),
            # whitespace around parens/argument is tolerated
            ("FROM_NOW ( INTERVAL '1' HOUR )", "FROM_NOW(INTERVAL '1' HOUR)"),
            # empty parens collapse to the bare keyword
            ("FROM_NOW()", "FROM_NOW"),
        ],
        ids=[
            "lowercase_keyword",
            "surrounding_whitespace",
            "lowercase_parameterized",
            "spaced_argument",
            "empty_parens",
        ],
    )
    def test_input_is_normalized(self, adapter, value, expected):
        assert adapter.render_start_mode(value) == expected

    @pytest.mark.parametrize(
        "value",
        [
            # a plain quoted string is lexically fine — the server decides
            # whether it's an acceptable argument for the keyword
            "FROM_NOW('1 h')",
            # bare words without quotes pass through untouched too
            "FROM_NOW(1 h)",
            # a well-formed literal with escaped quotes is preserved verbatim
            "FROM_NOW('it''s')",
        ],
        ids=["quoted_string", "bare_words", "doubled_quotes"],
    )
    def test_lexically_valid_arguments_pass_through_verbatim(self, adapter, value):
        assert adapter.render_start_mode(value) == value

    @pytest.mark.parametrize(
        "value, expected_substring",
        [
            # unknown keyword
            ("LATEST", "not a valid value for 'start_mode'"),
            ("EARLIEST('1 h')", "not a valid value for 'start_mode'"),
            # non-string config values stringify into the same rejection
            (True, "not a valid value for 'start_mode'"),
            (42, "not a valid value for 'start_mode'"),
            # malformed argument: anything beyond quoted literals and bare
            # words could break out of the rendered DDL, so the whole value
            # is rejected
            ("FROM_NOW('1 h' OR '1'='1')", "not a valid value for 'start_mode'"),
            ("FROM_TIMESTAMP('unterminated)", "not a valid value for 'start_mode'"),
            ("FROM_NOW('1 h') garbage", "not a valid value for 'start_mode'"),
            ("FROM_NOW(INTERVAL '1' HOUR); DROP TABLE t", "not a valid value for 'start_mode'"),
            ("FROM_NOW(INTERVAL ('1') HOUR)", "not a valid value for 'start_mode'"),
            # arity violations
            ("FROM_BEGINNING('1 h')", "does not take an argument"),
            ("RESUME_OR_FROM_BEGINNING('1 h')", "does not take an argument"),
            ("FROM_TIMESTAMP", "requires an argument"),
            ("FROM_TIMESTAMP()", "requires an argument"),
            ("RESUME_OR_FROM_TIMESTAMP('')", "requires an argument"),
        ],
        ids=[
            "unknown_keyword",
            "unknown_keyword_with_arg",
            "bool_value",
            "int_value",
            "quote_injection",
            "unterminated_literal",
            "trailing_garbage",
            "statement_injection",
            "nested_parens",
            "forbidden_arg",
            "forbidden_arg_resume",
            "missing_required_arg",
            "empty_parens_required_arg",
            "empty_literal_required_arg",
        ],
    )
    def test_invalid_values_raise(self, adapter, value, expected_substring):
        with pytest.raises(CompilationError) as exc_info:
            adapter.render_start_mode(value)
        assert expected_substring in str(exc_info.value)
