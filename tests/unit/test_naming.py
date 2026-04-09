"""Unit tests for statement name sanitization and validation."""

import pytest

from dbt.adapters.confluent.naming import (
    MAX_STATEMENT_NAME_LENGTH,
    sanitize_statement_name,
    validate_statement_name,
)


class TestSanitizeStatementName:
    def test_clean_name_unchanged(self):
        assert sanitize_statement_name("dbt-myproject-my_model") == "dbt-myproject-my_model"

    def test_alphanumeric_and_hyphens_preserved(self):
        assert sanitize_statement_name("abc-123_DEF") == "abc-123_DEF"

    def test_dots_replaced_with_hash(self):
        result = sanitize_statement_name("dbt-proj-my.model")
        assert "." not in result
        # Should have a hash suffix to avoid collisions
        assert result.startswith("dbt-proj-my_model-")
        assert len(result.split("-")[-1]) == 4  # 4-char hash

    def test_different_originals_get_different_hashes(self):
        """Two names that sanitize to the same base must get different hashes."""
        a = sanitize_statement_name("dbt-a.b")
        b = sanitize_statement_name("dbt-a:b")
        # Both have illegal chars replaced with '_', but originals differ
        # so their hashes should differ
        assert a != b

    def test_same_sanitized_name_is_stable(self):
        """Same input always produces the same output."""
        name = "dbt-proj-model.v2"
        assert sanitize_statement_name(name) == sanitize_statement_name(name)

    def test_no_collision_with_clean_name(self):
        """A name with illegal chars should not collide with its underscore equivalent."""
        dirty = sanitize_statement_name("dbt-a.b")
        clean = sanitize_statement_name("dbt-a_b")
        assert dirty != clean

    def test_truncation_when_too_long(self):
        long_name = "dbt-" + "a" * 100
        result = sanitize_statement_name(long_name)
        assert len(result) <= MAX_STATEMENT_NAME_LENGTH

    def test_truncation_preserves_hash(self):
        long_name = "dbt-" + "a" * 100
        result = sanitize_statement_name(long_name)
        # Should end with a hash suffix
        parts = result.rsplit("-", 1)
        assert len(parts) == 2
        assert len(parts[1]) == 6  # 6-char hash for truncation

    def test_truncation_with_illegal_chars(self):
        """Name with both illegal chars and excessive length."""
        long_name = "dbt-" + "a.b" * 50
        result = sanitize_statement_name(long_name)
        assert len(result) <= MAX_STATEMENT_NAME_LENGTH
        assert all(c in "abcdefghijklmnopqrstuvwxyz0123456789_-" for c in result)

    def test_exactly_72_chars_not_truncated(self):
        name = "a" * 72
        result = sanitize_statement_name(name)
        assert result == name
        assert len(result) == 72

    def test_73_chars_truncated(self):
        name = "a" * 73
        result = sanitize_statement_name(name)
        assert len(result) <= 72

    def test_empty_string(self):
        result = sanitize_statement_name("")
        assert result == ""

    def test_spaces_replaced(self):
        result = sanitize_statement_name("dbt my model")
        assert " " not in result
        assert result.startswith("dbt_my_model-")


class TestValidateStatementName:
    def test_valid_name(self):
        validate_statement_name("dbt-myproject-my_model")

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_statement_name("")

    def test_too_long_raises(self):
        with pytest.raises(ValueError, match="exceeds"):
            validate_statement_name("a" * 73)

    def test_exactly_72_valid(self):
        validate_statement_name("a" * 72)

    def test_illegal_chars_raises(self):
        with pytest.raises(ValueError, match="illegal characters"):
            validate_statement_name("dbt.model")

    def test_spaces_raises(self):
        with pytest.raises(ValueError, match="illegal characters"):
            validate_statement_name("dbt model")
