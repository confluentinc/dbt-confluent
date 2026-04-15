"""Unit tests for statement name sanitization."""

import pytest

from dbt.adapters.confluent.naming import (
    MAX_STATEMENT_NAME_LENGTH,
    sanitize_statement_name,
)


class TestSanitizeStatementName:
    def test_clean_name_unchanged(self):
        assert sanitize_statement_name("dbt-myproject-my-model") == "dbt-myproject-my-model"

    def test_lowercased(self):
        assert sanitize_statement_name("dbt-MyProject-Model") == "dbt-myproject-model"

    def test_underscores_replaced_with_hash(self):
        result = sanitize_statement_name("dbt-proj-my_model")
        assert "_" not in result
        assert result.startswith("dbt-proj-my-model-")
        assert len(result.split("-")[-1]) == 6  # 6-char hash

    def test_dots_replaced_with_hash(self):
        result = sanitize_statement_name("dbt-proj-my.model")
        assert "." not in result
        assert result.startswith("dbt-proj-my-model-")

    def test_different_originals_get_different_hashes(self):
        """Two names that sanitize to the same base must get different hashes."""
        a = sanitize_statement_name("dbt-a.b")
        b = sanitize_statement_name("dbt-a_b")
        assert a != b

    def test_same_input_is_stable(self):
        name = "dbt-proj-model_v2"
        assert sanitize_statement_name(name) == sanitize_statement_name(name)

    def test_no_collision_with_clean_name(self):
        """A name with illegal chars should not collide with its hyphen equivalent."""
        dirty = sanitize_statement_name("dbt-a.b")
        clean = sanitize_statement_name("dbt-a-b")
        assert dirty != clean

    def test_leading_hyphens_stripped(self):
        result = sanitize_statement_name("--dbt-model")
        assert result[0].isalnum()

    def test_leading_illegal_chars_stripped(self):
        result = sanitize_statement_name("__dbt-model")
        assert result[0].isalnum()

    def test_truncation_when_too_long(self):
        long_name = "dbt-" + "a" * 120
        result = sanitize_statement_name(long_name)
        assert len(result) <= MAX_STATEMENT_NAME_LENGTH

    def test_truncation_preserves_hash(self):
        long_name = "dbt-" + "a" * 120
        result = sanitize_statement_name(long_name)
        parts = result.rsplit("-", 1)
        assert len(parts) == 2
        assert len(parts[1]) == 6  # 6-char hash for truncation

    def test_truncation_with_illegal_chars(self):
        long_name = "dbt-" + "a_b" * 50
        result = sanitize_statement_name(long_name)
        assert len(result) <= MAX_STATEMENT_NAME_LENGTH
        assert all(c in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in result)

    def test_exactly_max_length_not_truncated(self):
        name = "a" * MAX_STATEMENT_NAME_LENGTH
        result = sanitize_statement_name(name)
        assert result == name
        assert len(result) == MAX_STATEMENT_NAME_LENGTH

    def test_one_over_max_truncated(self):
        name = "a" * (MAX_STATEMENT_NAME_LENGTH + 1)
        result = sanitize_statement_name(name)
        assert len(result) <= MAX_STATEMENT_NAME_LENGTH

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            sanitize_statement_name("")

    def test_spaces_replaced(self):
        result = sanitize_statement_name("dbt-my model")
        assert " " not in result

    def test_realistic_name(self):
        """Test a realistic dbt model name with underscores."""
        result = sanitize_statement_name("dbt-my_project-stg_orders_v2")
        assert "_" not in result
        assert result[0].isalnum()
        assert len(result) <= MAX_STATEMENT_NAME_LENGTH

    def test_suffix_preserved(self):
        result = sanitize_statement_name("dbt-myproject-mymodel-ddl")
        assert result == "dbt-myproject-mymodel-ddl"
