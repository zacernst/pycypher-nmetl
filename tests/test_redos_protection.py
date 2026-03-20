"""Security regression tests: ReDoS protection for the =~ regex operator.

Verifies that ``_validate_regex_pattern()`` rejects patterns known to cause
catastrophic backtracking while allowing legitimate patterns through.
"""

from __future__ import annotations

import pytest
from pycypher.binding_evaluator import _validate_regex_pattern


class TestReDoSProtection:
    """Patterns that cause catastrophic backtracking must be rejected."""

    @pytest.mark.parametrize(
        "pattern",
        [
            r"(a+)+b",
            r"(a*)+b",
            r"(a+)*b",
            r"(a*)*b",
            r"([a-z]+)+$",
            r"(x+x+)+y",
        ],
        ids=[
            "nested_plus_plus",
            "nested_star_plus",
            "nested_plus_star",
            "nested_star_star",
            "charset_nested_plus",
            "double_plus_nested",
        ],
    )
    def test_rejects_nested_quantifiers(self, pattern: str) -> None:
        """Nested quantifiers like (a+)+ cause exponential backtracking."""
        with pytest.raises(ValueError, match="catastrophic backtracking"):
            _validate_regex_pattern(pattern)

    def test_rejects_quantifier_on_quantifier(self) -> None:
        """Chained quantifiers like a{1,100}{1,100} are rejected (by regex compiler)."""
        with pytest.raises(ValueError, match="Invalid regex|multiple repeat"):
            _validate_regex_pattern(r"a{1,100}{1,100}")

    def test_rejects_overly_long_pattern(self) -> None:
        """Patterns exceeding the length limit are rejected."""
        long_pattern = "a" * 1001
        with pytest.raises(ValueError, match="maximum length"):
            _validate_regex_pattern(long_pattern)

    def test_rejects_invalid_regex(self) -> None:
        """Syntactically invalid regex is rejected."""
        with pytest.raises(ValueError, match="Invalid regex"):
            _validate_regex_pattern(r"[unterminated")

    @pytest.mark.parametrize(
        "pattern",
        [
            r"^[a-z]+$",
            r"\d{3}-\d{4}",
            r"foo|bar|baz",
            r"[A-Z][a-z]*",
            r"hello\s+world",
            r"^.{1,50}$",
            r"(foo)(bar)",
            r"\w+@\w+\.\w+",
        ],
        ids=[
            "simple_charset",
            "digit_pattern",
            "alternation",
            "optional_repeat",
            "whitespace_match",
            "bounded_dot",
            "capturing_groups",
            "email_like",
        ],
    )
    def test_allows_safe_patterns(self, pattern: str) -> None:
        """Legitimate regex patterns must pass validation."""
        _validate_regex_pattern(pattern)  # Should not raise

    def test_allows_max_length_pattern(self) -> None:
        """Pattern exactly at the limit should pass."""
        pattern = "a" * 1000
        _validate_regex_pattern(pattern)  # Should not raise
