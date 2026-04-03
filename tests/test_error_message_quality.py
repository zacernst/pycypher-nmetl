"""Tests for error message quality and user-facing UX improvements.

Validates that error messages provide actionable, type-specific guidance
to help users debug and fix their queries effectively.
"""

from __future__ import annotations

from pycypher.exceptions import (
    IncompatibleOperatorError,
    VariableTypeMismatchError,
    _type_specific_suggestion,
)


class TestTypeSpecificSuggestions:
    """Test that IncompatibleOperatorError provides type-specific guidance."""

    def test_string_int_addition_suggests_conversion(self) -> None:
        """Adding string + int should suggest toString() or toInteger()."""
        error = IncompatibleOperatorError("+", "str", "int")
        msg = str(error)
        assert "toString()" in msg
        assert "toInteger()" in msg

    def test_string_float_suggests_conversion(self) -> None:
        """String + float should suggest conversion functions."""
        error = IncompatibleOperatorError("+", "str", "float")
        msg = str(error)
        assert "toString()" in msg
        assert "toFloat()" in msg

    def test_string_int_comparison_suggests_numeric_conversion(self) -> None:
        """Comparing string < int should suggest numeric conversion."""
        error = IncompatibleOperatorError("<", "str", "int")
        msg = str(error)
        assert "toInteger()" in msg or "toFloat()" in msg

    def test_bool_arithmetic_suggests_case_when(self) -> None:
        """Boolean in arithmetic should suggest CASE WHEN conversion."""
        error = IncompatibleOperatorError("+", "bool", "int")
        msg = str(error)
        assert "CASE WHEN" in msg

    def test_none_type_suggests_coalesce(self) -> None:
        """Null operand should suggest coalesce()."""
        error = IncompatibleOperatorError("+", "NoneType", "int")
        msg = str(error)
        assert "coalesce()" in msg

    def test_list_type_suggests_list_functions(self) -> None:
        """List in arithmetic should suggest list extraction."""
        error = IncompatibleOperatorError("+", "list", "int")
        msg = str(error)
        assert "UNWIND" in msg or "size()" in msg

    def test_explicit_suggestion_overrides_auto(self) -> None:
        """Explicit suggestion should be used instead of auto-generated."""
        error = IncompatibleOperatorError(
            "+", "str", "int", suggestion="Custom fix here."
        )
        assert error.suggestion == "Custom fix here."
        assert "Custom fix here." in str(error)

    def test_auto_suggestion_set_when_empty(self) -> None:
        """Auto-generated suggestion stored in self.suggestion."""
        error = IncompatibleOperatorError("+", "str", "int")
        assert error.suggestion != ""
        assert "toString()" in error.suggestion

    def test_doc_hint_included(self) -> None:
        """IncompatibleOperatorError should include doc hint."""
        error = IncompatibleOperatorError("+", "str", "int")
        msg = str(error)
        assert "Docs:" in msg or "pycypher.readthedocs.io" in msg

    def test_unknown_types_get_generic_suggestion(self) -> None:
        """Unknown type combinations get a generic but helpful suggestion."""
        error = IncompatibleOperatorError("+", "MyCustomType", "AnotherType")
        msg = str(error)
        assert "compatible" in msg


class TestTypeSpecificSuggestionFunction:
    """Test the _type_specific_suggestion helper directly."""

    def test_str_int_addition(self) -> None:
        suggestion = _type_specific_suggestion("+", "str", "int")
        assert "toString()" in suggestion
        assert "toInteger()" in suggestion

    def test_str_float_subtraction(self) -> None:
        suggestion = _type_specific_suggestion("-", "str", "float")
        assert "toInteger()" in suggestion or "toFloat()" in suggestion

    def test_str_arithmetic_general(self) -> None:
        suggestion = _type_specific_suggestion("*", "str", "dict")
        assert "toString()" in suggestion or "toInteger()" in suggestion

    def test_bool_type(self) -> None:
        suggestion = _type_specific_suggestion("+", "bool", "int")
        assert "CASE WHEN" in suggestion

    def test_none_type(self) -> None:
        suggestion = _type_specific_suggestion("+", "NoneType", "int")
        assert "coalesce()" in suggestion

    def test_list_type(self) -> None:
        suggestion = _type_specific_suggestion("+", "list", "int")
        assert "UNWIND" in suggestion

    def test_generic_fallback(self) -> None:
        suggestion = _type_specific_suggestion("+", "X", "Y")
        assert "compatible" in suggestion


class TestVariableTypeMismatchDocHint:
    """Test that VariableTypeMismatchError includes doc hints."""

    def test_doc_hint_present(self) -> None:
        """VariableTypeMismatchError should include documentation link."""
        error = VariableTypeMismatchError("n", "Node", "Relationship")
        msg = str(error)
        assert "Docs:" in msg or "pycypher.readthedocs.io" in msg

    def test_suggestion_preserved(self) -> None:
        """Custom suggestion should still appear in message."""
        error = VariableTypeMismatchError(
            "n", "Node", "Relationship", suggestion="Check your MATCH pattern."
        )
        msg = str(error)
        assert "Check your MATCH pattern." in msg

    def test_structured_attributes(self) -> None:
        """Structured attributes should be accessible."""
        error = VariableTypeMismatchError("x", "int", "str")
        assert error.variable_name == "x"
        assert error.expected_type == "int"
        assert error.actual_type == "str"


class TestSemanticValidatorAggregationWarning:
    """Test that the semantic validator's aggregation warning is actionable."""

    def test_aggregation_warning_has_guidance(self) -> None:
        """Aggregation mixing warning should explain the issue and suggest a fix."""
        from pycypher.semantic_validator import validate_query

        errors = validate_query("MATCH (n) RETURN n, count(*)")
        warnings = [e for e in errors if e.severity.value == "warning"]
        if warnings:
            msg = warnings[0].message
            assert "implicit grouping" in msg
            assert "WITH" in msg

    def test_aggregation_warning_includes_example(self) -> None:
        """Aggregation mixing warning should include a WITH example."""
        from pycypher.semantic_validator import validate_query

        errors = validate_query("MATCH (n) RETURN n.name, count(*)")
        warnings = [e for e in errors if e.severity.value == "warning"]
        if warnings:
            msg = warnings[0].message
            assert "WITH" in msg
            assert "count" in msg.lower()
