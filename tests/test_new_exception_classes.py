"""Test the new exception classes defined in Loop 247.

Simple tests to verify that the new exception classes work correctly
before using them in the main TDD refactoring.
"""

from __future__ import annotations


class TestNewExceptionClasses:
    """Test that new exception classes are properly defined and functional."""

    def test_ast_conversion_error(self) -> None:
        """Test ASTConversionError exception class."""
        from pycypher.exceptions import ASTConversionError

        # Basic instantiation
        error = ASTConversionError("Test message")
        assert isinstance(error, ValueError)
        assert str(error).startswith("Test message")

        # With context
        error_with_context = ASTConversionError(
            "Conversion failed",
            query_fragment="MATCH (n) RETURN n",
            node_type="UnknownNode",
        )
        assert error_with_context.query_fragment == "MATCH (n) RETURN n"
        assert error_with_context.node_type == "UnknownNode"
        assert "MATCH (n) RETURN n" in str(error_with_context)

    def test_variable_not_found_error(self) -> None:
        """Test VariableNotFoundError exception class."""
        from pycypher.exceptions import VariableNotFoundError

        error = VariableNotFoundError("missing_var", ["a", "b", "c"])
        assert isinstance(error, ValueError)
        assert error.variable_name == "missing_var"
        assert error.available_variables == ["a", "b", "c"]
        assert "missing_var" in str(error)
        assert "a, b, c" in str(error)

    def test_incompatible_operator_error(self) -> None:
        """Test IncompatibleOperatorError exception class."""
        from pycypher.exceptions import IncompatibleOperatorError

        error = IncompatibleOperatorError(
            "+",
            "string",
            "integer",
            "Use toString()",
        )
        assert isinstance(error, TypeError)
        assert error.operator == "+"
        assert error.left_type == "string"
        assert error.right_type == "integer"
        assert error.suggestion == "Use toString()"
        assert "Use toString()" in str(error)

    def test_temporal_arithmetic_error(self) -> None:
        """Test TemporalArithmeticError exception class."""
        from pycypher.exceptions import TemporalArithmeticError

        error = TemporalArithmeticError("+", "date", "string")
        assert isinstance(
            error,
            TypeError,
        )  # Inherits from IncompatibleOperatorError
        assert error.operator == "+"
        assert error.left_type == "date"
        assert error.right_type == "string"
        assert "duration(" in str(error)  # Should have default example

    def test_unsupported_function_error(self) -> None:
        """Test UnsupportedFunctionError exception class."""
        from pycypher.exceptions import UnsupportedFunctionError

        error = UnsupportedFunctionError(
            "badfunction",
            ["count", "sum", "avg"],
            "aggregation",
        )
        assert isinstance(error, ValueError)
        assert error.function_name == "badfunction"
        assert error.supported_functions == ["count", "sum", "avg"]
        assert error.category == "aggregation"
        assert "badfunction" in str(error)
        assert "count" in str(error)

    def test_missing_parameter_error(self) -> None:
        """Test MissingParameterError exception class."""
        from pycypher.exceptions import MissingParameterError

        error = MissingParameterError("user_id")
        assert isinstance(error, ValueError)
        assert error.parameter_name == "user_id"
        assert "$user_id" in str(error)
        assert "parameters=" in str(error)

    def test_function_argument_error(self) -> None:
        """Test FunctionArgumentError exception class."""
        from pycypher.exceptions import FunctionArgumentError

        error = FunctionArgumentError(
            "percentile",
            2,
            1,
            "expression and percentile value",
        )
        assert isinstance(error, ValueError)
        assert error.function_name == "percentile"
        assert error.expected_args == 2
        assert error.actual_args == 1
        assert "percentile" in str(error)
        assert "expression and percentile" in str(error)

    def test_exception_inheritance(self) -> None:
        """Test that new exceptions inherit from appropriate base classes."""
        from pycypher.exceptions import (
            ASTConversionError,
            IncompatibleOperatorError,
            MissingParameterError,
            TemporalArithmeticError,
            UnsupportedFunctionError,
            VariableNotFoundError,
        )

        # Test inheritance for backward compatibility
        assert issubclass(ASTConversionError, ValueError)
        assert issubclass(VariableNotFoundError, ValueError)
        assert issubclass(IncompatibleOperatorError, TypeError)
        assert issubclass(TemporalArithmeticError, IncompatibleOperatorError)
        assert issubclass(TemporalArithmeticError, TypeError)  # Transitively
        assert issubclass(UnsupportedFunctionError, ValueError)
        assert issubclass(MissingParameterError, ValueError)

    def test_catching_existing_exceptions_still_works(self) -> None:
        """Test that existing catch blocks for ValueError/TypeError still work."""
        from pycypher.exceptions import (
            IncompatibleOperatorError,
            VariableNotFoundError,
        )

        # Should still be catchable as ValueError
        try:
            raise VariableNotFoundError("test", [])
        except ValueError:
            caught_as_valueerror = True
        else:
            caught_as_valueerror = False

        assert caught_as_valueerror

        # Should still be catchable as TypeError
        try:
            raise IncompatibleOperatorError("+", "str", "int")
        except TypeError:
            caught_as_typeerror = True
        else:
            caught_as_typeerror = False

        assert caught_as_typeerror
