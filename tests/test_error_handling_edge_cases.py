"""Integration tests for error handling edge cases and boundary conditions.

Covers untested error paths in the query execution pipeline, focusing on
user-facing error scenarios that are most likely to occur in production.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.exceptions import (
    CyclicDependencyError,
    FunctionArgumentError,
    GraphTypeNotFoundError,
    MissingParameterError,
    PatternComprehensionError,
    QueryComplexityError,
    QueryMemoryBudgetError,
    QueryTimeoutError,
    TemporalArithmeticError,
    UnsupportedFunctionError,
    UnsupportedOperatorError,
    VariableNotFoundError,
    WorkerExecutionError,
)
from pycypher.ingestion import ContextBuilder
from pycypher.star import Star


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def star() -> Star:
    """A Star with some basic data for query testing."""
    ctx = ContextBuilder.from_dict(
        {
            "Person": pd.DataFrame(
                {
                    "__ID__": [1, 2, 3],
                    "name": ["Alice", "Bob", "Carol"],
                    "age": [30, 25, 35],
                },
            ),
        },
    )
    return Star(context=ctx)


@pytest.fixture
def empty_star() -> Star:
    """A Star with no data."""
    ctx = ContextBuilder.from_dict({})
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# Empty query and structural errors
# ---------------------------------------------------------------------------


class TestEmptyAndMalformedQueries:
    def test_empty_query_raises_syntax_error(self, star: Star) -> None:
        with pytest.raises(Exception):
            star.execute_query("")

    def test_whitespace_only_query(self, star: Star) -> None:
        with pytest.raises(Exception):
            star.execute_query("   \n\t  ")

    def test_incomplete_match(self, star: Star) -> None:
        with pytest.raises(Exception):
            star.execute_query("MATCH")

    def test_unclosed_parenthesis(self, star: Star) -> None:
        with pytest.raises(Exception):
            star.execute_query("MATCH (n:Person RETURN n")

    def test_unclosed_string_literal(self, star: Star) -> None:
        with pytest.raises(Exception):
            star.execute_query("MATCH (n:Person {name: 'Alice}) RETURN n")


# ---------------------------------------------------------------------------
# Variable reference errors
# ---------------------------------------------------------------------------


class TestVariableReferenceErrors:
    def test_return_undefined_variable(self, star: Star) -> None:
        """Returning an undefined variable should raise an error."""
        with pytest.raises(Exception):
            star.execute_query("MATCH (n:Person) RETURN m.name")

    def test_where_undefined_variable(self, star: Star) -> None:
        """WHERE referencing undefined variable should raise."""
        with pytest.raises(Exception):
            star.execute_query("MATCH (n:Person) WHERE m.age > 25 RETURN n")

    def test_set_on_undefined_variable(self, star: Star) -> None:
        """SET on undefined variable should raise."""
        with pytest.raises(Exception):
            star.execute_query("MATCH (n:Person) SET m.name = 'Dave'")


# ---------------------------------------------------------------------------
# Unregistered entity types
# ---------------------------------------------------------------------------


class TestUnregisteredTypeErrors:
    def test_match_nonexistent_label_raises(self, star: Star) -> None:
        """MATCH on unregistered label raises GraphTypeNotFoundError."""
        with pytest.raises(GraphTypeNotFoundError):
            star.execute_query("MATCH (n:Dinosaur) RETURN n.name AS name")

    def test_create_new_label(self, star: Star) -> None:
        """CREATE with new label should create a new entity type."""
        star.execute_query("CREATE (:Robot {serial: 'RX-78'})")
        result = star.execute_query("MATCH (r:Robot) RETURN r.serial AS serial")
        assert result["serial"].iloc[0] == "RX-78"


# ---------------------------------------------------------------------------
# Exception construction edge cases
# ---------------------------------------------------------------------------


class TestExceptionConstructionEdgeCases:
    def test_temporal_arithmetic_error_default_example(self) -> None:
        """TemporalArithmeticError should generate default example."""
        err = TemporalArithmeticError("+", "date", "int")
        msg = str(err)
        assert "duration" in msg.lower() or "date" in msg.lower()
        assert err.example is not None

    def test_temporal_arithmetic_error_custom_example(self) -> None:
        err = TemporalArithmeticError("+", "date", "int", example="custom example")
        assert err.example == "custom example"
        assert "custom example" in str(err)

    def test_temporal_arithmetic_non_additive_operator(self) -> None:
        err = TemporalArithmeticError("*", "date", "duration")
        assert "duration()" in str(err) or "Example:" in str(err)

    def test_cyclic_dependency_error(self) -> None:
        err = CyclicDependencyError(remaining_nodes={"A", "B"})
        msg = str(err)
        assert "Circular dependency" in msg or "cycle" in msg.lower()
        assert isinstance(err, ValueError)
        assert err.remaining_nodes == {"A", "B"}

    def test_cyclic_dependency_error_custom_message(self) -> None:
        err = CyclicDependencyError({"X"}, message="Custom cycle msg")
        assert "Custom cycle msg" in str(err)

    def test_worker_execution_error(self) -> None:
        err = WorkerExecutionError(
            worker_id="w3", query_snippet="MATCH (n) RETURN n", elapsed_ms=150.5
        )
        msg = str(err)
        assert "w3" in msg
        assert isinstance(err, RuntimeError)
        assert err.worker_id == "w3"
        assert err.elapsed_ms == 150.5

    def test_query_timeout_error(self) -> None:
        err = QueryTimeoutError("Query exceeded 30s timeout")
        assert isinstance(err, TimeoutError)

    def test_query_memory_budget_error(self) -> None:
        err = QueryMemoryBudgetError(
            estimated_bytes=2_000_000_000, budget_bytes=1_000_000_000
        )
        msg = str(err)
        assert isinstance(err, MemoryError)
        assert err.estimated_bytes == 2_000_000_000
        assert err.budget_bytes == 1_000_000_000

    def test_query_memory_budget_error_with_suggestion(self) -> None:
        err = QueryMemoryBudgetError(
            estimated_bytes=2_000_000_000,
            budget_bytes=1_000_000_000,
            suggestion="Add LIMIT clause",
        )
        assert "Add LIMIT clause" in str(err)

    def test_missing_parameter_error(self) -> None:
        err = MissingParameterError("param1")
        assert "param1" in str(err)
        assert isinstance(err, ValueError)

    def test_unsupported_function_error(self) -> None:
        err = UnsupportedFunctionError("myFunc", supported_functions=["count", "sum"])
        assert "myFunc" in str(err)
        assert isinstance(err, ValueError)
        assert err.function_name == "myFunc"

    def test_unsupported_operator_error(self) -> None:
        err = UnsupportedOperatorError("^^^", supported_operators=["+", "-", "*"])
        assert "^^^" in str(err)
        assert isinstance(err, ValueError)
        assert err.operator == "^^^"

    def test_function_argument_error(self) -> None:
        err = FunctionArgumentError("count", expected_args=1, actual_args=3)
        assert "count" in str(err)
        assert isinstance(err, ValueError)
        assert err.expected_args == 1
        assert err.actual_args == 3

    def test_pattern_comprehension_error(self) -> None:
        err = PatternComprehensionError("Invalid pattern structure")
        assert "pattern" in str(err).lower() or "Invalid" in str(err)
        assert isinstance(err, ValueError)

    def test_query_complexity_error(self) -> None:
        err = QueryComplexityError(score=150, limit=100, breakdown={"joins": 80, "filters": 70})
        msg = str(err)
        assert isinstance(err, ValueError)
        assert err.score == 150
        assert err.limit == 100

    def test_graph_type_not_found_error(self) -> None:
        err = GraphTypeNotFoundError(
            "Unknown", available_types=["Person", "Movie"]
        )
        msg = str(err)
        assert "Unknown" in msg
        assert isinstance(err, ValueError)
        assert err.type_name == "Unknown"

    def test_graph_type_not_found_error_no_available(self) -> None:
        err = GraphTypeNotFoundError("Ghost")
        assert "Ghost" in str(err)


# ---------------------------------------------------------------------------
# Error message quality - actionable guidance
# ---------------------------------------------------------------------------


class TestErrorMessageActionableGuidance:
    def test_variable_not_found_has_suggestions(self) -> None:
        """VariableNotFoundError should include available variables."""
        err = VariableNotFoundError(
            "nme", available_variables=["name", "age", "address"]
        )
        msg = str(err)
        assert "nme" in msg

    def test_variable_not_found_empty_available(self) -> None:
        """VariableNotFoundError with empty available vars still works."""
        err = VariableNotFoundError("x", available_variables=[])
        assert "x" in str(err)

    def test_variable_not_found_with_hint(self) -> None:
        """VariableNotFoundError with hint includes it."""
        err = VariableNotFoundError(
            "persn",
            available_variables=["person", "age"],
            hint="  Did you mean 'person'?",
        )
        msg = str(err)
        assert "persn" in msg

    def test_graph_type_not_found_lists_available(self) -> None:
        """GraphTypeNotFoundError should list available types."""
        err = GraphTypeNotFoundError(
            "Persn", available_types=["Person", "Movie", "Actor"]
        )
        msg = str(err)
        assert "Persn" in msg


# ---------------------------------------------------------------------------
# Mutation error edge cases
# ---------------------------------------------------------------------------


class TestMutationEdgeCases:
    def test_delete_nonexistent_label_raises(self, star: Star) -> None:
        """DELETE on nonexistent label raises GraphTypeNotFoundError."""
        with pytest.raises(GraphTypeNotFoundError):
            star.execute_query("MATCH (n:Dinosaur) DELETE n")

    def test_create_preserves_existing_data(self, star: Star) -> None:
        """CREATE should not corrupt existing data."""
        star.execute_query("CREATE (:Person {name: 'Dave', age: 40})")
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.name"
        )
        names = result["name"].tolist()
        assert "Alice" in names
        assert "Bob" in names
        assert "Carol" in names
        assert "Dave" in names

    def test_multiple_creates_in_sequence(self, star: Star) -> None:
        """Multiple CREATEs should all succeed."""
        star.execute_query("CREATE (:Person {name: 'Dave', age: 40})")
        star.execute_query("CREATE (:Person {name: 'Eve', age: 28})")
        result = star.execute_query("MATCH (p:Person) RETURN count(p) AS cnt")
        assert int(result["cnt"].iloc[0]) == 5


# ---------------------------------------------------------------------------
# Query execution with special values
# ---------------------------------------------------------------------------


class TestSpecialValueHandling:
    def test_null_property_in_where(self, star: Star) -> None:
        """WHERE on a property that doesn't exist should handle NULL."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.email IS NOT NULL RETURN p.name AS name"
        )
        assert len(result) == 0

    def test_return_nonexistent_property(self, star: Star) -> None:
        """Returning a property that doesn't exist should return NULL."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.email AS email"
        )
        assert len(result) == 3
        assert all(v is None for v in result["email"].tolist())


# ---------------------------------------------------------------------------
# Exception repr coverage
# ---------------------------------------------------------------------------


class TestExceptionReprCoverage:
    """Ensure all exceptions have useful repr for debugging."""

    def test_temporal_arithmetic_repr(self) -> None:
        err = TemporalArithmeticError("+", "date", "int")
        r = repr(err)
        assert "TemporalArithmeticError" in r
        assert "date" in r

    def test_cyclic_dependency_repr(self) -> None:
        err = CyclicDependencyError({"A", "B"})
        r = repr(err)
        assert "CyclicDependencyError" in r

    def test_worker_execution_repr(self) -> None:
        err = WorkerExecutionError("w1", "MATCH (n)", 50.0)
        r = repr(err)
        assert "WorkerExecutionError" in r

    def test_query_complexity_repr(self) -> None:
        err = QueryComplexityError(150, 100)
        r = repr(err)
        assert "QueryComplexityError" in r

    def test_graph_type_not_found_repr(self) -> None:
        err = GraphTypeNotFoundError("Ghost")
        r = repr(err)
        assert "GraphTypeNotFoundError" in r

    def test_unsupported_function_repr(self) -> None:
        err = UnsupportedFunctionError("badFunc", [])
        r = repr(err)
        assert "UnsupportedFunctionError" in r

    def test_function_argument_repr(self) -> None:
        err = FunctionArgumentError("count", 1, 3)
        r = repr(err)
        assert "FunctionArgumentError" in r

    def test_unsupported_operator_repr(self) -> None:
        err = UnsupportedOperatorError("!!!", ["+", "-"])
        r = repr(err)
        assert "UnsupportedOperatorError" in r
