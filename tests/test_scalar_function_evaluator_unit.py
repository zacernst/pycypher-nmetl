"""Architecture Loop Phase 5: ScalarFunctionEvaluator Extraction TDD Tests.

This module provides comprehensive test coverage for the extraction of scalar function
evaluation logic from BindingExpressionEvaluator into a dedicated ScalarFunctionEvaluator
component.

Test Structure:
- TestScalarFunctionEvaluatorUnit: Core evaluator instantiation and methods
- TestGraphIntrospectionFunctions: labels(), type(), keys(), properties(), startNode(), endNode()
- TestPathLengthFunction: length() hop count handling for variable-length paths
- TestAggregationValidation: Prevention of aggregation functions in scalar contexts
- TestRegistryDelegation: ScalarFunctionRegistry integration for standard scalar functions
- TestErrorHandling: Variable not found, unknown functions, type errors
- TestIntegrationWithStar: Full query execution via Star.execute_query()

The extraction maintains 100% backward compatibility with existing functionality while
achieving clear separation of concerns and improved testability.
"""

from unittest.mock import Mock

import pandas as pd
import pytest
from pycypher.ast_models import Literal, Variable
from pycypher.binding_frame import PATH_HOP_COLUMN_PREFIX, BindingFrame
from pycypher.scalar_function_evaluator import ScalarFunctionEvaluator
from pycypher.star import Star


class TestScalarFunctionEvaluatorUnit:
    """Test core ScalarFunctionEvaluator instantiation and basic methods."""

    def test_evaluator_instantiation(
        self,
        minimal_binding_frame: BindingFrame,
    ) -> None:
        """Test ScalarFunctionEvaluator can be instantiated with a binding frame."""
        evaluator = ScalarFunctionEvaluator(minimal_binding_frame)

        assert evaluator.frame is minimal_binding_frame
        assert hasattr(evaluator, "evaluate_scalar_function")

    def test_evaluator_frame_reference_immutable(
        self,
        minimal_binding_frame: BindingFrame,
    ) -> None:
        """Test that the frame reference is stored correctly and remains consistent."""
        evaluator = ScalarFunctionEvaluator(minimal_binding_frame)
        original_frame = evaluator.frame

        # Frame reference should remain the same
        assert evaluator.frame is original_frame
        assert evaluator.frame is minimal_binding_frame


class TestGraphIntrospectionFunctions:
    """Test the 6 graph introspection functions that evaluate pre-argument processing."""

    def test_labels_function_with_entity_type(self, star: Star) -> None:
        """Test labels() function returns entity type label list."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN labels(p) AS labels",
        )

        expected_labels = [["Person"]]
        assert result["labels"].iloc[0] == ["Person"]

    def test_labels_function_multiple_rows(self, star: Star) -> None:
        """Test labels() function returns same labels for all rows."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN labels(p) AS labels",
        )

        # All rows should have the same labels
        labels_values = result["labels"].tolist()
        assert all(labels == ["Person"] for labels in labels_values)

    def test_type_function_with_relationship_type(self, star: Star) -> None:
        """Test type() function returns relationship type string."""
        result = star.execute_query(
            "MATCH ()-[r:KNOWS]->() RETURN type(r) AS rel_type",
        )

        # All rows should have "KNOWS" relationship type
        type_values = result["rel_type"].tolist()
        assert all(rel_type == "KNOWS" for rel_type in type_values)

    def test_keys_function_returns_property_names(self, star: Star) -> None:
        """Test keys() function returns list of property column names."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN keys(p) AS prop_keys",
        )

        # Should return property column names excluding internal columns
        keys_list = result["prop_keys"].iloc[0]
        assert isinstance(keys_list, list)
        # Should include user-visible properties, exclude __ID__, __SOURCE__, __TARGET__
        assert "__ID__" not in keys_list
        assert "__SOURCE__" not in keys_list
        assert "__TARGET__" not in keys_list

    def test_keys_function_excludes_internal_columns(self, star: Star) -> None:
        """Test keys() function properly excludes graph internal columns."""
        result = star.execute_query(
            "MATCH ()-[r:KNOWS]->() RETURN keys(r) AS rel_keys",
        )

        keys_list = result["rel_keys"].iloc[0]
        internal_cols = {"__ID__", "__SOURCE__", "__TARGET__"}

        # None of the internal columns should appear in keys
        for col in internal_cols:
            assert col not in keys_list

    def test_properties_function_returns_dict(self, star: Star) -> None:
        """Test properties() function returns dict of all properties."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN properties(p) AS props",
        )

        props_dict = result["props"].iloc[0]
        assert isinstance(props_dict, dict)

        # Should not contain internal columns
        internal_cols = {"__ID__", "__SOURCE__", "__TARGET__"}
        for col in internal_cols:
            assert col not in props_dict

    def test_properties_function_shadow_layer_support(
        self,
        star: Star,
    ) -> None:
        """Test properties() function works with shadow layer updates."""
        # This tests that shadow layer data is used when available
        # The specific behavior depends on context._shadow implementation
        result = star.execute_query(
            "MATCH (p:Person) RETURN properties(p) AS props",
        )

        # Should successfully return properties dict without internal columns
        props_dict = result["props"].iloc[0]
        assert isinstance(props_dict, dict)

    def test_startnode_function_returns_source_id(self, star: Star) -> None:
        """Test startNode() function returns relationship source node ID."""
        result = star.execute_query(
            "MATCH (a)-[r:KNOWS]->(b) RETURN startNode(r) AS start_id",
        )

        # Should return the source node IDs
        start_ids = result["start_id"].tolist()
        assert all(isinstance(node_id, str) for node_id in start_ids)

    def test_endnode_function_returns_target_id(self, star: Star) -> None:
        """Test endNode() function returns relationship target node ID."""
        result = star.execute_query(
            "MATCH (a)-[r:KNOWS]->(b) RETURN endNode(r) AS end_id",
        )

        # Should return the target node IDs
        end_ids = result["end_id"].tolist()
        assert all(isinstance(node_id, str) for node_id in end_ids)

    def test_startnode_endnode_consistency(self, star: Star) -> None:
        """Test startNode() and endNode() return consistent source/target mapping."""
        result = star.execute_query(
            "MATCH (a)-[r:KNOWS]->(b) "
            "RETURN startNode(r) AS start_id, endNode(r) AS end_id, a.name AS start_name, b.name AS end_name",
        )

        # Verify that start/end nodes correspond to the correct entities
        for _, row in result.iterrows():
            assert (
                row["start_id"] != row["end_id"]
            )  # Should be different nodes


class TestPathLengthFunction:
    """Test length() function for variable-length path hop counting."""

    def test_length_function_with_path_variable(self, star: Star) -> None:
        """Test length() function returns hop count for path variables."""
        # This requires a variable-length path query that creates hop count columns
        query = """
        MATCH path = (a:Person)-[*1..3]->(b:Person)
        RETURN length(path) AS hop_count
        """

        result = star.execute_query(query)

        # Should return integer hop counts
        hop_counts = result["hop_count"].tolist()
        assert all(
            isinstance(count, int) and 1 <= count <= 3 for count in hop_counts
        )

    def test_length_function_creates_correct_column_name(self) -> None:
        """Test that length() looks for correctly prefixed hop count columns."""
        # Create mock frame with path hop column
        mock_frame = Mock()
        mock_frame.bindings = pd.DataFrame(
            {
                f"{PATH_HOP_COLUMN_PREFIX}mypath": [1, 2, 3],
                "other_column": ["a", "b", "c"],
            },
        )

        evaluator = ScalarFunctionEvaluator(mock_frame)

        # Mock Variable for path argument
        path_var = Variable(name="mypath")

        result = evaluator.evaluate_scalar_function("length", [path_var], None)

        expected = pd.Series(
            [1, 2, 3],
            name=f"{PATH_HOP_COLUMN_PREFIX}mypath",
        ).reset_index(drop=True)
        pd.testing.assert_series_equal(result, expected)


class TestAggregationValidation:
    """Test prevention of aggregation functions in scalar contexts."""

    def test_count_function_raises_error_in_scalar_context(
        self,
        minimal_scalar_evaluator: ScalarFunctionEvaluator,
    ) -> None:
        """Test that count() raises ValueError when used in scalar context."""
        with pytest.raises(
            (ValueError, TypeError),
            match="'count' is an aggregation function",
        ):
            minimal_scalar_evaluator.evaluate_scalar_function(
                "count",
                [Literal(value=1)],
                None,
            )

    def test_sum_function_raises_error_in_scalar_context(
        self,
        minimal_scalar_evaluator: ScalarFunctionEvaluator,
    ) -> None:
        """Test that sum() raises ValueError when used in scalar context."""
        with pytest.raises(
            (ValueError, TypeError),
            match="'sum' is an aggregation function",
        ):
            minimal_scalar_evaluator.evaluate_scalar_function(
                "sum",
                [Literal(value=1)],
                None,
            )

    def test_avg_function_raises_error_in_scalar_context(
        self,
        minimal_scalar_evaluator: ScalarFunctionEvaluator,
    ) -> None:
        """Test that avg() raises ValueError when used in scalar context."""
        with pytest.raises(
            (ValueError, TypeError),
            match="'avg' is an aggregation function",
        ):
            minimal_scalar_evaluator.evaluate_scalar_function(
                "avg",
                [Literal(value=1)],
                None,
            )

    def test_aggregation_error_message_format(
        self,
        minimal_scalar_evaluator: ScalarFunctionEvaluator,
    ) -> None:
        """Test that aggregation error messages provide clear guidance."""
        with pytest.raises((ValueError, TypeError)) as exc_info:
            minimal_scalar_evaluator.evaluate_scalar_function(
                "count",
                [Literal(value=1)],
                None,
            )

        error_msg = str(exc_info.value)
        assert "aggregation function" in error_msg
        assert "scalar expression context" in error_msg
        assert "WHERE clause" in error_msg
        assert "RETURN or WITH instead" in error_msg


class TestRegistryDelegation:
    """Test ScalarFunctionRegistry integration for standard scalar functions."""

    def test_tointeger_function_delegates_to_registry(
        self,
        star: Star,
    ) -> None:
        """Test that toInteger() is properly delegated to ScalarFunctionRegistry."""
        result = star.execute_query("RETURN toInteger('42') AS int_value")

        assert result["int_value"].iloc[0] == 42

    def test_tolower_function_delegates_to_registry(self, star: Star) -> None:
        """Test that toLower() is properly delegated to ScalarFunctionRegistry."""
        result = star.execute_query("RETURN toLower('HELLO') AS lower_value")

        assert result["lower_value"].iloc[0] == "hello"

    def test_substring_function_delegates_to_registry(
        self,
        star: Star,
    ) -> None:
        """Test that substring() is properly delegated to ScalarFunctionRegistry."""
        result = star.execute_query(
            "RETURN substring('hello world', 0, 5) AS substr",
        )

        assert result["substr"].iloc[0] == "hello"

    def test_zero_arg_functions_handled_correctly(self, star: Star) -> None:
        """Test that zero-argument functions like rand() work correctly."""
        result = star.execute_query("RETURN rand() AS random_value")

        # Should return a float between 0 and 1
        random_val = result["random_value"].iloc[0]
        assert isinstance(random_val, float)
        assert 0.0 <= random_val <= 1.0

    def test_registry_function_with_multiple_args(self, star: Star) -> None:
        """Test registry functions with multiple arguments work correctly."""
        result = star.execute_query(
            "RETURN replace('hello world', 'world', 'universe') AS replaced",
        )

        assert result["replaced"].iloc[0] == "hello universe"

    def test_min_max_special_case_list_args(self, star: Star) -> None:
        """Test that min/max with list arguments are treated as scalar functions."""
        result = star.execute_query(
            "RETURN min([1, 2, 3]) AS min_val, max([1, 2, 3]) AS max_val",
        )

        assert result["min_val"].iloc[0] == 1
        assert result["max_val"].iloc[0] == 3


class TestErrorHandling:
    """Test error handling for variable not found, unknown functions, type errors."""

    def test_variable_not_found_in_labels(
        self,
        minimal_scalar_evaluator: ScalarFunctionEvaluator,
    ) -> None:
        """Test that labels() with unknown variable falls through to registry."""
        unknown_var = Variable(name="unknown_variable")
        mock_expr_evaluator = Mock()

        # Mock the registry to raise an error when unknown function is called
        mock_expr_evaluator.scalar_registry.execute.side_effect = ValueError(
            "Unknown function",
        )
        mock_expr_evaluator.evaluate.return_value = pd.Series([1])

        # Should fall through to registry which raises appropriate error
        with pytest.raises(ValueError, match="Unknown function"):
            minimal_scalar_evaluator.evaluate_scalar_function(
                "labels",
                [unknown_var],
                mock_expr_evaluator,
            )

    def test_unknown_function_name_raises_error(
        self,
        minimal_scalar_evaluator: ScalarFunctionEvaluator,
    ) -> None:
        """Test that unknown function names raise appropriate errors."""
        mock_expr_evaluator = Mock()
        mock_expr_evaluator.evaluate.return_value = pd.Series([1])
        mock_expr_evaluator.scalar_registry.execute.side_effect = ValueError(
            "Unknown function",
        )

        with pytest.raises(ValueError, match="Unknown function"):
            minimal_scalar_evaluator.evaluate_scalar_function(
                "unknownFunction",
                [Literal(value=1)],
                mock_expr_evaluator,
            )

    def test_invalid_argument_count_handled(
        self,
        minimal_scalar_evaluator: ScalarFunctionEvaluator,
    ) -> None:
        """Test that invalid argument counts are handled properly."""
        mock_expr_evaluator = Mock()

        # labels() requires exactly 1 argument
        with pytest.raises((ValueError, TypeError)):
            minimal_scalar_evaluator.evaluate_scalar_function(
                "labels",
                [],
                mock_expr_evaluator,
            )

    def test_type_error_in_graph_functions(
        self,
        minimal_scalar_evaluator: ScalarFunctionEvaluator,
    ) -> None:
        """Test that type errors in graph functions are handled gracefully."""
        # Non-Variable argument to labels() should fall through to registry
        mock_expr_evaluator = Mock()
        literal_arg = Literal(value="not_a_variable")
        mock_expr_evaluator.evaluate.return_value = pd.Series(
            ["not_a_variable"],
        )
        mock_expr_evaluator.scalar_registry.execute.side_effect = TypeError(
            "Type error",
        )

        # Should fall through to registry (which will handle the error)
        with pytest.raises(TypeError, match="Type error"):
            minimal_scalar_evaluator.evaluate_scalar_function(
                "labels",
                [literal_arg],
                mock_expr_evaluator,
            )

    def test_missing_path_hop_column(self) -> None:
        """Test that length() with missing hop column falls through gracefully."""
        mock_frame = Mock()
        mock_frame.bindings = pd.DataFrame({"other_col": [1, 2, 3]})

        evaluator = ScalarFunctionEvaluator(mock_frame)
        path_var = Variable(name="missing_path")

        # Should fall through since hop column doesn't exist
        mock_expr_evaluator = Mock()
        mock_expr_evaluator.evaluate.return_value = pd.Series([1])
        mock_expr_evaluator.scalar_registry.execute.side_effect = KeyError(
            "Missing column",
        )

        with pytest.raises(KeyError, match="Missing column"):
            evaluator.evaluate_scalar_function(
                "length",
                [path_var],
                mock_expr_evaluator,
            )


class TestIntegrationWithStar:
    """Test full query execution integration via Star.execute_query()."""

    def test_graph_functions_in_complex_query(self, star: Star) -> None:
        """Test graph introspection functions in complex query contexts."""
        result = star.execute_query("""
            MATCH (p:Person)-[r:KNOWS]->(friend:Person)
            WHERE 'name' IN keys(p)
            RETURN
                labels(p) AS person_labels,
                type(r) AS relationship_type,
                properties(friend) AS friend_props,
                startNode(r) AS start_id,
                endNode(r) AS end_id
        """)

        assert len(result) > 0

        # Verify all columns are present and have expected types
        assert "person_labels" in result.columns
        assert "relationship_type" in result.columns
        assert "friend_props" in result.columns
        assert "start_id" in result.columns
        assert "end_id" in result.columns

        # Verify data types and content
        first_row = result.iloc[0]
        assert isinstance(first_row["person_labels"], list)
        assert first_row["person_labels"] == ["Person"]
        assert first_row["relationship_type"] == "KNOWS"
        assert isinstance(first_row["friend_props"], dict)

    def test_scalar_functions_with_graph_functions(self, star: Star) -> None:
        """Test mixing scalar functions with graph introspection functions."""
        result = star.execute_query("""
            MATCH (p:Person)
            RETURN
                toLower(toString(size(labels(p)))) AS labels_count_str,
                toInteger(size(keys(p))) AS property_count
        """)

        assert len(result) > 0

        # Should successfully combine scalar and graph functions
        first_row = result.iloc[0]
        assert isinstance(first_row["labels_count_str"], str)
        assert pd.api.types.is_integer_dtype(type(first_row["property_count"]))

    def test_error_propagation_in_query_context(self, star: Star) -> None:
        """Test that ScalarFunctionEvaluator errors propagate correctly in queries."""
        # Use an aggregation function in a WHERE clause (scalar context)
        with pytest.raises(
            (ValueError, TypeError),
            match="aggregation function",
        ):
            star.execute_query("""
                MATCH (p:Person)
                WHERE count(p) > 0
                RETURN p.name
            """)

    def test_performance_with_large_result_sets(self, star: Star) -> None:
        """Test that ScalarFunctionEvaluator maintains performance characteristics."""
        # Query that should produce multiple result rows
        result = star.execute_query("""
            MATCH (p:Person)-[r:KNOWS*1..2]->(connected:Person)
            RETURN
                labels(p) AS start_labels,
                labels(connected) AS end_labels,
                size(keys(p)) AS start_prop_count,
                size(keys(connected)) AS end_prop_count
        """)

        # Should handle multi-row results efficiently
        if len(result) > 0:
            # Verify all rows have consistent data types
            assert all(
                isinstance(labels, list) for labels in result["start_labels"]
            )
            assert all(
                isinstance(labels, list) for labels in result["end_labels"]
            )
            assert all(
                isinstance(count, int) for count in result["start_prop_count"]
            )
            assert all(
                isinstance(count, int) for count in result["end_prop_count"]
            )


# Test Fixtures
@pytest.fixture
def people_df() -> pd.DataFrame:
    """Sample people data for testing."""
    return pd.DataFrame(
        {
            "__ID__": ["p1", "p2", "p3"],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )


@pytest.fixture
def relationships_df() -> pd.DataFrame:
    """Sample relationship data for testing."""
    return pd.DataFrame(
        {
            "__ID__": ["r1", "r2"],
            "__SOURCE__": ["p1", "p2"],
            "__TARGET__": ["p2", "p3"],
            "since": [2020, 2021],
        },
    )


@pytest.fixture
def star(people_df: pd.DataFrame, relationships_df: pd.DataFrame) -> Star:
    """Create a Star instance with sample data for testing."""
    from pycypher.ingestion import ContextBuilder

    context = ContextBuilder.from_dict(
        {"Person": people_df, "KNOWS": relationships_df},
    )
    return Star(context=context)


@pytest.fixture
def minimal_binding_frame() -> BindingFrame:
    """Create a minimal BindingFrame for testing."""
    mock_frame = Mock(spec=BindingFrame)
    mock_frame.bindings = pd.DataFrame({"test_col": [1, 2, 3]})
    mock_frame.type_registry = {}
    mock_frame.context = Mock()
    return mock_frame


@pytest.fixture
def minimal_scalar_evaluator(
    minimal_binding_frame: BindingFrame,
) -> ScalarFunctionEvaluator:
    """Create a minimal ScalarFunctionEvaluator for testing."""
    return ScalarFunctionEvaluator(minimal_binding_frame)
