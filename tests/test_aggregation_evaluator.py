"""TDD tests for AggregationExpressionEvaluator Phase 3 implementation.

This module provides comprehensive test coverage for the AggregationExpressionEvaluator
extracted from BindingExpressionEvaluator as part of Architecture Loop 280 refactoring.

Run with:
    uv run pytest tests/test_architecture_loop_280_aggregation_evaluator_tdd.py -v
"""

import pandas as pd
import pytest
from pycypher.aggregation_evaluator import (
    _AGG_OPS,
    _PERCENTILE_AGGREGATIONS,
    KNOWN_AGGREGATIONS,
    AggregationExpressionEvaluator,
    _agg_avg,
    _agg_max,
    _agg_min,
    _agg_percentile_cont,
    _agg_percentile_disc,
    _agg_stdev,
    _agg_stdevp,
    _agg_sum,
    _normalize_func_args,
)
from pycypher.ast_models import (
    Arithmetic,
    CountStar,
    FunctionInvocation,
    IntegerLiteral,
)
from pycypher.binding_frame import BindingFrame
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)


class TestAggregationHelperFunctions:
    """Test helper functions for aggregation operations."""

    def test_agg_sum_basic_operation(self) -> None:
        """Test basic sum aggregation with numeric values."""
        values = pd.Series([10, 20, 30, 40])
        result = _agg_sum(values)
        assert result == 100.0

    def test_agg_sum_with_nulls(self) -> None:
        """Test sum aggregation ignores null values."""
        values = pd.Series([10, None, 30, None, 50])
        result = _agg_sum(values)
        assert result == 90.0

    def test_agg_sum_all_nulls(self) -> None:
        """Test sum over all-null series returns None (Cypher specification)."""
        values = pd.Series([None, None, None])
        result = _agg_sum(values)
        assert result is None

    def test_agg_sum_empty_series(self) -> None:
        """Test sum over empty series returns None."""
        values = pd.Series([], dtype="float64")
        result = _agg_sum(values)
        assert result is None

    def test_agg_avg_basic_operation(self) -> None:
        """Test basic average aggregation."""
        values = pd.Series([10, 20, 30, 40])
        result = _agg_avg(values)
        assert result == 25.0

    def test_agg_avg_with_nulls(self) -> None:
        """Test average ignores null values."""
        values = pd.Series([10, None, 30, None, 50])
        result = _agg_avg(values)
        assert result == 30.0  # (10 + 30 + 50) / 3

    def test_agg_avg_all_nulls(self) -> None:
        """Test average over all-null series returns None."""
        values = pd.Series([None, None, None])
        result = _agg_avg(values)
        assert result is None

    def test_agg_min_basic_operation(self) -> None:
        """Test basic minimum aggregation."""
        values = pd.Series([30, 10, 40, 20])
        result = _agg_min(values)
        assert result == 10

    def test_agg_min_with_nulls(self) -> None:
        """Test minimum ignores null values."""
        values = pd.Series([30, None, 10, None, 40])
        result = _agg_min(values)
        assert result == 10

    def test_agg_min_all_nulls(self) -> None:
        """Test minimum over all-null series returns None."""
        values = pd.Series([None, None, None])
        result = _agg_min(values)
        assert result is None

    def test_agg_max_basic_operation(self) -> None:
        """Test basic maximum aggregation."""
        values = pd.Series([30, 10, 40, 20])
        result = _agg_max(values)
        assert result == 40

    def test_agg_max_with_nulls(self) -> None:
        """Test maximum ignores null values."""
        values = pd.Series([30, None, 10, None, 40])
        result = _agg_max(values)
        assert result == 40

    def test_agg_max_all_nulls(self) -> None:
        """Test maximum over all-null series returns None."""
        values = pd.Series([None, None, None])
        result = _agg_max(values)
        assert result is None

    def test_agg_percentile_cont_basic(self) -> None:
        """Test continuous percentile (linear interpolation)."""
        values = pd.Series([1, 2, 3, 4, 5])
        result = _agg_percentile_cont(values, 0.5)  # 50th percentile (median)
        assert result == 3.0

    def test_agg_percentile_cont_with_nulls(self) -> None:
        """Test continuous percentile ignores nulls."""
        values = pd.Series([1, None, 2, None, 3, 4, 5])
        result = _agg_percentile_cont(values, 0.5)
        assert result == 3.0

    def test_agg_percentile_cont_all_nulls(self) -> None:
        """Test continuous percentile over all-null series returns None."""
        values = pd.Series([None, None, None])
        result = _agg_percentile_cont(values, 0.5)
        assert result is None

    def test_agg_percentile_disc_basic(self) -> None:
        """Test discrete percentile (lower interpolation)."""
        values = pd.Series([1, 2, 3, 4, 5])
        result = _agg_percentile_disc(
            values,
            0.6,
        )  # Should select actual value
        assert isinstance(result, float)

    def test_agg_percentile_disc_all_nulls(self) -> None:
        """Test discrete percentile over all-null series returns None."""
        values = pd.Series([None, None, None])
        result = _agg_percentile_disc(values, 0.5)
        assert result is None

    def test_agg_stdev_basic_operation(self) -> None:
        """Test sample standard deviation (ddof=1)."""
        values = pd.Series([1, 2, 3, 4, 5])
        result = _agg_stdev(values)
        expected = values.std()  # pandas default is ddof=1
        assert result == expected

    def test_agg_stdev_insufficient_data(self) -> None:
        """Test standard deviation with < 2 values returns None."""
        values = pd.Series([42])
        result = _agg_stdev(values)
        assert result is None

    def test_agg_stdev_with_nulls(self) -> None:
        """Test standard deviation ignores nulls."""
        values = pd.Series([1, None, 2, None, 3, 4, 5])
        result = _agg_stdev(values)
        expected = pd.Series([1, 2, 3, 4, 5]).std()
        assert result == expected

    def test_agg_stdevp_basic_operation(self) -> None:
        """Test population standard deviation (ddof=0)."""
        values = pd.Series([1, 2, 3, 4, 5])
        result = _agg_stdevp(values)
        expected = values.std(ddof=0)
        assert result == expected

    def test_agg_stdevp_empty_series(self) -> None:
        """Test population standard deviation with empty series returns None."""
        values = pd.Series([], dtype="float64")
        result = _agg_stdevp(values)
        assert result is None


class TestNormalizeFuncArgs:
    """Test function argument normalization helper."""

    def test_normalize_none_arguments(self) -> None:
        """Test normalization of None arguments."""
        result = _normalize_func_args(None)
        assert result == []

    def test_normalize_dict_arguments(self) -> None:
        """Test normalization of dictionary-style arguments with 'arguments' key."""
        arguments = {"arguments": ["n.age", "0.5"], "distinct": True}
        result = _normalize_func_args(arguments)
        assert result == ["n.age", "0.5"]

    def test_normalize_dict_args_key(self) -> None:
        """Test normalization of dictionary-style arguments with 'args' key."""
        arguments = {"args": ["n.salary", "0.8"]}
        result = _normalize_func_args(arguments)
        assert result == ["n.salary", "0.8"]

    def test_normalize_dict_no_special_keys(self) -> None:
        """Test normalization of dictionary without 'arguments' or 'args' keys."""
        arguments = {"expression": "n.age", "distinct": True}
        result = _normalize_func_args(arguments)
        assert result == []

    def test_normalize_list_arguments(self) -> None:
        """Test normalization of list arguments."""
        arguments = ["n.age", "0.5"]
        result = _normalize_func_args(arguments)
        assert result == ["n.age", "0.5"]

    def test_normalize_single_argument(self) -> None:
        """Test normalization of single argument returns empty list."""
        result = _normalize_func_args("n.age")
        assert result == []  # Single strings don't match expected dict/list format


class TestDispatchTables:
    """Test aggregation dispatch tables."""

    def test_agg_ops_table_completeness(self) -> None:
        """Test AGG_OPS dispatch table contains expected aggregations."""
        expected_ops = {
            "collect",
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "stdev",
            "stdevp",
        }
        assert set(_AGG_OPS.keys()) == expected_ops

    def test_percentile_aggregations_set(self) -> None:
        """Test percentile aggregations set."""
        expected = {"percentilecont", "percentiledisc"}
        assert expected == _PERCENTILE_AGGREGATIONS

    def test_known_aggregations_completeness(self) -> None:
        """Test KNOWN_AGGREGATIONS includes all aggregation functions."""
        expected = set(_AGG_OPS.keys()) | _PERCENTILE_AGGREGATIONS
        assert expected == KNOWN_AGGREGATIONS

    def test_collect_operation(self) -> None:
        """Test collect aggregation function."""
        values = pd.Series([1, 2, 3])
        result = _AGG_OPS["collect"](values)
        assert result == [1, 2, 3]

    def test_count_operation(self) -> None:
        """Test count aggregation function (non-null count)."""
        values = pd.Series([1, None, 3, None, 5])
        result = _AGG_OPS["count"](values)
        assert result == 3  # Only non-null values


class TestAggregationExpressionEvaluator:
    """Test AggregationExpressionEvaluator class functionality."""

    @pytest.fixture
    def test_frame(self) -> BindingFrame:
        """Create test BindingFrame with sample data."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4, 5],
                "Person__age": [25, 30, 35, 40, 45],
                "Person__salary": [50000, 60000, 70000, 80000, 90000],
            },
        )

        person_table = EntityTable.from_dataframe(
            "Person",
            pd.DataFrame(
                {
                    ID_COLUMN: [1, 2, 3, 4, 5],
                    "age": [25, 30, 35, 40, 45],
                    "salary": [50000, 60000, 70000, 80000, 90000],
                },
            ),
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        type_registry = {"p": "Person"}
        return BindingFrame(
            bindings=df,
            context=context,
            type_registry=type_registry,
        )

    @pytest.fixture
    def aggregation_evaluator(
        self,
        test_frame: BindingFrame,
    ) -> AggregationExpressionEvaluator:
        """Create AggregationExpressionEvaluator instance."""
        return AggregationExpressionEvaluator(test_frame)

    def test_evaluator_initialization(self, test_frame: BindingFrame) -> None:
        """Test evaluator initializes correctly."""
        evaluator = AggregationExpressionEvaluator(test_frame)
        assert evaluator.frame is test_frame


class TestCountStarEvaluation:
    """Test COUNT(*) evaluation."""

    @pytest.fixture
    def test_frame(self) -> BindingFrame:
        """Create test BindingFrame with sample data."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "Person__age": [25, 30, 35, 40],
            },
        )

        person_table = EntityTable.from_dataframe(
            "Person",
            pd.DataFrame(
                {
                    ID_COLUMN: [1, 2, 3, 4],
                    "age": [25, 30, 35, 40],
                },
            ),
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        type_registry = {"p": "Person"}
        return BindingFrame(
            bindings=df,
            context=context,
            type_registry=type_registry,
        )

    def test_count_star_evaluation(self, test_frame: BindingFrame) -> None:
        """Test COUNT(*) returns frame length."""
        count_star = CountStar()

        evaluator = AggregationExpressionEvaluator(test_frame)
        result = evaluator.evaluate_aggregation(count_star, None)
        assert result == 4  # Length of test frame


class TestFunctionInvocationEvaluation:
    """Test function invocation evaluation."""

    @pytest.fixture
    def test_frame(self) -> BindingFrame:
        """Create test BindingFrame with sample data."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4, 5],
                "Person__salary": [
                    50000,
                    60000,
                    None,
                    80000,
                    90000,
                ],  # Include null for testing
            },
        )

        person_table = EntityTable.from_dataframe(
            "Person",
            pd.DataFrame(
                {
                    ID_COLUMN: [1, 2, 3, 4, 5],
                    "salary": [50000, 60000, None, 80000, 90000],
                },
            ),
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        type_registry = {"p": "Person"}
        return BindingFrame(
            bindings=df,
            context=context,
            type_registry=type_registry,
        )

    def test_sum_aggregation(self, test_frame: BindingFrame) -> None:
        """Test SUM aggregation function."""
        sum_function = FunctionInvocation(
            name="sum",
            arguments={"expression": "n.salary"},
        )

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "n.salary":
                    return pd.Series([50000, 60000, None, 80000, 90000])
                return pd.Series([0, 0, 0, 0, 0])

        evaluator = AggregationExpressionEvaluator(test_frame)
        mock_evaluator = MockExpressionEvaluator()
        result = evaluator.evaluate_aggregation(sum_function, mock_evaluator)

        # Sum should ignore null: 50000 + 60000 + 80000 + 90000 = 280000
        assert result == 280000.0

    def test_count_with_expression(self, test_frame: BindingFrame) -> None:
        """Test COUNT(expression) - counts non-null values."""
        count_function = FunctionInvocation(
            name="count",
            arguments={"expression": "n.salary"},
        )

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "n.salary":
                    return pd.Series([50000, 60000, None, 80000, 90000])
                return pd.Series([0, 0, 0, 0, 0])

        evaluator = AggregationExpressionEvaluator(test_frame)
        mock_evaluator = MockExpressionEvaluator()
        result = evaluator.evaluate_aggregation(count_function, mock_evaluator)

        # COUNT should count non-null values: 4 (excludes the None)
        assert result == 4

    def test_count_without_expression(self, test_frame: BindingFrame) -> None:
        """Test COUNT() without expression - equivalent to COUNT(*)."""
        count_function = FunctionInvocation(name="count", arguments=None)

        evaluator = AggregationExpressionEvaluator(test_frame)
        result = evaluator.evaluate_aggregation(count_function, None)

        # COUNT() should return frame length
        assert result == 5

    def test_unsupported_aggregation_function(
        self,
        test_frame: BindingFrame,
    ) -> None:
        """Test unsupported aggregation function raises UnsupportedFunctionError."""
        unsupported_function = FunctionInvocation(
            name="unsupported",
            arguments={"expression": "n.age"},
        )

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                return pd.Series([1, 2, 3, 4, 5])

        evaluator = AggregationExpressionEvaluator(test_frame)
        mock_evaluator = MockExpressionEvaluator()

        # Import the expected exception
        from pycypher.exceptions import UnsupportedFunctionError

        with pytest.raises(UnsupportedFunctionError):
            evaluator.evaluate_aggregation(
                unsupported_function,
                mock_evaluator,
            )


class TestDistinctAggregation:
    """Test DISTINCT modifier in aggregations."""

    @pytest.fixture
    def test_frame(self) -> BindingFrame:
        """Create test BindingFrame with duplicate values."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4, 5],
                "Person__category": [
                    1,
                    1,
                    2,
                    2,
                    3,
                ],  # Duplicate values for DISTINCT testing
            },
        )

        person_table = EntityTable.from_dataframe(
            "Person",
            pd.DataFrame(
                {
                    ID_COLUMN: [1, 2, 3, 4, 5],
                    "category": [1, 1, 2, 2, 3],
                },
            ),
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        type_registry = {"p": "Person"}
        return BindingFrame(
            bindings=df,
            context=context,
            type_registry=type_registry,
        )

    def test_sum_distinct(self, test_frame: BindingFrame) -> None:
        """Test SUM with DISTINCT modifier."""
        sum_distinct_function = FunctionInvocation(
            name="sum",
            arguments={"expression": "n.category", "distinct": True},
        )

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "n.category":
                    return pd.Series(
                        [1, 1, 2, 2, 3],
                    )  # Duplicates: should sum to 1+2+3=6
                return pd.Series([0, 0, 0, 0, 0])

        evaluator = AggregationExpressionEvaluator(test_frame)
        mock_evaluator = MockExpressionEvaluator()
        result = evaluator.evaluate_aggregation(
            sum_distinct_function,
            mock_evaluator,
        )

        # SUM DISTINCT should deduplicate: 1 + 2 + 3 = 6
        assert result == 6.0


class TestPercentileAggregations:
    """Test percentile aggregation functions."""

    @pytest.fixture
    def test_frame(self) -> BindingFrame:
        """Create test BindingFrame for percentile testing."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4, 5],
                "Person__score": [10, 20, 30, 40, 50],
            },
        )

        person_table = EntityTable.from_dataframe(
            "Person",
            pd.DataFrame(
                {
                    ID_COLUMN: [1, 2, 3, 4, 5],
                    "score": [10, 20, 30, 40, 50],
                },
            ),
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        type_registry = {"p": "Person"}
        return BindingFrame(
            bindings=df,
            context=context,
            type_registry=type_registry,
        )

    def test_percentile_cont_evaluation(
        self,
        test_frame: BindingFrame,
    ) -> None:
        """Test percentileCont evaluation."""
        percentile_cont_function = FunctionInvocation(
            name="percentileCont",
            arguments={
                "arguments": ["n.score", "0.5"],
            },  # 50th percentile (median)
        )

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "n.score":
                    return pd.Series([10, 20, 30, 40, 50])
                if expr == "0.5":
                    return pd.Series([0.5])
                return pd.Series([0])

        evaluator = AggregationExpressionEvaluator(test_frame)
        mock_evaluator = MockExpressionEvaluator()
        result = evaluator.evaluate_aggregation(
            percentile_cont_function,
            mock_evaluator,
        )

        # 50th percentile of [10, 20, 30, 40, 50] is 30.0
        assert result == 30.0

    def test_percentile_disc_evaluation(
        self,
        test_frame: BindingFrame,
    ) -> None:
        """Test percentileDisc evaluation."""
        percentile_disc_function = FunctionInvocation(
            name="percentileDisc",
            arguments={"arguments": ["n.score", "0.5"]},  # 50th percentile
        )

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "n.score":
                    return pd.Series([10, 20, 30, 40, 50])
                if expr == "0.5":
                    return pd.Series([0.5])
                return pd.Series([0])

        evaluator = AggregationExpressionEvaluator(test_frame)
        mock_evaluator = MockExpressionEvaluator()
        result = evaluator.evaluate_aggregation(
            percentile_disc_function,
            mock_evaluator,
        )

        # Should return an actual value from the dataset
        assert isinstance(result, float)
        assert result in [10.0, 20.0, 30.0, 40.0, 50.0]

    def test_percentile_insufficient_arguments(
        self,
        test_frame: BindingFrame,
    ) -> None:
        """Test percentile functions with insufficient arguments."""
        percentile_function = FunctionInvocation(
            name="percentileCont",
            arguments={
                "arguments": ["n.score"],
            },  # Missing percentile argument
        )

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                return pd.Series([10, 20, 30, 40, 50])

        evaluator = AggregationExpressionEvaluator(test_frame)
        mock_evaluator = MockExpressionEvaluator()

        from pycypher.exceptions import FunctionArgumentError

        with pytest.raises(FunctionArgumentError):
            evaluator.evaluate_aggregation(percentile_function, mock_evaluator)


class TestGroupedAggregation:
    """Test grouped aggregation evaluation."""

    @pytest.fixture
    def test_frame(self) -> BindingFrame:
        """Create test BindingFrame for grouped aggregation."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4, 5, 6],
                "Person__department": ["A", "A", "B", "B", "C", "C"],
                "Person__salary": [50000, 60000, 70000, 80000, 90000, 100000],
            },
        )

        person_table = EntityTable.from_dataframe(
            "Person",
            pd.DataFrame(
                {
                    ID_COLUMN: [1, 2, 3, 4, 5, 6],
                    "department": ["A", "A", "B", "B", "C", "C"],
                    "salary": [50000, 60000, 70000, 80000, 90000, 100000],
                },
            ),
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        type_registry = {"p": "Person"}
        return BindingFrame(
            bindings=df,
            context=context,
            type_registry=type_registry,
        )

    def test_grouped_count_star(self, test_frame: BindingFrame) -> None:
        """Test grouped COUNT(*) evaluation."""
        count_star = CountStar()

        group_df = pd.DataFrame(
            {
                "Person__department": ["A", "A", "B", "B", "C", "C"],
            },
        )

        evaluator = AggregationExpressionEvaluator(test_frame)
        result = evaluator.evaluate_aggregation_grouped(
            count_star,
            group_df,
            ["Person__department"],
            None,
        )

        # Should return Series with count per group: [2, 2, 2] for groups A, B, C
        assert isinstance(result, pd.Series)
        assert len(result) == 3  # 3 groups
        assert all(count == 2 for count in result)  # Each group has 2 members

    def test_grouped_sum_aggregation(self, test_frame: BindingFrame) -> None:
        """Test grouped SUM aggregation."""
        sum_function = FunctionInvocation(
            name="sum",
            arguments={"expression": "n.salary"},
        )

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "n.salary":
                    return pd.Series(
                        [50000, 60000, 70000, 80000, 90000, 100000],
                    )
                return pd.Series([0, 0, 0, 0, 0, 0])

        group_df = pd.DataFrame(
            {
                "Person__department": ["A", "A", "B", "B", "C", "C"],
            },
        )

        evaluator = AggregationExpressionEvaluator(test_frame)
        mock_evaluator = MockExpressionEvaluator()
        result = evaluator.evaluate_aggregation_grouped(
            sum_function,
            group_df,
            ["Person__department"],
            mock_evaluator,
        )

        # Should return Series with sum per group
        # A: 50000 + 60000 = 110000
        # B: 70000 + 80000 = 150000
        # C: 90000 + 100000 = 190000
        assert isinstance(result, pd.Series)
        assert len(result) == 3
        expected_sums = [110000.0, 150000.0, 190000.0]
        assert list(result) == expected_sums

    def test_grouped_unsupported_aggregation(
        self,
        test_frame: BindingFrame,
    ) -> None:
        """Test grouped aggregation with unsupported function returns None."""
        unsupported_function = FunctionInvocation(
            name="unsupported",
            arguments={"expression": "n.salary"},
        )

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                return pd.Series([50000, 60000, 70000, 80000])

        group_df = pd.DataFrame(
            {
                "Person__department": ["A", "A", "B", "B"],
            },
        )

        evaluator = AggregationExpressionEvaluator(test_frame)
        mock_evaluator = MockExpressionEvaluator()
        result = evaluator.evaluate_aggregation_grouped(
            unsupported_function,
            group_df,
            ["Person__department"],
            mock_evaluator,
        )

        assert result is None


class TestArithmeticInAggregation:
    """Test arithmetic operations within aggregation expressions."""

    @pytest.fixture
    def test_frame(self) -> BindingFrame:
        """Create test BindingFrame for arithmetic testing."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "Person__count": [10, 20, 30, 40],
            },
        )

        person_table = EntityTable.from_dataframe(
            "Person",
            pd.DataFrame(
                {
                    ID_COLUMN: [1, 2, 3, 4],
                    "count": [10, 20, 30, 40],
                },
            ),
        )

        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        type_registry = {"p": "Person"}
        return BindingFrame(
            bindings=df,
            context=context,
            type_registry=type_registry,
        )

    def test_arithmetic_in_aggregation(self, test_frame: BindingFrame) -> None:
        """Test arithmetic operations within aggregations (e.g., count(*) + 1)."""
        count_star = CountStar()
        literal_one = IntegerLiteral(value=1)
        arithmetic_expr = Arithmetic(
            operator="+",
            left=count_star,
            right=literal_one,
        )

        class MockExpressionEvaluator:
            def _eval_as_scalar(self, expr):
                if isinstance(expr, CountStar):
                    return 4  # Frame length
                if isinstance(expr, IntegerLiteral):
                    return expr.value
                return 0

        evaluator = AggregationExpressionEvaluator(test_frame)
        mock_evaluator = MockExpressionEvaluator()
        result = evaluator.evaluate_aggregation(
            arithmetic_expr,
            mock_evaluator,
        )

        # COUNT(*) + 1 = 4 + 1 = 5
        assert result == 5

    def test_division_by_zero_in_aggregation(
        self,
        test_frame: BindingFrame,
    ) -> None:
        """Test division by zero in aggregation arithmetic returns None."""
        literal_ten = IntegerLiteral(value=10)
        literal_zero = IntegerLiteral(value=0)
        division_expr = Arithmetic(
            operator="/",
            left=literal_ten,
            right=literal_zero,
        )

        class MockExpressionEvaluator:
            def _eval_as_scalar(self, expr):
                return expr.value

        evaluator = AggregationExpressionEvaluator(test_frame)
        mock_evaluator = MockExpressionEvaluator()
        result = evaluator.evaluate_aggregation(division_expr, mock_evaluator)

        # Division by zero should return None
        assert result is None

    def test_unsupported_arithmetic_operator(
        self,
        test_frame: BindingFrame,
    ) -> None:
        """Test unsupported arithmetic operator in aggregation raises ValueError."""
        literal_ten = IntegerLiteral(value=10)
        literal_two = IntegerLiteral(value=2)
        arithmetic_expr = Arithmetic(
            operator="@",
            left=literal_ten,
            right=literal_two,
        )  # Unsupported operator

        class MockExpressionEvaluator:
            def _eval_as_scalar(self, expr):
                return expr.value

        evaluator = AggregationExpressionEvaluator(test_frame)
        mock_evaluator = MockExpressionEvaluator()

        with pytest.raises(TypeError, match="Operator '@' incompatible"):
            evaluator.evaluate_aggregation(arithmetic_expr, mock_evaluator)


class TestErrorHandling:
    """Test error handling in aggregation evaluation."""

    @pytest.fixture
    def test_frame(self) -> BindingFrame:
        """Create minimal test BindingFrame."""
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3]})
        person_table = EntityTable.from_dataframe(
            "Person",
            pd.DataFrame({ID_COLUMN: [1, 2, 3]}),
        )
        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        )
        type_registry = {"p": "Person"}
        return BindingFrame(
            bindings=df,
            context=context,
            type_registry=type_registry,
        )

    def test_invalid_expression_type(self, test_frame: BindingFrame) -> None:
        """Test evaluation with invalid expression type raises ValueError."""

        class InvalidExpression:
            pass

        evaluator = AggregationExpressionEvaluator(test_frame)

        with pytest.raises(
            ValueError,
            match="Expected FunctionInvocation or CountStar",
        ):
            evaluator.evaluate_aggregation(InvalidExpression(), None)

    def test_missing_argument_expression(
        self,
        test_frame: BindingFrame,
    ) -> None:
        """Test aggregation without required argument raises ValueError."""
        sum_function = FunctionInvocation(name="sum", arguments=None)

        evaluator = AggregationExpressionEvaluator(test_frame)

        from pycypher.exceptions import FunctionArgumentError

        with pytest.raises(FunctionArgumentError, match="sum"):
            evaluator.evaluate_aggregation(sum_function, None)
