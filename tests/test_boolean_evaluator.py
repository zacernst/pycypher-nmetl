"""TDD tests for BooleanExpressionEvaluator Phase 2 implementation.

This module provides comprehensive test coverage for the BooleanExpressionEvaluator
extracted from BindingExpressionEvaluator as part of Architecture Loop 277 refactoring.

Run with:
    uv run pytest tests/test_architecture_loop_277_boolean_evaluator_tdd.py -v
"""

import operator

import pandas as pd
import pytest
from pycypher.binding_frame import BindingFrame
from pycypher.boolean_evaluator import (
    _BOOL_FOLD_OPS,
    BooleanExpressionEvaluator,
    kleene_and,
    kleene_not,
    kleene_or,
    kleene_xor,
)
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)


class TestKleeneLogicHelpers:
    """Test Kleene three-valued logic helper functions."""

    def test_kleene_and_true_true(self) -> None:
        """Test Kleene AND with true AND true."""
        left = pd.Series([True, True, True])
        right = pd.Series([True, True, True])
        result = kleene_and(left, right)

        expected = pd.Series([True, True, True], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_kleene_and_false_false(self) -> None:
        """Test Kleene AND with false AND false."""
        left = pd.Series([False, False, False])
        right = pd.Series([False, False, False])
        result = kleene_and(left, right)

        expected = pd.Series([False, False, False], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_kleene_and_mixed_values(self) -> None:
        """Test Kleene AND with mixed boolean values."""
        left = pd.Series([True, False, True, False])
        right = pd.Series([True, True, False, False])
        result = kleene_and(left, right)

        expected = pd.Series([True, False, False, False], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_kleene_and_null_handling(self) -> None:
        """Test Kleene AND with null values."""
        left = pd.Series([True, False, None, None])
        right = pd.Series([None, None, True, False])
        result = kleene_and(left, right)

        # null AND false = false, null AND true = null
        expected = pd.Series([None, False, None, False], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_kleene_or_true_false(self) -> None:
        """Test Kleene OR with mixed values."""
        left = pd.Series([True, False, True, False])
        right = pd.Series([True, True, False, False])
        result = kleene_or(left, right)

        expected = pd.Series([True, True, True, False], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_kleene_or_null_handling(self) -> None:
        """Test Kleene OR with null values."""
        left = pd.Series([True, False, None, None])
        right = pd.Series([None, None, True, False])
        result = kleene_or(left, right)

        # null OR true = true, null OR false = null
        expected = pd.Series([True, None, True, None], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_kleene_xor_basic(self) -> None:
        """Test Kleene XOR with basic values."""
        left = pd.Series([True, False, True, False])
        right = pd.Series([True, True, False, False])
        result = kleene_xor(left, right)

        expected = pd.Series([False, True, True, False], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_kleene_xor_null_handling(self) -> None:
        """Test Kleene XOR with null values (null XOR anything = null)."""
        left = pd.Series([True, False, None, None])
        right = pd.Series([None, None, True, False])
        result = kleene_xor(left, right)

        expected = pd.Series([None, None, None, None], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_kleene_not_basic(self) -> None:
        """Test Kleene NOT with basic values."""
        series = pd.Series([True, False, True, False])
        result = kleene_not(series)

        expected = pd.Series([False, True, False, True], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_kleene_not_null_handling(self) -> None:
        """Test Kleene NOT with null values (NOT null = null)."""
        series = pd.Series([True, False, None, None])
        result = kleene_not(series)

        expected = pd.Series([False, True, None, None], dtype=object)
        pd.testing.assert_series_equal(result, expected)


class TestBooleanFoldOpsTable:
    """Test boolean fold operations dispatch table."""

    def test_bool_fold_ops_table(self) -> None:
        """Test boolean fold ops table is correctly defined."""
        assert len(_BOOL_FOLD_OPS) == 3
        assert "and" in _BOOL_FOLD_OPS
        assert "or" in _BOOL_FOLD_OPS
        assert "xor" in _BOOL_FOLD_OPS

        # Test operations and identity elements
        and_op, and_identity = _BOOL_FOLD_OPS["and"]
        assert and_op is operator.and_
        assert and_identity is True

        or_op, or_identity = _BOOL_FOLD_OPS["or"]
        assert or_op is operator.or_
        assert or_identity is False

        xor_op, xor_identity = _BOOL_FOLD_OPS["xor"]
        assert xor_op is operator.xor
        assert xor_identity is False

    def test_bool_fold_ops_functionality(self) -> None:
        """Test boolean fold operations work correctly."""
        and_op, _ = _BOOL_FOLD_OPS["and"]
        or_op, _ = _BOOL_FOLD_OPS["or"]
        xor_op, _ = _BOOL_FOLD_OPS["xor"]

        assert and_op(True, True) is True
        assert and_op(True, False) is False
        assert or_op(True, False) is True
        assert or_op(False, False) is False
        assert xor_op(True, False) is True
        assert xor_op(True, True) is False


class TestBooleanExpressionEvaluator:
    """Test BooleanExpressionEvaluator class functionality."""

    @pytest.fixture
    def test_frame(self) -> BindingFrame:
        """Create test BindingFrame with sample data."""
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4],
                "Person__active": [True, False, True, False],
                "Person__verified": [True, True, False, False],
            },
        )

        person_table = EntityTable.from_dataframe(
            "Person",
            pd.DataFrame(
                {
                    ID_COLUMN: [1, 2, 3, 4],
                    "active": [True, False, True, False],
                    "verified": [True, True, False, False],
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
    def boolean_evaluator(
        self,
        test_frame: BindingFrame,
    ) -> BooleanExpressionEvaluator:
        """Create BooleanExpressionEvaluator instance."""
        return BooleanExpressionEvaluator(test_frame)

    def test_evaluator_initialization(self, test_frame: BindingFrame) -> None:
        """Test evaluator initializes correctly."""
        evaluator = BooleanExpressionEvaluator(test_frame)
        assert evaluator.frame is test_frame

    def test_evaluate_and_basic(
        self,
        boolean_evaluator: BooleanExpressionEvaluator,
    ) -> None:
        """Test AND evaluation with basic operands."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "true_expr":
                    return pd.Series([True, True, True, True])
                if expr == "false_expr":
                    return pd.Series([False, False, False, False])
                if expr == "mixed_expr":
                    return pd.Series([True, False, True, False])
                return pd.Series([True, True, True, True])

        mock_evaluator = MockExpressionEvaluator()
        result = boolean_evaluator.evaluate_and(
            ["true_expr", "mixed_expr"],
            mock_evaluator,
        )

        expected = pd.Series([True, False, True, False], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_and_empty_operands(
        self,
        boolean_evaluator: BooleanExpressionEvaluator,
    ) -> None:
        """Test AND evaluation with empty operands (should return True)."""

        class MockExpressionEvaluator:
            pass

        mock_evaluator = MockExpressionEvaluator()
        result = boolean_evaluator.evaluate_and([], mock_evaluator)

        expected = pd.Series([True, True, True, True])
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_or_basic(
        self,
        boolean_evaluator: BooleanExpressionEvaluator,
    ) -> None:
        """Test OR evaluation with basic operands."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "false_expr":
                    return pd.Series([False, False, False, False])
                if expr == "mixed_expr":
                    return pd.Series([True, False, True, False])
                return pd.Series([False, False, False, False])

        mock_evaluator = MockExpressionEvaluator()
        result = boolean_evaluator.evaluate_or(
            ["false_expr", "mixed_expr"],
            mock_evaluator,
        )

        expected = pd.Series([True, False, True, False], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_or_empty_operands(
        self,
        boolean_evaluator: BooleanExpressionEvaluator,
    ) -> None:
        """Test OR evaluation with empty operands (should return False)."""

        class MockExpressionEvaluator:
            pass

        mock_evaluator = MockExpressionEvaluator()
        result = boolean_evaluator.evaluate_or([], mock_evaluator)

        expected = pd.Series([False, False, False, False])
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_not_basic(
        self,
        boolean_evaluator: BooleanExpressionEvaluator,
    ) -> None:
        """Test NOT evaluation with basic operand."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "mixed_expr":
                    return pd.Series([True, False, True, False])
                return pd.Series([True, True, True, True])

        mock_evaluator = MockExpressionEvaluator()
        result = boolean_evaluator.evaluate_not("mixed_expr", mock_evaluator)

        expected = pd.Series([False, True, False, True], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_xor_basic(
        self,
        boolean_evaluator: BooleanExpressionEvaluator,
    ) -> None:
        """Test XOR evaluation with basic operands."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "left_expr":
                    return pd.Series([True, False, True, False])
                if expr == "right_expr":
                    return pd.Series([True, True, False, False])
                return pd.Series([False, False, False, False])

        mock_evaluator = MockExpressionEvaluator()
        result = boolean_evaluator.evaluate_xor(
            ["left_expr", "right_expr"],
            mock_evaluator,
        )

        expected = pd.Series([False, True, True, False], dtype=object)
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_xor_empty_operands(
        self,
        boolean_evaluator: BooleanExpressionEvaluator,
    ) -> None:
        """Test XOR evaluation with empty operands (should return False)."""

        class MockExpressionEvaluator:
            pass

        mock_evaluator = MockExpressionEvaluator()
        result = boolean_evaluator.evaluate_xor([], mock_evaluator)

        expected = pd.Series([False, False, False, False])
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_bool_chain_and(
        self,
        boolean_evaluator: BooleanExpressionEvaluator,
    ) -> None:
        """Test boolean chain evaluation with AND operation."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "expr1":
                    return pd.Series([True, True, False, None])
                if expr == "expr2":
                    return pd.Series([True, False, False, True])
                return pd.Series([True, True, True, True])

        mock_evaluator = MockExpressionEvaluator()
        result = boolean_evaluator.evaluate_bool_chain(
            "and",
            ["expr1", "expr2"],
            mock_evaluator,
        )

        # null-safe operation: None becomes False
        expected = pd.Series([True, False, False, False])
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_bool_chain_or(
        self,
        boolean_evaluator: BooleanExpressionEvaluator,
    ) -> None:
        """Test boolean chain evaluation with OR operation."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                if expr == "expr1":
                    return pd.Series([False, False, True, None])
                if expr == "expr2":
                    return pd.Series([False, True, False, False])
                return pd.Series([False, False, False, False])

        mock_evaluator = MockExpressionEvaluator()
        result = boolean_evaluator.evaluate_bool_chain(
            "or",
            ["expr1", "expr2"],
            mock_evaluator,
        )

        # null-safe operation: None becomes False
        expected = pd.Series([False, True, True, False])
        pd.testing.assert_series_equal(result, expected)

    def test_evaluate_bool_chain_empty_operands(
        self,
        boolean_evaluator: BooleanExpressionEvaluator,
    ) -> None:
        """Test boolean chain evaluation with empty operands."""

        class MockExpressionEvaluator:
            pass

        mock_evaluator = MockExpressionEvaluator()

        # AND with empty operands returns True
        result_and = boolean_evaluator.evaluate_bool_chain(
            "and",
            [],
            mock_evaluator,
        )
        expected_and = pd.Series([True, True, True, True])
        pd.testing.assert_series_equal(result_and, expected_and)

        # OR with empty operands returns False
        result_or = boolean_evaluator.evaluate_bool_chain(
            "or",
            [],
            mock_evaluator,
        )
        expected_or = pd.Series([False, False, False, False])
        pd.testing.assert_series_equal(result_or, expected_or)

        # XOR with empty operands returns False
        result_xor = boolean_evaluator.evaluate_bool_chain(
            "xor",
            [],
            mock_evaluator,
        )
        expected_xor = pd.Series([False, False, False, False])
        pd.testing.assert_series_equal(result_xor, expected_xor)

    def test_evaluate_bool_chain_unsupported_operation(
        self,
        boolean_evaluator: BooleanExpressionEvaluator,
    ) -> None:
        """Test boolean chain evaluation with unsupported operation raises ValueError."""

        class MockExpressionEvaluator:
            @staticmethod
            def evaluate(expr):
                return pd.Series([True, False, True, False])

        mock_evaluator = MockExpressionEvaluator()

        with pytest.raises(ValueError, match="Unsupported boolean operator"):
            boolean_evaluator.evaluate_bool_chain(
                "nand",
                ["expr1"],
                mock_evaluator,
            )

    def test_null_safe_helper(
        self,
        boolean_evaluator: BooleanExpressionEvaluator,
    ) -> None:
        """Test null-safe helper function."""
        series = pd.Series([True, False, None, None])
        result = boolean_evaluator._null_safe(series)

        expected = pd.Series([True, False, False, False], dtype=object)
        pd.testing.assert_series_equal(result, expected)


class TestBooleanEvaluatorIntegration:
    """Test BooleanExpressionEvaluator integration scenarios."""

    @pytest.fixture
    def evaluator_and_mock(self) -> tuple[BooleanExpressionEvaluator, type]:
        """Create evaluator with a 6-row frame and a configurable mock."""
        df = pd.DataFrame(
            {
                ID_COLUMN: range(6),
                "X__val": [True, False, None, True, False, None],
            },
        )
        person_table = EntityTable.from_dataframe(
            "X",
            pd.DataFrame(
                {
                    ID_COLUMN: range(6),
                    "val": [True, False, None, True, False, None],
                },
            ),
        )
        context = Context(
            entity_mapping=EntityMapping(mapping={"X": person_table}),
        )
        frame = BindingFrame(
            bindings=df,
            context=context,
            type_registry={"x": "X"},
        )
        evaluator = BooleanExpressionEvaluator(frame)

        class MockEval:
            """Mock that maps string labels to predetermined series."""

            registry: dict[str, pd.Series] = {}

            @classmethod
            def evaluate(cls, expr: str) -> pd.Series:
                return cls.registry[expr]

        return evaluator, MockEval

    def test_and_three_operands(
        self,
        evaluator_and_mock: tuple[BooleanExpressionEvaluator, type],
    ) -> None:
        """AND fold across three operands propagates Kleene null correctly."""
        ev, mock = evaluator_and_mock
        mock.registry = {
            "a": pd.Series([True, True, True, True, True, True]),
            "b": pd.Series([True, True, False, True, True, True]),
            "c": pd.Series([True, False, True, True, True, True]),
        }
        result = ev.evaluate_and(["a", "b", "c"], mock)
        expected = pd.Series(
            [True, False, False, True, True, True],
            dtype=object,
        )
        pd.testing.assert_series_equal(result, expected)

    def test_or_three_operands(
        self,
        evaluator_and_mock: tuple[BooleanExpressionEvaluator, type],
    ) -> None:
        """OR fold across three operands."""
        ev, mock = evaluator_and_mock
        mock.registry = {
            "a": pd.Series([False, False, False, False, True, False]),
            "b": pd.Series([False, False, False, False, False, False]),
            "c": pd.Series([False, False, True, False, False, False]),
        }
        result = ev.evaluate_or(["a", "b", "c"], mock)
        expected = pd.Series(
            [False, False, True, False, True, False],
            dtype=object,
        )
        pd.testing.assert_series_equal(result, expected)

    def test_xor_three_operands(
        self,
        evaluator_and_mock: tuple[BooleanExpressionEvaluator, type],
    ) -> None:
        """XOR fold across three operands: (T ^ F) ^ T = False."""
        ev, mock = evaluator_and_mock
        mock.registry = {
            "a": pd.Series([True, False, True, False, True, False]),
            "b": pd.Series([False, False, True, True, False, False]),
            "c": pd.Series([True, True, True, True, True, True]),
        }
        result = ev.evaluate_xor(["a", "b", "c"], mock)
        expected = pd.Series(
            [False, True, True, False, False, True],
            dtype=object,
        )
        pd.testing.assert_series_equal(result, expected)

    def test_not_with_null_values(
        self,
        evaluator_and_mock: tuple[BooleanExpressionEvaluator, type],
    ) -> None:
        """NOT propagates null correctly."""
        ev, mock = evaluator_and_mock
        mock.registry = {
            "expr": pd.Series([True, False, None, True, None, False]),
        }
        result = ev.evaluate_not("expr", mock)
        expected = pd.Series(
            [False, True, None, False, None, True],
            dtype=object,
        )
        pd.testing.assert_series_equal(result, expected)

    def test_bool_chain_xor(
        self,
        evaluator_and_mock: tuple[BooleanExpressionEvaluator, type],
    ) -> None:
        """Boolean chain XOR with null-safe coercion."""
        ev, mock = evaluator_and_mock
        mock.registry = {
            "a": pd.Series([True, False, None, True, False, None]),
            "b": pd.Series([False, True, True, None, None, False]),
        }
        result = ev.evaluate_bool_chain("xor", ["a", "b"], mock)
        # null coerced to False: a=[T,F,F,T,F,F], b=[F,T,T,F,F,F]
        expected = pd.Series([True, True, True, True, False, False])
        pd.testing.assert_series_equal(result, expected)

    def test_single_operand_and(
        self,
        evaluator_and_mock: tuple[BooleanExpressionEvaluator, type],
    ) -> None:
        """AND with single operand returns that operand unchanged."""
        ev, mock = evaluator_and_mock
        mock.registry = {
            "only": pd.Series([True, False, True, False, True, False]),
        }
        result = ev.evaluate_and(["only"], mock)
        for i in range(6):
            assert result.iloc[i] == [True, False, True, False, True, False][i]

    def test_single_operand_or(
        self,
        evaluator_and_mock: tuple[BooleanExpressionEvaluator, type],
    ) -> None:
        """OR with single operand returns that operand unchanged."""
        ev, mock = evaluator_and_mock
        mock.registry = {
            "only": pd.Series([True, False, True, False, True, False]),
        }
        result = ev.evaluate_or(["only"], mock)
        for i in range(6):
            assert result.iloc[i] == [True, False, True, False, True, False][i]


class TestBooleanEvaluatorEdgeCases:
    """Test edge cases in Kleene three-valued logic."""

    def test_kleene_and_all_null(self) -> None:
        """AND with all null values returns null."""
        left = pd.Series([None, None, None])
        right = pd.Series([None, None, None])
        result = kleene_and(left, right)
        assert all(v is None for v in result)

    def test_kleene_or_all_null(self) -> None:
        """OR with all null values returns null."""
        left = pd.Series([None, None, None])
        right = pd.Series([None, None, None])
        result = kleene_or(left, right)
        assert all(v is None for v in result)

    def test_kleene_not_all_null(self) -> None:
        """NOT null returns null."""
        s = pd.Series([None, None, None])
        result = kleene_not(s)
        assert all(v is None for v in result)

    def test_kleene_and_empty_series(self) -> None:
        """AND with empty series returns empty series."""
        left = pd.Series([], dtype=object)
        right = pd.Series([], dtype=object)
        result = kleene_and(left, right)
        assert len(result) == 0

    def test_kleene_or_empty_series(self) -> None:
        """OR with empty series returns empty series."""
        left = pd.Series([], dtype=object)
        right = pd.Series([], dtype=object)
        result = kleene_or(left, right)
        assert len(result) == 0

    def test_kleene_xor_empty_series(self) -> None:
        """XOR with empty series returns empty series."""
        left = pd.Series([], dtype=object)
        right = pd.Series([], dtype=object)
        result = kleene_xor(left, right)
        assert len(result) == 0

    def test_kleene_not_empty_series(self) -> None:
        """NOT with empty series returns empty series."""
        s = pd.Series([], dtype=object)
        result = kleene_not(s)
        assert len(result) == 0

    def test_kleene_and_preserves_index(self) -> None:
        """AND preserves the index of the left operand."""
        left = pd.Series([True, False], index=[10, 20])
        right = pd.Series([True, True], index=[10, 20])
        result = kleene_and(left, right)
        assert list(result.index) == [10, 20]

    def test_kleene_or_preserves_index(self) -> None:
        """OR preserves the index of the left operand."""
        left = pd.Series([False, True], index=[5, 15])
        right = pd.Series([True, False], index=[5, 15])
        result = kleene_or(left, right)
        assert list(result.index) == [5, 15]

    def test_kleene_not_preserves_index(self) -> None:
        """NOT preserves the index."""
        s = pd.Series([True, False], index=[100, 200])
        result = kleene_not(s)
        assert list(result.index) == [100, 200]

    def test_kleene_xor_preserves_index(self) -> None:
        """XOR preserves the index of the left operand."""
        left = pd.Series([True, False], index=[3, 7])
        right = pd.Series([False, False], index=[3, 7])
        result = kleene_xor(left, right)
        assert list(result.index) == [3, 7]

    def test_kleene_and_single_element(self) -> None:
        """AND with single-element series."""
        result = kleene_and(pd.Series([True]), pd.Series([False]))
        assert result.iloc[0] is False or not result.iloc[0]

    def test_kleene_functions_return_object_dtype(self) -> None:
        """All Kleene functions return object dtype to allow None values."""
        left = pd.Series([True, None])
        right = pd.Series([False, True])
        assert kleene_and(left, right).dtype == object
        assert kleene_or(left, right).dtype == object
        assert kleene_xor(left, right).dtype == object
        assert kleene_not(left).dtype == object


class TestBooleanEvaluatorRegression:
    """Test BooleanExpressionEvaluator maintains compatibility with original behavior."""

    def test_and_null_false_returns_false(self) -> None:
        """Regression: null AND false must be false, not null."""
        left = pd.Series([None])
        right = pd.Series([False])
        result = kleene_and(left, right)
        assert not result.iloc[0]

    def test_or_null_true_returns_true(self) -> None:
        """Regression: null OR true must be true, not null."""
        left = pd.Series([None])
        right = pd.Series([True])
        result = kleene_or(left, right)
        assert result.iloc[0]

    def test_xor_null_anything_returns_null(self) -> None:
        """Regression: null XOR anything must be null."""
        left = pd.Series([None, None])
        right = pd.Series([True, False])
        result = kleene_xor(left, right)
        assert result.iloc[0] is None
        assert result.iloc[1] is None

    def test_not_null_returns_null(self) -> None:
        """Regression: NOT null must be null."""
        result = kleene_not(pd.Series([None]))
        assert result.iloc[0] is None

    def test_bool_chain_null_safe_coercion(self) -> None:
        """Regression: bool_chain must coerce nulls to False before folding."""
        df = pd.DataFrame({ID_COLUMN: [0, 1]})
        person_table = EntityTable.from_dataframe(
            "X",
            pd.DataFrame({ID_COLUMN: [0, 1], "v": [True, False]}),
        )
        context = Context(
            entity_mapping=EntityMapping(mapping={"X": person_table}),
        )
        frame = BindingFrame(
            bindings=df,
            context=context,
            type_registry={"x": "X"},
        )
        ev = BooleanExpressionEvaluator(frame)

        class MockEval:
            """Mock evaluator for regression test."""

            @staticmethod
            def evaluate(expr: str) -> pd.Series:
                if expr == "null_expr":
                    return pd.Series([None, None])
                return pd.Series([True, False])

        result = ev.evaluate_bool_chain(
            "and",
            ["null_expr", "true_false"],
            MockEval,
        )
        # null coerced to False: [False AND True, False AND False] = [False, False]
        assert not result.iloc[0]
        assert not result.iloc[1]
