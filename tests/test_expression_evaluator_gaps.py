"""Tests for ExpressionEvaluator coverage gaps.

Covers:
- NullLiteral and ListLiteral evaluation
- Error paths in property lookup and variable evaluation
- Unsupported expression type error
- evaluate_aggregation() dispatch (collect, count, sum, avg, min, max)
- Aggregation argument format variations
"""

from __future__ import annotations

import pandas as pd
import pytest

from pycypher.ast_models import (
    BooleanLiteral,
    CountStar,
    FloatLiteral,
    FunctionInvocation,
    IntegerLiteral,
    ListLiteral,
    NullLiteral,
    PropertyLookup,
    StringLiteral,
    Variable,
)
from pycypher.expression_evaluator import ExpressionEvaluator
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    Projection,
)


# ---------------------------------------------------------------------------
# Shared fixtures (same pattern as test_arithmetic_operations.py)
# ---------------------------------------------------------------------------


@pytest.fixture()
def person_data() -> pd.DataFrame:
    """Raw Person entity data."""
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        }
    )


@pytest.fixture()
def person_context(person_data: pd.DataFrame) -> Context:
    """Context with Person entities."""
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=person_data,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


@pytest.fixture()
def person_relation(person_context: Context) -> Projection:
    """Projection relation with variable 'p' → Person."""
    table = person_context.entity_mapping["Person"]
    rel = Projection(
        relation=table,
        projected_column_names={f"Person__{ID_COLUMN}": f"Person__{ID_COLUMN}"},
    )
    rel.variable_map = {Variable(name="p"): f"Person__{ID_COLUMN}"}
    rel.variable_type_map = {Variable(name="p"): "Person"}
    return rel


# ---------------------------------------------------------------------------
# Literal evaluation branches
# ---------------------------------------------------------------------------


class TestLiteralBranches:
    """Cover NullLiteral and ListLiteral evaluate() branches."""

    def test_null_literal(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """NullLiteral produces a Series of None values."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)
        result, name = evaluator.evaluate(NullLiteral(), df)
        assert name == "null"
        assert len(result) == len(df)
        assert result.isna().all()

    def test_list_literal(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """ListLiteral broadcasts the list to every row."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)
        result, name = evaluator.evaluate(ListLiteral(value=[1, 2, 3]), df)
        assert name == "list"
        assert len(result) == len(df)
        assert all(v == [1, 2, 3] for v in result)


# ---------------------------------------------------------------------------
# Unsupported expression type
# ---------------------------------------------------------------------------


class TestUnsupportedExpression:
    """Cover the default match-case branch in evaluate()."""

    def test_unsupported_expression_raises(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """An unrecognized expression type raises NotImplementedError."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)

        # Use a plain Expression subclass that doesn't match any case
        class _FakeExpr:
            pass

        with pytest.raises(NotImplementedError, match="not yet supported"):
            evaluator.evaluate(_FakeExpr(), df)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Property lookup error paths
# ---------------------------------------------------------------------------


class TestPropertyLookupErrors:
    """Cover ValueError branches in _evaluate_property_lookup."""

    def test_unknown_variable_raises(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Property lookup on a variable not in the relation raises ValueError."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)
        expr = PropertyLookup(expression=Variable(name="z"), property="name")
        with pytest.raises(ValueError, match="Variable z not found"):
            evaluator.evaluate(expr, df)

    def test_missing_type_info_raises(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Property lookup with variable that has no type info raises ValueError."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)

        # Add a variable with no type info
        untyped = Variable(name="u")
        person_relation.variable_map[untyped] = f"Person__{ID_COLUMN}"
        # deliberately NOT adding to variable_type_map

        expr = PropertyLookup(expression=Variable(name="u"), property="name")
        with pytest.raises(ValueError, match="has no type information"):
            evaluator.evaluate(expr, df)


# ---------------------------------------------------------------------------
# Variable evaluation error paths
# ---------------------------------------------------------------------------


class TestVariableErrors:
    """Cover ValueError branch in _evaluate_variable."""

    def test_unknown_variable_raises(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Evaluating an unknown variable raises ValueError."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)
        with pytest.raises(ValueError, match="Variable q not found"):
            evaluator.evaluate(Variable(name="q"), df)


# ---------------------------------------------------------------------------
# Aggregation dispatch (evaluate_aggregation)
# ---------------------------------------------------------------------------


class TestEvaluateAggregation:
    """Cover all aggregation dispatch branches."""

    def _make_evaluator(
        self, context: Context, relation: Projection
    ) -> tuple[ExpressionEvaluator, pd.DataFrame]:
        evaluator = ExpressionEvaluator(context=context, relation=relation)
        df = relation.to_pandas(context=context)
        return evaluator, df

    def _age_invocation(self, func_name: str) -> FunctionInvocation:
        """Helper: FunctionInvocation of func_name(p.age)."""
        return FunctionInvocation(
            name=func_name,
            arguments={
                "arguments": [
                    PropertyLookup(expression=Variable(name="p"), property="age"),
                ]
            },
        )

    # -- COUNT(*) --

    def test_count_star(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """COUNT(*) returns total row count."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        result = evaluator.evaluate_aggregation(CountStar(), df)
        assert result == 3

    # -- collect --

    def test_collect(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """collect(p.age) returns list of all values."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        result = evaluator.evaluate_aggregation(self._age_invocation("collect"), df)
        assert result == [25, 30, 35]

    # -- count --

    def test_count(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """count(p.age) returns count of non-null values."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        result = evaluator.evaluate_aggregation(self._age_invocation("count"), df)
        assert result == 3

    def test_count_no_args(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """count() with no args treated as count(*)."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        expr = FunctionInvocation(name="count", arguments={})
        result = evaluator.evaluate_aggregation(expr, df)
        assert result == 3

    # -- sum --

    def test_sum(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """sum(p.age) returns sum of values."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        result = evaluator.evaluate_aggregation(self._age_invocation("sum"), df)
        assert result == 90.0

    # -- avg --

    def test_avg(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """avg(p.age) returns mean of values."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        result = evaluator.evaluate_aggregation(self._age_invocation("avg"), df)
        assert result == 30.0

    # -- min / max --

    def test_min(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """min(p.age) returns minimum value."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        result = evaluator.evaluate_aggregation(self._age_invocation("min"), df)
        assert result == 25

    def test_max(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """max(p.age) returns maximum value."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        result = evaluator.evaluate_aggregation(self._age_invocation("max"), df)
        assert result == 35

    # -- unsupported aggregation --

    def test_unsupported_aggregation_raises(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Unsupported aggregation function raises ValueError."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        expr = FunctionInvocation(
            name="percentile",
            arguments={"arguments": [IntegerLiteral(value=1)]},
        )
        with pytest.raises(ValueError, match="Unsupported aggregation"):
            evaluator.evaluate_aggregation(expr, df)

    # -- non-FunctionInvocation raises --

    def test_non_function_raises(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Passing a non-FunctionInvocation/CountStar raises ValueError."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        with pytest.raises(ValueError, match="Expected FunctionInvocation"):
            evaluator.evaluate_aggregation(IntegerLiteral(value=1), df)

    # -- argument requires expression --

    def test_missing_arg_raises(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Aggregation (non-count) without arg expression raises ValueError."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        expr = FunctionInvocation(name="sum", arguments={})
        with pytest.raises(ValueError, match="requires an argument"):
            evaluator.evaluate_aggregation(expr, df)

    # -- argument format: 'args' key --

    def test_args_key_format(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Arguments dict with 'args' key is handled."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        expr = FunctionInvocation(
            name="collect",
            arguments={
                "args": [
                    PropertyLookup(expression=Variable(name="p"), property="age"),
                ]
            },
        )
        result = evaluator.evaluate_aggregation(expr, df)
        assert result == [25, 30, 35]

    # -- argument format: 'expression' key --

    def test_expression_key_format(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Arguments dict with 'expression' key is handled."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        expr = FunctionInvocation(
            name="count",
            arguments={
                "expression": PropertyLookup(
                    expression=Variable(name="p"), property="age"
                ),
            },
        )
        result = evaluator.evaluate_aggregation(expr, df)
        assert result == 3

    # -- namespaced function name --

    def test_namespaced_function_name(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Dict-style function name is extracted correctly."""
        evaluator, df = self._make_evaluator(person_context, person_relation)
        expr = FunctionInvocation(
            name={"namespace": "math", "name": "count"},
            arguments={
                "arguments": [
                    PropertyLookup(expression=Variable(name="p"), property="age"),
                ]
            },
        )
        result = evaluator.evaluate_aggregation(expr, df)
        assert result == 3


# ---------------------------------------------------------------------------
# Scalar function argument format: 'args' key
# ---------------------------------------------------------------------------


class TestScalarFunctionArgFormats:
    """Cover arg-parsing branches in _evaluate_scalar_function."""

    def test_args_key_format(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """'args' dict key is handled for scalar functions."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)
        expr = FunctionInvocation(
            name="toUpper",
            arguments={
                "args": [
                    PropertyLookup(expression=Variable(name="p"), property="name"),
                ]
            },
        )
        result, _ = evaluator.evaluate(expr, df)
        assert result.tolist() == ["ALICE", "BOB", "CHARLIE"]

    def test_namespaced_scalar_function(
        self, person_context: Context, person_relation: Projection
    ) -> None:
        """Dict-style (namespaced) function name is extracted for scalar functions."""
        evaluator = ExpressionEvaluator(
            context=person_context, relation=person_relation
        )
        df = person_relation.to_pandas(context=person_context)
        expr = FunctionInvocation(
            name={"namespace": "cypher", "name": "toUpper"},
            arguments={
                "arguments": [
                    PropertyLookup(expression=Variable(name="p"), property="name"),
                ]
            },
        )
        result, _ = evaluator.evaluate(expr, df)
        assert result.tolist() == ["ALICE", "BOB", "CHARLIE"]
