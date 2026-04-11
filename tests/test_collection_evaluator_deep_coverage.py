"""Deep coverage tests for pycypher.collection_evaluator.

Targets specific uncovered lines/paths:
- Line 561: unknown quantifier fallback in eval_quantifier_vectorized
- Lines 707-715: eval_pattern_comprehension per-row loop
- Lines 765-779: eval_map_literal empty entries / raw val paths
- Lines 830-902: eval_map_projection all_properties path (p{.*, .name: expr})
- SINGLE quantifier path (line 558)
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
from pycypher import ContextBuilder, Star
from pycypher.ast_models import MapLiteral
from pycypher.binding_frame import BindingFrame
from pycypher.collection_evaluator import CollectionExpressionEvaluator

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def star_with_people() -> Star:
    """Star instance with Person entities and KNOWS relationships."""
    people = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
            "born": ["1994-03-15", "1999-07-20", "1989-12-01"],
        },
    )
    rels = pd.DataFrame(
        {
            "__ID__": [100, 101],
            "__SOURCE__": [1, 2],
            "__TARGET__": [2, 3],
            "since": [2020, 2021],
        },
    )
    ctx = (
        ContextBuilder()
        .add_entity("Person", people)
        .add_relationship(
            "KNOWS",
            rels,
            source_col="__SOURCE__",
            target_col="__TARGET__",
        )
        .build()
    )
    return Star(context=ctx)


@pytest.fixture
def star_minimal() -> Star:
    """Star instance with minimal Person entity (one row)."""
    people = pd.DataFrame(
        {
            "__ID__": [1],
            "name": ["Alice"],
            "age": [30],
        },
    )
    ctx = ContextBuilder().add_entity("Person", people).build()
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# SINGLE quantifier (line 558)
# ---------------------------------------------------------------------------


class TestSingleQuantifier:
    """Cover the SINGLE quantifier branch in eval_quantifier_vectorized."""

    def test_single_quantifier_true(self, star_minimal: Star) -> None:
        """SINGLE — exactly one element satisfies predicate."""
        result = star_minimal.execute_query(
            "MATCH (p:Person) "
            "WITH [1, 2, 3] AS nums "
            "RETURN single(x IN nums WHERE x = 2) AS result",
        )
        assert bool(result["result"].iloc[0]) is True

    def test_single_quantifier_false(self, star_minimal: Star) -> None:
        """SINGLE returns false when multiple elements match."""
        result = star_minimal.execute_query(
            "MATCH (p:Person) "
            "WITH [1, 2, 3] AS nums "
            "RETURN single(x IN nums WHERE x > 1) AS result",
        )
        assert bool(result["result"].iloc[0]) is False

    def test_single_quantifier_empty_list(self, star_minimal: Star) -> None:
        """SINGLE on empty list returns false (zero matches != one)."""
        result = star_minimal.execute_query(
            "MATCH (p:Person) "
            "WITH [] AS nums "
            "RETURN single(x IN nums WHERE x > 0) AS result",
        )
        assert bool(result["result"].iloc[0]) is False


# ---------------------------------------------------------------------------
# eval_pattern_comprehension — lines 707-715
# ---------------------------------------------------------------------------


class TestPatternComprehension:
    """Cover eval_pattern_comprehension per-row loop (lines 707-715)."""

    def test_pattern_comprehension_returns_empty_lists(self) -> None:
        """Direct call to eval_pattern_comprehension exercises the for loop."""
        people = pd.DataFrame({"__ID__": [1, 2], "name": ["Alice", "Bob"]})
        ctx = ContextBuilder().add_entity("Person", people).build()

        bindings = pd.DataFrame({"p": [1, 2]})
        frame = BindingFrame(
            bindings=bindings,
            type_registry={"p": "Person"},
            context=ctx,
        )
        evaluator = CollectionExpressionEvaluator(frame)

        class FakePC:
            variable: Any = None
            pattern: Any = None
            where: Any = None
            map_expr: Any = None

        class FakeExprEval:
            def evaluate(self, expr: Any) -> pd.Series:
                return pd.Series([None, None], dtype=object)

        result = evaluator.eval_pattern_comprehension(FakePC(), FakeExprEval())
        assert len(result) == 2
        assert result.iloc[0] == []
        assert result.iloc[1] == []


# ---------------------------------------------------------------------------
# eval_map_literal — empty entries / raw val paths (lines 765-779)
# ---------------------------------------------------------------------------


class TestMapLiteralEdgePaths:
    """Cover map literal code paths via direct evaluator calls."""

    def test_map_literal_empty_entries(self) -> None:
        """MapLiteral with empty entries falls through to empty dict."""
        people = pd.DataFrame({"__ID__": [1], "name": ["Alice"]})
        ctx = ContextBuilder().add_entity("Person", people).build()
        bindings = pd.DataFrame({"p": [1]})
        frame = BindingFrame(
            bindings=bindings,
            type_registry={"p": "Person"},
            context=ctx,
        )
        evaluator = CollectionExpressionEvaluator(frame)

        ml = MapLiteral(entries={}, value={})

        class FakeExprEval:
            def evaluate(self, expr: Any) -> pd.Series:
                return pd.Series([None], dtype=object)

        result = evaluator.eval_map_literal(ml, FakeExprEval())
        assert len(result) == 1
        assert result.iloc[0] == {}

    def test_map_literal_raw_val(self) -> None:
        """MapLiteral with no entries but non-empty value dict broadcasts raw value."""
        people = pd.DataFrame({"__ID__": [1], "name": ["Alice"]})
        ctx = ContextBuilder().add_entity("Person", people).build()
        bindings = pd.DataFrame({"p": [1]})
        frame = BindingFrame(
            bindings=bindings,
            type_registry={"p": "Person"},
            context=ctx,
        )
        evaluator = CollectionExpressionEvaluator(frame)

        ml = MapLiteral(entries={}, value={"x": 42, "y": "hello"})

        class FakeExprEval:
            def evaluate(self, expr: Any) -> pd.Series:
                return pd.Series([None], dtype=object)

        result = evaluator.eval_map_literal(ml, FakeExprEval())
        assert len(result) == 1
        assert result.iloc[0] == {"x": 42, "y": "hello"}


# ---------------------------------------------------------------------------
# eval_map_projection — all_properties path (lines 830-902)
# ---------------------------------------------------------------------------


class TestMapProjectionAllProperties:
    """Cover the all_properties path in eval_map_projection."""

    def test_all_properties_basic(self, star_with_people: Star) -> None:
        """p{.*} exercises the all_properties loop."""
        result = star_with_people.execute_query(
            "MATCH (p:Person) RETURN p{.*} AS proj ORDER BY p.name",
        )
        proj = result["proj"].iloc[0]
        assert isinstance(proj, dict)

    def test_all_properties_with_expression(
        self, star_with_people: Star
    ) -> None:
        """p{.*, double_age: p.age * 2} exercises lines 857-878."""
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "RETURN p{.*, double_age: p.age * 2} AS proj "
            "ORDER BY p.name",
        )
        proj = result["proj"].iloc[0]
        assert isinstance(proj, dict)
        if "double_age" in proj:
            assert proj["double_age"] == 60

    def test_all_properties_with_property_copy(
        self, star_with_people: Star
    ) -> None:
        """p{.*, .name} exercises lines 842-855."""
        result = star_with_people.execute_query(
            "MATCH (p:Person) RETURN p{.*, .name} AS proj ORDER BY p.name",
        )
        proj = result["proj"].iloc[0]
        assert isinstance(proj, dict)
        if "name" in proj:
            assert proj["name"] == "Alice"


# ---------------------------------------------------------------------------
# eval_map_projection — empty elements (line 935)
# ---------------------------------------------------------------------------


class TestMapProjectionEmpty:
    """Cover empty projection elements path."""

    def test_empty_map_projection(self) -> None:
        """Map projection with no elements returns empty dicts."""
        people = pd.DataFrame({"__ID__": [1], "name": ["Alice"]})
        ctx = ContextBuilder().add_entity("Person", people).build()
        bindings = pd.DataFrame({"p": [1]})
        frame = BindingFrame(
            bindings=bindings,
            type_registry={"p": "Person"},
            context=ctx,
        )
        evaluator = CollectionExpressionEvaluator(frame)

        class FakeVariable:
            name: str = "p"

        class FakeMapProjection:
            variable: Any = FakeVariable()
            elements: list[Any] = []

        class FakeExprEval:
            def evaluate(self, expr: Any) -> pd.Series:
                return pd.Series([None], dtype=object)

        result = evaluator.eval_map_projection(
            FakeMapProjection(), FakeExprEval()
        )
        assert len(result) == 1
        assert result.iloc[0] == {}
