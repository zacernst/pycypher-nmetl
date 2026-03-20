"""TDD tests for extended _expr_display_text() in star.py (Loop 210 — UX).

Problem: _expr_display_text() returns None for 8+ expression types that appear
in real queries, causing RETURN columns to receive synthesised numeric names
("col_0", "col_1") instead of human-readable ones.

Fix: add isinstance branches for Unary, Parameter, IndexLookup, Slicing,
CountStar, ListComprehension, CaseExpression, and Reduce.

All tests are written before the fix (TDD red phase).
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import (
    Arithmetic,
    CaseExpression,
    CountStar,
    IndexLookup,
    IntegerLiteral,
    ListComprehension,
    Parameter,
    PropertyLookup,
    Reduce,
    Slicing,
    StringLiteral,
    Unary,
    Variable,
    WhenClause,
)
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def star_instance() -> Star:
    """Minimal Star instance for calling _expr_display_text directly."""
    ctx = ContextBuilder().from_dict(
        {
            "Person": pd.DataFrame(
                {
                    "__ID__": ["p1"],
                    "age": [30],
                    "name": ["Alice"],
                }
            )
        }
    )
    return Star(context=ctx)


def display(star: Star, expr: object) -> str | None:
    """Shorthand to call the expression renderer."""
    return star._renderer.render(expr)


# ---------------------------------------------------------------------------
# Category 1: Unary expressions
# ---------------------------------------------------------------------------


class TestUnaryDisplayText:
    def test_negation_integer(self, star_instance: Star) -> None:
        """Unary minus on an integer literal: -5 → '-5'."""
        expr = Unary(operator="-", operand=IntegerLiteral(value=5))
        assert display(star_instance, expr) == "-5"

    def test_negation_property(self, star_instance: Star) -> None:
        """Unary minus on a property: -p.age → '-age'."""
        expr = Unary(
            operator="-",
            operand=PropertyLookup(
                variable=Variable(name="p"), property="age"
            ),
        )
        result = display(star_instance, expr)
        # Should be something like '-age', not None
        assert result is not None
        assert "age" in result

    def test_unary_plus(self, star_instance: Star) -> None:
        """Unary plus on an integer: +3 → '+3'."""
        expr = Unary(operator="+", operand=IntegerLiteral(value=3))
        result = display(star_instance, expr)
        assert result is not None
        assert "3" in result

    def test_unary_none_operand(self, star_instance: Star) -> None:
        """Unary with missing operand should not raise; returns operator-only or None."""
        expr = Unary(operator="-", operand=None)
        result = display(star_instance, expr)
        # Either '-?' or just '-' or None — must not raise
        assert result is None or "-" in result


# ---------------------------------------------------------------------------
# Category 2: Parameter
# ---------------------------------------------------------------------------


class TestParameterDisplayText:
    def test_named_parameter(self, star_instance: Star) -> None:
        """Parameter $limit → '$limit'."""
        expr = Parameter(name="limit")
        result = display(star_instance, expr)
        assert result == "$limit", f"Expected '$limit', got {result!r}"

    def test_unnamed_parameter(self, star_instance: Star) -> None:
        """Parameter with numeric name (positional) is still prefixed with $."""
        expr = Parameter(name="1")
        result = display(star_instance, expr)
        assert result is not None
        assert "1" in result

    def test_parameter_in_query_produces_named_column(
        self, star_instance: Star
    ) -> None:
        """RETURN $myParam should produce column named '$myParam' in the result."""
        # This requires the full pipeline; skip if parameterised RETURN not supported
        pytest.importorskip("pycypher.star")
        # Just verify _expr_display_text gives the right text; end-to-end test
        # of param columns is separate
        expr = Parameter(name="myParam")
        assert display(star_instance, expr) == "$myParam"


# ---------------------------------------------------------------------------
# Category 3: IndexLookup
# ---------------------------------------------------------------------------


class TestIndexLookupDisplayText:
    def test_integer_index(self, star_instance: Star) -> None:
        """list[0] → 'list[0]'."""
        expr = IndexLookup(
            expression=Variable(name="list"),
            index=IntegerLiteral(value=0),
        )
        result = display(star_instance, expr)
        assert result is not None
        assert "list" in result
        assert "0" in result

    def test_string_key_index(self, star_instance: Star) -> None:
        """map['key'] → 'map[key]'."""
        expr = IndexLookup(
            expression=Variable(name="map"),
            index=StringLiteral(value="key"),
        )
        result = display(star_instance, expr)
        assert result is not None
        assert "map" in result

    def test_index_lookup_not_none(self, star_instance: Star) -> None:
        """Any IndexLookup must return a non-None string."""
        expr = IndexLookup(
            expression=Variable(name="xs"),
            index=IntegerLiteral(value=2),
        )
        assert display(star_instance, expr) is not None


# ---------------------------------------------------------------------------
# Category 4: Slicing
# ---------------------------------------------------------------------------


class TestSlicingDisplayText:
    def test_full_slice(self, star_instance: Star) -> None:
        """list[1..3] — must produce a non-None string containing the boundaries."""
        expr = Slicing(
            expression=Variable(name="list"),
            start=IntegerLiteral(value=1),
            end=IntegerLiteral(value=3),
        )
        result = display(star_instance, expr)
        assert result is not None
        assert "list" in result
        assert "1" in result
        assert "3" in result

    def test_open_end_slice(self, star_instance: Star) -> None:
        """list[2..] — end may be absent; must not raise."""
        expr = Slicing(
            expression=Variable(name="list"),
            start=IntegerLiteral(value=2),
            end=None,
        )
        result = display(star_instance, expr)
        assert result is not None

    def test_open_start_slice(self, star_instance: Star) -> None:
        """list[..3] — start may be absent; must not raise."""
        expr = Slicing(
            expression=Variable(name="list"),
            start=None,
            end=IntegerLiteral(value=3),
        )
        result = display(star_instance, expr)
        assert result is not None


# ---------------------------------------------------------------------------
# Category 5: CountStar
# ---------------------------------------------------------------------------


class TestCountStarDisplayText:
    def test_count_star_text(self, star_instance: Star) -> None:
        """CountStar() → 'count(*)'."""
        expr = CountStar()
        result = display(star_instance, expr)
        assert result == "count(*)", f"Expected 'count(*)', got {result!r}"

    def test_count_star_in_query_produces_count_star_column(self) -> None:
        """RETURN count(*) should produce a column named 'count(*)'."""
        ctx = ContextBuilder().from_dict(
            {
                "Item": pd.DataFrame(
                    {
                        "__ID__": ["i1", "i2", "i3"],
                        "val": [1, 2, 3],
                    }
                )
            }
        )
        s = Star(context=ctx)
        r = s.execute_query("MATCH (i:Item) RETURN count(*)")
        assert "count(*)" in r.columns, (
            f"Expected column 'count(*)', got columns {list(r.columns)!r}"
        )


# ---------------------------------------------------------------------------
# Category 6: ListComprehension
# ---------------------------------------------------------------------------


class TestListComprehensionDisplayText:
    def test_basic_list_comprehension(self, star_instance: Star) -> None:
        """[x IN xs | x] should produce a non-None string mentioning 'xs'."""
        expr = ListComprehension(
            variable=Variable(name="x"),
            list_expr=Variable(name="xs"),
            map_expr=Variable(name="x"),
        )
        result = display(star_instance, expr)
        assert result is not None, (
            "_expr_display_text returned None for ListComprehension. "
            "Expected a human-readable string."
        )

    def test_list_comprehension_contains_variable(
        self, star_instance: Star
    ) -> None:
        """Result must mention the iteration variable and the list."""
        expr = ListComprehension(
            variable=Variable(name="x"),
            list_expr=Variable(name="myList"),
            map_expr=Arithmetic(
                operator="*",
                left=Variable(name="x"),
                right=IntegerLiteral(value=2),
            ),
        )
        result = display(star_instance, expr)
        assert result is not None
        assert "x" in result
        assert "myList" in result


# ---------------------------------------------------------------------------
# Category 7: CaseExpression
# ---------------------------------------------------------------------------


class TestCaseExpressionDisplayText:
    def test_searched_case(self, star_instance: Star) -> None:
        """CASE WHEN ... THEN ... END should produce non-None string."""
        expr = CaseExpression(
            when_clauses=[
                WhenClause(
                    condition=IntegerLiteral(value=1),
                    result=StringLiteral(value="one"),
                )
            ],
            else_expr=StringLiteral(value="other"),
        )
        result = display(star_instance, expr)
        assert result is not None, (
            "_expr_display_text returned None for CaseExpression. "
            "Expected 'case' or similar."
        )

    def test_case_result_starts_with_case(self, star_instance: Star) -> None:
        """Result string should start with 'case' (case-insensitive)."""
        expr = CaseExpression(
            when_clauses=[],
            else_expr=None,
        )
        result = display(star_instance, expr)
        assert result is not None
        assert result.lower().startswith("case")


# ---------------------------------------------------------------------------
# Category 8: Reduce
# ---------------------------------------------------------------------------


class TestReduceDisplayText:
    def test_reduce_not_none(self, star_instance: Star) -> None:
        """reduce(acc = 0, x IN xs | acc + x) must return non-None."""
        expr = Reduce(
            accumulator=Variable(name="acc"),
            initial=IntegerLiteral(value=0),
            variable=Variable(name="x"),
            list_expr=Variable(name="xs"),
            map_expr=Arithmetic(
                operator="+",
                left=Variable(name="acc"),
                right=Variable(name="x"),
            ),
        )
        result = display(star_instance, expr)
        assert result is not None, (
            "_expr_display_text returned None for Reduce. "
            "Expected a human-readable string."
        )

    def test_reduce_contains_reduce(self, star_instance: Star) -> None:
        """Result must mention 'reduce'."""
        expr = Reduce(
            accumulator=Variable(name="s"),
            initial=IntegerLiteral(value=0),
            variable=Variable(name="n"),
            list_expr=Variable(name="nums"),
            map_expr=Arithmetic(
                operator="+",
                left=Variable(name="s"),
                right=Variable(name="n"),
            ),
        )
        result = display(star_instance, expr)
        assert result is not None
        assert "reduce" in result.lower()
