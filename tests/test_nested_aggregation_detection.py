"""Tests for recursive aggregation detection in _contains_aggregation.

Flaw: _contains_aggregation only checks the outermost AST node.
Queries like `RETURN count(p.name) + 1` incorrectly fall into the
simple-projection path, producing wrong results.

TDD red phase.
"""

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star


@pytest.fixture
def simple_context() -> Context:
    """Three people — minimal context for aggregation tests."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "salary": [90_000, 80_000, 70_000],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "salary"],
        source_obj_attribute_map={"name": "name", "salary": "salary"},
        attribute_map={"name": "name", "salary": "salary"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Unit tests on Star._contains_aggregation
# ─────────────────────────────────────────────────────────────────────────────


class TestContainsAggregationUnit:
    """Test _contains_aggregation directly via Star helper, using hand-built AST nodes."""

    def _star(self, context: Context) -> Star:
        return Star(context=context)

    def test_count_star_is_aggregation(self, simple_context: Context) -> None:
        from pycypher.ast_models import CountStar

        star = self._star(simple_context)
        assert star._contains_aggregation(CountStar()) is True

    def test_function_invocation_count_is_aggregation(
        self,
        simple_context: Context,
    ) -> None:
        from pycypher.ast_models import (
            FunctionInvocation,
            PropertyLookup,
            Variable,
        )

        star = self._star(simple_context)
        expr = FunctionInvocation(
            name="count",
            arguments={
                "arguments": [
                    PropertyLookup(
                        expression=Variable(name="n"),
                        property="name",
                    ),
                ],
            },
        )
        assert star._contains_aggregation(expr) is True

    def test_function_invocation_sum_is_aggregation(
        self,
        simple_context: Context,
    ) -> None:
        from pycypher.ast_models import (
            FunctionInvocation,
            PropertyLookup,
            Variable,
        )

        star = self._star(simple_context)
        expr = FunctionInvocation(
            name="sum",
            arguments={
                "arguments": [
                    PropertyLookup(
                        expression=Variable(name="n"),
                        property="salary",
                    ),
                ],
            },
        )
        assert star._contains_aggregation(expr) is True

    def test_plain_property_is_not_aggregation(
        self,
        simple_context: Context,
    ) -> None:
        from pycypher.ast_models import PropertyLookup, Variable

        star = self._star(simple_context)
        expr = PropertyLookup(expression=Variable(name="n"), property="name")
        assert star._contains_aggregation(expr) is False

    def test_scalar_function_is_not_aggregation(
        self,
        simple_context: Context,
    ) -> None:
        from pycypher.ast_models import (
            FunctionInvocation,
            PropertyLookup,
            Variable,
        )

        star = self._star(simple_context)
        expr = FunctionInvocation(
            name="toUpper",
            arguments={
                "arguments": [
                    PropertyLookup(
                        expression=Variable(name="n"),
                        property="name",
                    ),
                ],
            },
        )
        assert star._contains_aggregation(expr) is False

    def test_arithmetic_of_non_agg_is_not_aggregation(
        self,
        simple_context: Context,
    ) -> None:
        from pycypher.ast_models import (
            Arithmetic,
            IntegerLiteral,
            PropertyLookup,
            Variable,
        )

        star = self._star(simple_context)
        expr = Arithmetic(
            operator="+",
            left=PropertyLookup(
                expression=Variable(name="n"),
                property="salary",
            ),
            right=IntegerLiteral(value=1),
        )
        assert star._contains_aggregation(expr) is False

    def test_aggregation_plus_literal_is_aggregation(
        self,
        simple_context: Context,
    ) -> None:
        """count(n.name) + 1 — aggregation nested inside Arithmetic."""
        from pycypher.ast_models import (
            Arithmetic,
            FunctionInvocation,
            IntegerLiteral,
            PropertyLookup,
            Variable,
        )

        star = self._star(simple_context)
        count_expr = FunctionInvocation(
            name="count",
            arguments={
                "arguments": [
                    PropertyLookup(
                        expression=Variable(name="n"),
                        property="name",
                    ),
                ],
            },
        )
        expr = Arithmetic(
            operator="+",
            left=count_expr,
            right=IntegerLiteral(value=1),
        )
        assert star._contains_aggregation(expr) is True

    def test_aggregation_times_literal_is_aggregation(
        self,
        simple_context: Context,
    ) -> None:
        """sum(n.salary) * 2 — aggregation nested inside Arithmetic."""
        from pycypher.ast_models import (
            Arithmetic,
            FunctionInvocation,
            IntegerLiteral,
            PropertyLookup,
            Variable,
        )

        star = self._star(simple_context)
        sum_expr = FunctionInvocation(
            name="sum",
            arguments={
                "arguments": [
                    PropertyLookup(
                        expression=Variable(name="n"),
                        property="salary",
                    ),
                ],
            },
        )
        expr = Arithmetic(
            operator="*",
            left=sum_expr,
            right=IntegerLiteral(value=2),
        )
        assert star._contains_aggregation(expr) is True

    def test_two_aggregations_added_is_aggregation(
        self,
        simple_context: Context,
    ) -> None:
        """sum(n.salary) + count(*) — aggregations in both branches."""
        from pycypher.ast_models import (
            Arithmetic,
            CountStar,
            FunctionInvocation,
            PropertyLookup,
            Variable,
        )

        star = self._star(simple_context)
        sum_expr = FunctionInvocation(
            name="sum",
            arguments={
                "arguments": [
                    PropertyLookup(
                        expression=Variable(name="n"),
                        property="salary",
                    ),
                ],
            },
        )
        expr = Arithmetic(
            operator="+",
            left=sum_expr,
            right=CountStar(),
        )
        assert star._contains_aggregation(expr) is True


# ─────────────────────────────────────────────────────────────────────────────
# Execution correctness tests — nested aggregation in RETURN
# ─────────────────────────────────────────────────────────────────────────────


class TestNestedAggregationExecution:
    """End-to-end query tests for aggregation-containing complex expressions."""

    def test_count_plus_one(self, simple_context: Context) -> None:
        """RETURN count(*) + 1 — should be 4 (3 people + 1)."""
        star = Star(context=simple_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN count(*) + 1 AS n",
        )
        assert len(result) == 1
        assert result["n"].iloc[0] == 4

    def test_sum_times_two(self, simple_context: Context) -> None:
        """RETURN sum(p.salary) * 2 — should be 480_000."""
        star = Star(context=simple_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN sum(p.salary) * 2 AS total",
        )
        assert len(result) == 1
        assert result["total"].iloc[0] == 480_000

    def test_with_count_plus_one(self, simple_context: Context) -> None:
        """WITH count(*) + 1 AS n RETURN n — same as above but via WITH."""
        star = Star(context=simple_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH count(*) + 1 AS n RETURN n",
        )
        assert len(result) == 1
        assert result["n"].iloc[0] == 4
