"""Tests for star.py coverage gaps.

Covers:
- _from_with_clause validation (WHERE, ORDER BY, DISTINCT, SKIP/LIMIT, missing alias)
- _from_return_clause validation (ORDER BY, DISTINCT, SKIP/LIMIT, missing alias)
- execute_query happy-path and error paths
- to_pandas delegation
- _contains_aggregation with various inputs (already partly covered, extending)
"""

from __future__ import annotations

import pandas as pd
import pytest

from pycypher.ast_models import (
    CountStar,
    FunctionInvocation,
    IntegerLiteral,
    PropertyLookup,
    Return,
    ReturnItem,
    StringLiteral,
    Variable,
    With,
)
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    Projection,
)
from pycypher.star import Star


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def person_context() -> Context:
    """Context with Person entities."""
    data = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=data,
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


@pytest.fixture()
def star(person_context: Context) -> Star:
    """Star instance with person context."""
    return Star(context=person_context)


# ---------------------------------------------------------------------------
# _from_with_clause validation errors
# ---------------------------------------------------------------------------


class TestWithClauseValidation:
    """Cover NotImplementedError / ValueError branches in _from_with_clause."""

    def test_where_not_supported(
        self, star: Star, person_relation: Projection
    ) -> None:
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    ),
                    alias="name",
                )
            ],
            where=IntegerLiteral(value=1),  # any non-None expression
        )
        with pytest.raises(NotImplementedError, match="WHERE"):
            star._from_with_clause(with_clause, person_relation)

    def test_order_by_not_supported(
        self, star: Star, person_relation: Projection
    ) -> None:
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    ),
                    alias="name",
                )
            ],
            order_by=[],  # non-None triggers the check
        )
        with pytest.raises(NotImplementedError, match="ORDER BY"):
            star._from_with_clause(with_clause, person_relation)

    def test_distinct_not_supported(
        self, star: Star, person_relation: Projection
    ) -> None:
        with_clause = With(
            distinct=True,
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    ),
                    alias="name",
                )
            ],
        )
        with pytest.raises(NotImplementedError, match="DISTINCT"):
            star._from_with_clause(with_clause, person_relation)

    def test_skip_not_supported(
        self, star: Star, person_relation: Projection
    ) -> None:
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    ),
                    alias="name",
                )
            ],
            skip=5,
        )
        with pytest.raises(NotImplementedError, match="SKIP"):
            star._from_with_clause(with_clause, person_relation)

    def test_limit_not_supported(
        self, star: Star, person_relation: Projection
    ) -> None:
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    ),
                    alias="name",
                )
            ],
            limit=10,
        )
        with pytest.raises(NotImplementedError, match="SKIP"):
            star._from_with_clause(with_clause, person_relation)

    def test_missing_expression_raises(
        self, star: Star, person_relation: Projection
    ) -> None:
        with_clause = With(
            items=[ReturnItem(expression=None, alias="x")],
        )
        with pytest.raises(ValueError, match="must have an expression"):
            star._from_with_clause(with_clause, person_relation)

    def test_missing_alias_raises(
        self, star: Star, person_relation: Projection
    ) -> None:
        with_clause = With(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    ),
                    alias=None,
                )
            ],
        )
        with pytest.raises(ValueError, match="must have aliases"):
            star._from_with_clause(with_clause, person_relation)

    def test_wrong_type_raises(
        self, star: Star, person_relation: Projection
    ) -> None:
        with pytest.raises(TypeError, match="Expected With"):
            star._from_with_clause("not_a_with", person_relation)


# ---------------------------------------------------------------------------
# _from_return_clause validation errors
# ---------------------------------------------------------------------------


class TestReturnClauseValidation:
    """Cover NotImplementedError / ValueError branches in _from_return_clause."""

    def test_order_by_not_supported(
        self, star: Star, person_relation: Projection
    ) -> None:
        ret = Return(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    ),
                    alias="name",
                )
            ],
            order_by=[],
        )
        with pytest.raises(NotImplementedError, match="ORDER BY"):
            star._from_return_clause(ret, person_relation)

    def test_distinct_not_supported(
        self, star: Star, person_relation: Projection
    ) -> None:
        ret = Return(
            distinct=True,
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    ),
                    alias="name",
                )
            ],
        )
        with pytest.raises(NotImplementedError, match="DISTINCT"):
            star._from_return_clause(ret, person_relation)

    def test_skip_limit_not_supported(
        self, star: Star, person_relation: Projection
    ) -> None:
        ret = Return(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    ),
                    alias="name",
                )
            ],
            skip=1,
        )
        with pytest.raises(NotImplementedError, match="SKIP"):
            star._from_return_clause(ret, person_relation)

    def test_missing_expression_raises(
        self, star: Star, person_relation: Projection
    ) -> None:
        ret = Return(items=[ReturnItem(expression=None, alias="x")])
        with pytest.raises(ValueError, match="must have an expression"):
            star._from_return_clause(ret, person_relation)

    def test_missing_alias_raises(
        self, star: Star, person_relation: Projection
    ) -> None:
        ret = Return(
            items=[
                ReturnItem(
                    expression=PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    ),
                    alias=None,
                )
            ],
        )
        with pytest.raises(ValueError, match="must have aliases"):
            star._from_return_clause(ret, person_relation)

    def test_wrong_type_raises(
        self, star: Star, person_relation: Projection
    ) -> None:
        with pytest.raises(TypeError, match="Expected Return"):
            star._from_return_clause("not_a_return", person_relation)


# ---------------------------------------------------------------------------
# to_pandas delegation
# ---------------------------------------------------------------------------


class TestToPandas:
    """Cover Star.to_pandas delegation."""

    def test_delegates_to_relation(
        self, star: Star, person_context: Context, person_relation: Projection
    ) -> None:
        """Star.to_pandas() delegates to relation.to_pandas(context=...)."""
        result = star.to_pandas(person_relation)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# _contains_aggregation — extra paths
# ---------------------------------------------------------------------------


class TestContainsAggregation:
    """Cover edge cases in _contains_aggregation."""

    def test_count_star(self, star: Star) -> None:
        """CountStar is always aggregation."""
        assert star._contains_aggregation(CountStar()) is True

    def test_namespaced_scalar_function(self, star: Star) -> None:
        """Dict-style function name for scalar → False."""
        expr = FunctionInvocation(
            name={"namespace": "cypher", "name": "toUpper"},
            arguments={"arguments": [StringLiteral(value="hi")]},
        )
        assert star._contains_aggregation(expr) is False

    def test_namespaced_agg_function(self, star: Star) -> None:
        """Dict-style function name for aggregation → True."""
        expr = FunctionInvocation(
            name={"namespace": "cypher", "name": "count"},
            arguments={"arguments": [IntegerLiteral(value=1)]},
        )
        assert star._contains_aggregation(expr) is True

    def test_unknown_function_defaults_scalar(self, star: Star) -> None:
        """Unknown function name defaults to scalar (False)."""
        expr = FunctionInvocation(
            name="totally_custom_func",
            arguments={"arguments": [IntegerLiteral(value=1)]},
        )
        assert star._contains_aggregation(expr) is False

    def test_non_function_expression(self, star: Star) -> None:
        """Non-FunctionInvocation expression returns False."""
        assert star._contains_aggregation(IntegerLiteral(value=42)) is False


# ---------------------------------------------------------------------------
# execute_query error paths
# ---------------------------------------------------------------------------


class TestExecuteQueryErrors:
    """Cover error paths in execute_query."""

    def test_invalid_query_type(self, star: Star) -> None:
        """Passing a non-Query/non-str raises TypeError."""
        with pytest.raises(TypeError, match="Expected Query"):
            star.execute_query(42)

    def test_with_without_match(self, star: Star) -> None:
        """WITH without preceding MATCH raises ValueError."""
        from pycypher.ast_models import Query

        query = Query(
            clauses=[
                With(
                    items=[
                        ReturnItem(
                            expression=IntegerLiteral(value=1), alias="x"
                        )
                    ]
                ),
            ]
        )
        with pytest.raises(ValueError, match="requires preceding MATCH"):
            star.execute_query(query)

    def test_return_without_match(self, star: Star) -> None:
        """RETURN without preceding MATCH raises ValueError."""
        from pycypher.ast_models import Query

        query = Query(
            clauses=[
                Return(
                    items=[
                        ReturnItem(
                            expression=IntegerLiteral(value=1), alias="x"
                        )
                    ]
                ),
            ]
        )
        with pytest.raises(ValueError, match="requires preceding MATCH"):
            star.execute_query(query)

    def test_empty_query(self, star: Star) -> None:
        """Query with no clauses raises ValueError."""
        from pycypher.ast_models import Query

        with pytest.raises(ValueError, match="at least one clause"):
            star.execute_query(Query(clauses=[]))

    def test_unsupported_clause_type(self, star: Star) -> None:
        """Unknown clause type raises NotImplementedError."""
        from pycypher.ast_models import Create, Query

        # Create is not supported by execute_query
        query = Query(clauses=[Create()])
        with pytest.raises(NotImplementedError, match="not supported"):
            star.execute_query(query)
