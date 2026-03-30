"""TDD tests for ORDER BY ... NULLS FIRST / NULLS LAST.

Neo4j 5.x (and openCypher) support null-placement control in ORDER BY:
  ORDER BY n.val ASC NULLS FIRST   — nulls appear before non-nulls
  ORDER BY n.val DESC NULLS LAST   — nulls appear after non-nulls
  ORDER BY n.val NULLS FIRST       — direction defaults to ASC, nulls first
  ORDER BY n.val                   — default: ascending, nulls last

Neo4j default null placement: NULLS LAST for both ASC and DESC.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import ASTConverter, OrderByItem
from pycypher.grammar_parser import GrammarParser
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.star import Star


@pytest.fixture
def parser() -> GrammarParser:
    return GrammarParser()


@pytest.fixture
def ctx():
    """Context with nullable score column for sorting tests."""
    return ContextBuilder().from_dict(
        {
            "Person": pd.DataFrame(
                {
                    "__ID__": ["p1", "p2", "p3", "p4", "p5"],
                    "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
                    "score": [10.0, None, 30.0, None, 20.0],
                },
            ),
        },
    )


@pytest.fixture
def multi_null_ctx():
    """Context where multiple rows have null values."""
    return ContextBuilder().from_dict(
        {
            "Item": pd.DataFrame(
                {
                    "__ID__": ["i1", "i2", "i3", "i4", "i5", "i6"],
                    "val": [3.0, None, 1.0, None, 2.0, None],
                    "tag": ["c", "x", "a", "y", "b", "z"],
                },
            ),
        },
    )


# ---------------------------------------------------------------------------
# Parsing: grammar must accept NULLS FIRST / NULLS LAST syntax
# ---------------------------------------------------------------------------


class TestOrderByNullsParsing:
    def test_nulls_last_asc_parses(self, parser: GrammarParser) -> None:
        tree = parser.parse(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score ASC NULLS LAST",
        )
        assert tree is not None

    def test_nulls_first_asc_parses(self, parser: GrammarParser) -> None:
        tree = parser.parse(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score ASC NULLS FIRST",
        )
        assert tree is not None

    def test_nulls_first_desc_parses(self, parser: GrammarParser) -> None:
        tree = parser.parse(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score DESC NULLS FIRST",
        )
        assert tree is not None

    def test_nulls_last_desc_parses(self, parser: GrammarParser) -> None:
        tree = parser.parse(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score DESC NULLS LAST",
        )
        assert tree is not None

    def test_nulls_first_without_direction_parses(
        self,
        parser: GrammarParser,
    ) -> None:
        """NULLS FIRST without explicit ASC/DESC should parse (defaults to ASC)."""
        tree = parser.parse(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score NULLS FIRST",
        )
        assert tree is not None

    def test_nulls_last_without_direction_parses(
        self,
        parser: GrammarParser,
    ) -> None:
        tree = parser.parse(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score NULLS LAST",
        )
        assert tree is not None

    def test_multiple_order_items_with_nulls_parses(
        self,
        parser: GrammarParser,
    ) -> None:
        tree = parser.parse(
            "MATCH (n:Person) RETURN n.score, n.name "
            "ORDER BY n.score ASC NULLS FIRST, n.name DESC NULLS LAST",
        )
        assert tree is not None

    def test_existing_order_by_still_parses(
        self,
        parser: GrammarParser,
    ) -> None:
        """Plain ORDER BY without NULLS must still work."""
        tree = parser.parse(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score DESC",
        )
        assert tree is not None


# ---------------------------------------------------------------------------
# AST: nulls_placement field on OrderByItem
# ---------------------------------------------------------------------------


class TestOrderByItemAST:
    def test_nulls_first_in_ast(self, parser: GrammarParser) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score ASC NULLS FIRST",
        )
        return_clause = ast.clauses[-1]
        order_item: OrderByItem = return_clause.order_by[0]
        assert order_item.nulls_placement == "first"
        assert order_item.ascending is True

    def test_nulls_last_in_ast(self, parser: GrammarParser) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score DESC NULLS LAST",
        )
        return_clause = ast.clauses[-1]
        order_item: OrderByItem = return_clause.order_by[0]
        assert order_item.nulls_placement == "last"
        assert order_item.ascending is False

    def test_no_nulls_keyword_gives_none(self, parser: GrammarParser) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score",
        )
        return_clause = ast.clauses[-1]
        order_item: OrderByItem = return_clause.order_by[0]
        assert order_item.nulls_placement is None

    def test_nulls_first_without_direction_ascending_true(
        self,
        parser: GrammarParser,
    ) -> None:
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score NULLS FIRST",
        )
        return_clause = ast.clauses[-1]
        order_item: OrderByItem = return_clause.order_by[0]
        assert order_item.nulls_placement == "first"
        assert order_item.ascending is True


# ---------------------------------------------------------------------------
# Execution: correct null placement in query results
# ---------------------------------------------------------------------------


class TestOrderByNullsExecution:
    def test_asc_nulls_last_default(self, ctx: ContextBuilder) -> None:
        """Default behavior: ascending with nulls at end."""
        s = Star(context=ctx)
        result = s.execute_query(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score",
        )
        scores = result["score"].tolist()
        non_null = [v for v in scores if v is not None and v == v]
        null_count = sum(1 for v in scores if v is None or v != v)
        # Non-nulls appear first, sorted ascending
        assert non_null == [10.0, 20.0, 30.0]
        # Nulls at the end
        assert scores[-null_count:] == [None] * null_count or all(
            v != v for v in scores[-null_count:]
        )

    def test_asc_nulls_first(self, ctx: ContextBuilder) -> None:
        """NULLS FIRST puts null rows before non-null rows."""
        s = Star(context=ctx)
        result = s.execute_query(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score ASC NULLS FIRST",
        )
        scores = result["score"].tolist()
        # First two should be null (Dave and Bob have null scores)
        null_count = sum(1 for v in scores if v is None or v != v)
        assert null_count == 2
        first_nulls = scores[:null_count]
        assert all(v is None or v != v for v in first_nulls)
        # Remainder should be ascending non-nulls
        non_null_tail = [v for v in scores[null_count:] if v is not None]
        assert non_null_tail == sorted(non_null_tail)

    def test_desc_nulls_last(self, ctx: ContextBuilder) -> None:
        """DESC NULLS LAST: non-nulls descending, nulls at end."""
        s = Star(context=ctx)
        result = s.execute_query(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score DESC NULLS LAST",
        )
        scores = result["score"].tolist()
        non_null = [v for v in scores if v is not None and v == v]
        assert non_null == [30.0, 20.0, 10.0]  # descending
        # Nulls at the end
        null_count = sum(1 for v in scores if v is None or v != v)
        assert null_count == 2
        assert all(v is None or v != v for v in scores[-null_count:])

    def test_desc_nulls_first(self, ctx: ContextBuilder) -> None:
        """DESC NULLS FIRST: nulls appear before descending non-nulls."""
        s = Star(context=ctx)
        result = s.execute_query(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score DESC NULLS FIRST",
        )
        scores = result["score"].tolist()
        null_count = 2
        # First rows should be null
        assert all(v is None or v != v for v in scores[:null_count])
        # Remaining rows should be descending non-nulls
        non_null = [v for v in scores[null_count:] if v is not None]
        assert non_null == [30.0, 20.0, 10.0]

    def test_nulls_first_no_nulls_in_data(self) -> None:
        """NULLS FIRST on a column with no nulls: just ascending order."""
        ctx = ContextBuilder().from_dict(
            {
                "Num": pd.DataFrame(
                    {
                        "__ID__": ["a", "b", "c"],
                        "val": [3.0, 1.0, 2.0],
                    },
                ),
            },
        )
        s = Star(context=ctx)
        result = s.execute_query(
            "MATCH (n:Num) RETURN n.val ORDER BY n.val ASC NULLS FIRST",
        )
        assert list(result["val"]) == [1.0, 2.0, 3.0]

    def test_nulls_last_explicit_same_as_default(
        self,
        ctx: ContextBuilder,
    ) -> None:
        """Explicit NULLS LAST must match the default (no keyword) behavior."""
        s = Star(context=ctx)
        result_default = s.execute_query(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score ASC",
        )
        result_explicit = s.execute_query(
            "MATCH (n:Person) RETURN n.score ORDER BY n.score ASC NULLS LAST",
        )
        assert list(result_default["score"]) == list(result_explicit["score"])

    def test_multi_column_mixed_null_placement(
        self,
        multi_null_ctx: ContextBuilder,
    ) -> None:
        """Two sort keys with different null placements."""
        s = Star(context=multi_null_ctx)
        result = s.execute_query(
            "MATCH (n:Item) RETURN n.val, n.tag "
            "ORDER BY n.val ASC NULLS FIRST, n.tag ASC NULLS LAST",
        )
        vals = result["val"].tolist()
        # Nulls first for val: the 3 null-val rows come first
        null_count = sum(1 for v in vals if v is None or v != v)
        assert null_count == 3
        assert all(v is None or v != v for v in vals[:null_count])

    def test_existing_functions_unaffected(self) -> None:
        """last() and head() functions still work after grammar change."""
        ctx = ContextBuilder().from_dict(
            {
                "Lst": pd.DataFrame(
                    {
                        "__ID__": ["l1"],
                        "nums": [[1, 2, 3]],
                    },
                ),
            },
        )
        s = Star(context=ctx)
        result = s.execute_query("MATCH (n:Lst) RETURN last(n.nums) AS v")
        assert result["v"].iloc[0] == 3
