"""Tests for REDUCE expression evaluation.

Cypher REDUCE syntax:
    reduce(accumulator = initial, variable IN list | step_expression)

Examples:
    reduce(s = 0, x IN [1,2,3] | s + x)       -> 6
    reduce(s = '', x IN ['a','b'] | s + x)    -> 'ab'
    reduce(s = 0, x IN n.scores | s + x)      -> per-row sum

"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def star_with_lists() -> Star:
    df = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "scores": [[85, 90, 78], [92, 88], [95]],
            "tags": [["python", "sql"], ["java", "sql"], ["python"]],
        },
    )
    return Star(context=ContextBuilder.from_dict({"Person": df}))


@pytest.fixture
def simple_star() -> Star:
    df = pd.DataFrame({"__ID__": [1], "name": ["X"]})
    return Star(context=ContextBuilder.from_dict({"N": df}))


# ===========================================================================
# Literal list reduce
# ===========================================================================


class TestReduceLiteralList:
    """reduce() applied to literal lists."""

    def test_sum_integers(self, simple_star: Star) -> None:
        result = simple_star.execute_query(
            "MATCH (n:N) WITH reduce(s = 0, x IN [1, 2, 3, 4, 5] | s + x) AS total "
            "RETURN total AS total",
        )
        assert int(result["total"].iloc[0]) == 15

    def test_product_integers(self, simple_star: Star) -> None:
        result = simple_star.execute_query(
            "MATCH (n:N) WITH reduce(p = 1, x IN [1, 2, 3, 4] | p * x) AS product "
            "RETURN product AS product",
        )
        assert int(result["product"].iloc[0]) == 24

    def test_sum_empty_list_returns_initial(self, simple_star: Star) -> None:
        result = simple_star.execute_query(
            "MATCH (n:N) WITH reduce(s = 0, x IN [] | s + x) AS total "
            "RETURN total AS total",
        )
        assert result["total"].iloc[0] == 0

    def test_single_element_list(self, simple_star: Star) -> None:
        result = simple_star.execute_query(
            "MATCH (n:N) WITH reduce(s = 0, x IN [42] | s + x) AS total "
            "RETURN total AS total",
        )
        assert int(result["total"].iloc[0]) == 42

    def test_max_via_case(self, simple_star: Star) -> None:
        """Reduce can compute max using a CASE expression."""
        result = simple_star.execute_query(
            "MATCH (n:N) "
            "WITH reduce(m = 0, x IN [3, 7, 2, 9, 1] | CASE WHEN x > m THEN x ELSE m END) AS mx "
            "RETURN mx AS mx",
        )
        assert int(result["mx"].iloc[0]) == 9


# ===========================================================================
# Property list reduce (per-row)
# ===========================================================================


class TestReducePropertyList:
    """reduce() over per-row list properties."""

    def test_sum_score_list_per_row(self, star_with_lists: Star) -> None:
        result = star_with_lists.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS name, reduce(s = 0, x IN p.scores | s + x) AS total "
            "RETURN name, total "
            "ORDER BY name ASC",
        )
        totals = dict(zip(result["name"], result["total"].astype(int)))
        assert totals["Alice"] == 85 + 90 + 78  # 253
        assert totals["Bob"] == 92 + 88  # 180
        assert totals["Carol"] == 95

    def test_reduce_count_elements(self, star_with_lists: Star) -> None:
        """Reduce can count non-zero elements."""
        result = star_with_lists.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS name, reduce(c = 0, x IN p.scores | c + 1) AS n "
            "RETURN name, n "
            "ORDER BY name ASC",
        )
        counts = dict(zip(result["name"], result["n"].astype(int)))
        assert counts["Alice"] == 3
        assert counts["Bob"] == 2
        assert counts["Carol"] == 1

    def test_reduce_in_return_clause(self, star_with_lists: Star) -> None:
        """reduce() works in RETURN directly (not just WITH)."""
        result = star_with_lists.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, "
            "reduce(s = 0, x IN p.scores | s + x) AS total "
            "ORDER BY name ASC",
        )
        assert "total" in result.columns
        assert len(result) == 3
        assert int(result["total"].iloc[0]) == 253  # Alice: 85+90+78

    def test_reduce_with_where_filter(self, star_with_lists: Star) -> None:
        """reduce() on filtered rows."""
        result = star_with_lists.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "WITH reduce(s = 0, x IN p.scores | s + x) AS total "
            "RETURN total AS total",
        )
        assert int(result["total"].iloc[0]) == 180  # Bob: 92+88
