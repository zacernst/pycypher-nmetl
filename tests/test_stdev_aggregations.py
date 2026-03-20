"""Tests for stdev() and stdevp() aggregation functions.

``stdev(expr)``  computes the sample standard deviation (ddof=1, Bessel's
correction) of a numeric expression, ignoring nulls.

``stdevp(expr)`` computes the population standard deviation (ddof=0) of a
numeric expression, ignoring nulls.

Both follow the null-handling conventions already established for ``avg``:
null-only or empty input returns null, not 0 or NaN.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def star() -> Star:
    """Five persons with known scores for deterministic std-dev calculations."""
    persons = pd.DataFrame(
        {
            "__ID__": ["p1", "p2", "p3", "p4", "p5"],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "score": [10.0, 20.0, 30.0, 40.0, 50.0],
            # Two nulls so we can test null-ignoring behaviour.
            "rating": [4.0, None, 3.0, None, 5.0],
        }
    )
    return Star(context=ContextBuilder.from_dict({"Person": persons}))


# ---------------------------------------------------------------------------
# stdev() — sample standard deviation
# ---------------------------------------------------------------------------


class TestStdev:
    """stdev(expr) returns the sample standard deviation (ddof=1)."""

    def test_stdev_basic_accuracy(self, star: Star) -> None:
        """stdev over five values matches statistics.stdev exactly."""
        result = star.execute_query(
            "MATCH (p:Person) WITH stdev(p.score) AS sd RETURN sd AS sd"
        )
        # statistics.stdev([10, 20, 30, 40, 50]) == 15.811388300841896
        assert abs(result["sd"].iloc[0] - 15.811388300841896) < 1e-9

    def test_stdev_ignores_nulls(self, star: Star) -> None:
        """stdev ignores null values, computing only over non-null entries."""
        result = star.execute_query(
            "MATCH (p:Person) WITH stdev(p.rating) AS sd RETURN sd AS sd"
        )
        # Non-null ratings: [4.0, 3.0, 5.0] → stdev (ddof=1)
        # statistics.stdev([4, 3, 5]) == 1.0
        assert abs(result["sd"].iloc[0] - 1.0) < 1e-9

    def test_stdev_single_value_returns_null(self, star: Star) -> None:
        """stdev of a single value is undefined (ddof=1 divides by 0) → null."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "WITH stdev(p.score) AS sd RETURN sd AS sd"
        )
        # pandas std(ddof=1) returns NaN for single-element series
        assert pd.isna(result["sd"].iloc[0])

    def test_stdev_all_same_values(self, star: Star) -> None:
        """stdev of a constant series is 0.0."""
        persons_same = pd.DataFrame(
            {"__ID__": ["a", "b", "c"], "score": [7.0, 7.0, 7.0]}
        )
        s = Star(context=ContextBuilder.from_dict({"Person": persons_same}))
        result = s.execute_query(
            "MATCH (p:Person) WITH stdev(p.score) AS sd RETURN sd AS sd"
        )
        assert result["sd"].iloc[0] == 0.0

    def test_stdev_in_return_clause(self, star: Star) -> None:
        """stdev works in RETURN as well as WITH."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN stdev(p.score) AS sd"
        )
        assert abs(result["sd"].iloc[0] - 15.811388300841896) < 1e-9

    def test_stdev_all_nulls_returns_null(self) -> None:
        """stdev over an all-null column returns null."""
        df = pd.DataFrame({"__ID__": ["x", "y"], "val": [None, None]})
        s = Star(context=ContextBuilder.from_dict({"T": df}))
        result = s.execute_query(
            "MATCH (t:T) WITH stdev(t.val) AS sd RETURN sd AS sd"
        )
        assert pd.isna(result["sd"].iloc[0])

    def test_stdev_grouped_by_category(self, star: Star) -> None:
        """stdev works correctly in grouped (non-global) aggregations."""
        persons = pd.DataFrame(
            {
                "__ID__": ["p1", "p2", "p3", "p4"],
                "dept": ["A", "A", "B", "B"],
                "score": [10.0, 30.0, 20.0, 40.0],
            }
        )
        s = Star(context=ContextBuilder.from_dict({"Person": persons}))
        result = s.execute_query(
            "MATCH (p:Person) "
            "WITH p.dept AS dept, stdev(p.score) AS sd "
            "RETURN dept AS dept, sd AS sd"
        )
        dept_sd = {row["dept"]: row["sd"] for _, row in result.iterrows()}
        # stdev([10, 30]) = 14.142135..., stdev([20, 40]) = 14.142135...
        assert abs(dept_sd["A"] - 14.142135623730951) < 1e-9
        assert abs(dept_sd["B"] - 14.142135623730951) < 1e-9


# ---------------------------------------------------------------------------
# stdevp() — population standard deviation
# ---------------------------------------------------------------------------


class TestStdevp:
    """stdevp(expr) returns the population standard deviation (ddof=0)."""

    def test_stdevp_basic_accuracy(self, star: Star) -> None:
        """stdevp over five values matches statistics.pstdev exactly."""
        result = star.execute_query(
            "MATCH (p:Person) WITH stdevp(p.score) AS sd RETURN sd AS sd"
        )
        # statistics.pstdev([10, 20, 30, 40, 50]) == 14.142135623730951
        assert abs(result["sd"].iloc[0] - 14.142135623730951) < 1e-9

    def test_stdevp_ignores_nulls(self, star: Star) -> None:
        """stdevp ignores null values."""
        result = star.execute_query(
            "MATCH (p:Person) WITH stdevp(p.rating) AS sd RETURN sd AS sd"
        )
        # Non-null ratings: [4.0, 3.0, 5.0] → pstdev
        # statistics.pstdev([4, 3, 5]) == 0.816496580927726
        assert abs(result["sd"].iloc[0] - 0.816496580927726) < 1e-9

    def test_stdevp_single_value_is_zero(self, star: Star) -> None:
        """stdevp of a single value is 0 (deviation from itself is 0)."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "WITH stdevp(p.score) AS sd RETURN sd AS sd"
        )
        # pandas std(ddof=0) of single element == 0.0
        assert result["sd"].iloc[0] == 0.0

    def test_stdevp_smaller_than_stdev(self, star: Star) -> None:
        """Population stdev is always ≤ sample stdev for n > 1."""
        r_samp = star.execute_query(
            "MATCH (p:Person) WITH stdev(p.score) AS sd RETURN sd AS sd"
        )
        r_pop = star.execute_query(
            "MATCH (p:Person) WITH stdevp(p.score) AS sd RETURN sd AS sd"
        )
        assert r_pop["sd"].iloc[0] < r_samp["sd"].iloc[0]

    def test_stdevp_all_nulls_returns_null(self) -> None:
        """stdevp over an all-null column returns null."""
        df = pd.DataFrame({"__ID__": ["x", "y"], "val": [None, None]})
        s = Star(context=ContextBuilder.from_dict({"T": df}))
        result = s.execute_query(
            "MATCH (t:T) WITH stdevp(t.val) AS sd RETURN sd AS sd"
        )
        assert pd.isna(result["sd"].iloc[0])

    def test_stdevp_vs_manual_formula(self, star: Star) -> None:
        """Cross-check stdevp against manual formula sqrt(variance)."""
        import math

        result = star.execute_query(
            "MATCH (p:Person) WITH stdevp(p.score) AS sd RETURN sd AS sd"
        )
        scores = [10.0, 20.0, 30.0, 40.0, 50.0]
        mean = sum(scores) / len(scores)
        variance = sum((x - mean) ** 2 for x in scores) / len(scores)
        expected = math.sqrt(variance)
        assert abs(result["sd"].iloc[0] - expected) < 1e-9
