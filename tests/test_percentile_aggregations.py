"""Tests for percentileCont() and percentileDisc() aggregation functions.

``percentileCont(expr, percentile)`` computes the continuous percentile via
linear interpolation, returning a float.

``percentileDisc(expr, percentile)`` computes the discrete percentile by
selecting the nearest value at or below the target quantile (equivalent to
pandas ``interpolation='lower'``), returning an actual value from the dataset.

The ``percentile`` argument must be a literal float in ``[0.0, 1.0]``.

Both functions ignore nulls and return null when all input values are null.
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
    """Five persons with scores [10, 20, 30, 40, 50] — clean, no nulls."""
    persons = pd.DataFrame(
        {
            "__ID__": ["p1", "p2", "p3", "p4", "p5"],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "score": [10.0, 20.0, 30.0, 40.0, 50.0],
            # Two nulls in salary so null-ignoring behaviour is tested.
            "salary": [100.0, None, 200.0, None, 300.0],
        }
    )
    return Star(context=ContextBuilder.from_dict({"Person": persons}))


# ---------------------------------------------------------------------------
# percentileCont — linear interpolation
# ---------------------------------------------------------------------------


class TestPercentileCont:
    """percentileCont(expr, p) uses linear interpolation."""

    def test_percentilecont_median(self, star: Star) -> None:
        """percentileCont at 0.5 returns the median (linear interpolation)."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH percentileCont(p.score, 0.5) AS med "
            "RETURN med AS med"
        )
        # Scores: [10, 20, 30, 40, 50] → linear p50 == 30.0
        assert result["med"].iloc[0] == 30.0

    def test_percentilecont_lower_quartile(self, star: Star) -> None:
        """percentileCont at 0.25 returns Q1 via linear interpolation."""
        result = star.execute_query(
            "MATCH (p:Person) WITH percentileCont(p.score, 0.25) AS q1 RETURN q1 AS q1"
        )
        # pandas quantile(0.25, interpolation='linear') on [10..50] == 20.0
        assert result["q1"].iloc[0] == 20.0

    def test_percentilecont_upper_quartile(self, star: Star) -> None:
        """percentileCont at 0.75 returns Q3."""
        result = star.execute_query(
            "MATCH (p:Person) WITH percentileCont(p.score, 0.75) AS q3 RETURN q3 AS q3"
        )
        assert result["q3"].iloc[0] == 40.0

    def test_percentilecont_min(self, star: Star) -> None:
        """percentileCont at 0.0 returns the minimum."""
        result = star.execute_query(
            "MATCH (p:Person) WITH percentileCont(p.score, 0.0) AS v RETURN v AS v"
        )
        assert result["v"].iloc[0] == 10.0

    def test_percentilecont_max(self, star: Star) -> None:
        """percentileCont at 1.0 returns the maximum."""
        result = star.execute_query(
            "MATCH (p:Person) WITH percentileCont(p.score, 1.0) AS v RETURN v AS v"
        )
        assert result["v"].iloc[0] == 50.0

    def test_percentilecont_ignores_nulls(self, star: Star) -> None:
        """percentileCont ignores null values in the aggregated expression."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH percentileCont(p.salary, 0.5) AS med "
            "RETURN med AS med"
        )
        # Non-null salaries: [100, 200, 300] → p50 = 200.0
        assert result["med"].iloc[0] == 200.0

    def test_percentilecont_all_nulls_returns_null(self) -> None:
        """percentileCont over all-null input returns null."""
        df = pd.DataFrame({"__ID__": ["a", "b"], "val": [None, None]})
        s = Star(context=ContextBuilder.from_dict({"T": df}))
        result = s.execute_query(
            "MATCH (t:T) WITH percentileCont(t.val, 0.5) AS med RETURN med AS med"
        )
        assert pd.isna(result["med"].iloc[0])

    def test_percentilecont_in_return_clause(self, star: Star) -> None:
        """percentileCont works in RETURN as well as WITH."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN percentileCont(p.score, 0.5) AS med"
        )
        assert result["med"].iloc[0] == 30.0

    def test_percentilecont_grouped(self, star: Star) -> None:
        """percentileCont produces per-group results correctly."""
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
            "WITH p.dept AS dept, percentileCont(p.score, 0.5) AS med "
            "RETURN dept AS dept, med AS med"
        )
        dept_med = {row["dept"]: row["med"] for _, row in result.iterrows()}
        assert dept_med["A"] == 20.0  # [10, 30] → 20.0
        assert dept_med["B"] == 30.0  # [20, 40] → 30.0


# ---------------------------------------------------------------------------
# percentileDisc — discrete (lower) interpolation
# ---------------------------------------------------------------------------


class TestPercentileDisc:
    """percentileDisc(expr, p) returns an actual dataset value (lower interpolation)."""

    def test_percentiledisc_median(self, star: Star) -> None:
        """percentileDisc at 0.5 returns the exact median from the dataset."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH percentileDisc(p.score, 0.5) AS med "
            "RETURN med AS med"
        )
        # Scores: [10, 20, 30, 40, 50] → discrete p50 = 30.0
        assert result["med"].iloc[0] == 30.0

    def test_percentiledisc_lower_interpolation(self, star: Star) -> None:
        """percentileDisc uses lower interpolation — picks nearest value ≤ quantile."""
        result = star.execute_query(
            "MATCH (p:Person) WITH percentileDisc(p.score, 0.4) AS v RETURN v AS v"
        )
        # pandas quantile(0.4, interpolation='lower') on [10..50] == 20.0
        assert result["v"].iloc[0] == 20.0

    def test_percentiledisc_min(self, star: Star) -> None:
        """percentileDisc at 0.0 returns the minimum value."""
        result = star.execute_query(
            "MATCH (p:Person) WITH percentileDisc(p.score, 0.0) AS v RETURN v AS v"
        )
        assert result["v"].iloc[0] == 10.0

    def test_percentiledisc_max(self, star: Star) -> None:
        """percentileDisc at 1.0 returns the maximum value."""
        result = star.execute_query(
            "MATCH (p:Person) WITH percentileDisc(p.score, 1.0) AS v RETURN v AS v"
        )
        assert result["v"].iloc[0] == 50.0

    def test_percentiledisc_ignores_nulls(self, star: Star) -> None:
        """percentileDisc ignores nulls."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH percentileDisc(p.salary, 0.5) AS med "
            "RETURN med AS med"
        )
        # Non-null salaries: [100, 200, 300] → discrete p50 = 200.0
        assert result["med"].iloc[0] == 200.0

    def test_percentiledisc_all_nulls_returns_null(self) -> None:
        """percentileDisc over all-null input returns null."""
        df = pd.DataFrame({"__ID__": ["a", "b"], "val": [None, None]})
        s = Star(context=ContextBuilder.from_dict({"T": df}))
        result = s.execute_query(
            "MATCH (t:T) WITH percentileDisc(t.val, 0.5) AS med RETURN med AS med"
        )
        assert pd.isna(result["med"].iloc[0])

    def test_percentiledisc_returns_actual_dataset_value(
        self, star: Star
    ) -> None:
        """percentileDisc always returns a value from the input dataset."""
        result = star.execute_query(
            "MATCH (p:Person) WITH percentileDisc(p.score, 0.3) AS v RETURN v AS v"
        )
        # The returned value must appear in the input dataset
        assert result["v"].iloc[0] in {10.0, 20.0, 30.0, 40.0, 50.0}

    def test_percentilecont_and_percentiledisc_agree_at_endpoints(
        self, star: Star
    ) -> None:
        """Both functions agree at 0.0 (min) and 1.0 (max)."""
        r_cont_min = star.execute_query(
            "MATCH (p:Person) WITH percentileCont(p.score, 0.0) AS v RETURN v AS v"
        )
        r_disc_min = star.execute_query(
            "MATCH (p:Person) WITH percentileDisc(p.score, 0.0) AS v RETURN v AS v"
        )
        r_cont_max = star.execute_query(
            "MATCH (p:Person) WITH percentileCont(p.score, 1.0) AS v RETURN v AS v"
        )
        r_disc_max = star.execute_query(
            "MATCH (p:Person) WITH percentileDisc(p.score, 1.0) AS v RETURN v AS v"
        )
        assert r_cont_min["v"].iloc[0] == r_disc_min["v"].iloc[0] == 10.0
        assert r_cont_max["v"].iloc[0] == r_disc_max["v"].iloc[0] == 50.0
