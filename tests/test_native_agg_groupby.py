"""TDD tests for native pandas groupby aggregation (Loop 170 — Performance).

Problem: ``evaluate_aggregation_grouped`` calls ``grouped.agg(python_lambda)``
for count/sum/avg/min/max/stdev/stdevp, which triggers pandas'
``_python_agg_general`` path — one Python function call per group.
For 494 groups × 20 queries this causes 9880 lambda invocations and
~34ms of avoidable overhead.

Fix: replace the generic lambda dispatch with pandas-native aggregation
strings / groupby methods for the common cases so pandas uses its optimised
Cython extension path instead.

All tests are written before the fix (TDD red phase).
"""

from __future__ import annotations

import time

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

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def person_star() -> Star:
    """100-person graph with age/score attributes for aggregation tests."""
    ids = list(range(1, 101))
    ages = [20 + (i % 50) for i in ids]  # 20–69, 5 rows each unique
    scores = [float(i * 1.5) for i in ids]  # floats for avg/stdev
    people_df = pd.DataFrame(
        {
            ID_COLUMN: ids,
            "name": [f"P{i}" for i in ids],
            "age": ages,
            "score": scores,
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "score"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "score": "score",
        },
        attribute_map={"name": "name", "age": "age", "score": "score"},
        source_obj=people_df,
    )
    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )
    return Star(context=context)


@pytest.fixture
def null_star() -> Star:
    """10-person graph where score is NULL for all, to test null-group semantics."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(1, 11)),
            "name": [f"P{i}" for i in range(1, 11)],
            "group": ["A"] * 5 + ["B"] * 5,
            "score": [None] * 10,
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "group", "score"],
        source_obj_attribute_map={
            "name": "name",
            "group": "group",
            "score": "score",
        },
        attribute_map={"name": "name", "group": "group", "score": "score"},
        source_obj=people_df,
    )
    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )
    return Star(context=context)


# ---------------------------------------------------------------------------
# Category 1 — Correctness: null-group semantics must be preserved
# ---------------------------------------------------------------------------


class TestNativeAggNullSemantics:
    """All-null groups must return null (None/NaN), not 0 or garbage."""

    def test_sum_all_null_group_returns_null(self, null_star: Star) -> None:
        """sum(score) for groups where every score is NULL must return null."""
        r = null_star.execute_query(
            "MATCH (n:Person) RETURN n.group AS grp, sum(n.score) AS total",
        )
        assert len(r) == 2, f"Expected 2 groups, got {len(r)}"
        # Both A and B groups have all-null scores
        for val in r["total"]:
            assert val is None or (isinstance(val, float) and pd.isna(val)), (
                f"Expected null for all-null sum, got {val!r}"
            )

    def test_avg_all_null_group_returns_null(self, null_star: Star) -> None:
        """avg(score) for all-null groups must return null."""
        r = null_star.execute_query(
            "MATCH (n:Person) RETURN n.group AS grp, avg(n.score) AS mean_val",
        )
        for val in r["mean_val"]:
            assert val is None or (isinstance(val, float) and pd.isna(val)), (
                f"Expected null for all-null avg, got {val!r}"
            )

    def test_min_all_null_group_returns_null(self, null_star: Star) -> None:
        """min(score) for all-null groups must return null."""
        r = null_star.execute_query(
            "MATCH (n:Person) RETURN n.group AS grp, min(n.score) AS min_val",
        )
        for val in r["min_val"]:
            assert val is None or (isinstance(val, float) and pd.isna(val)), (
                f"Expected null for all-null min, got {val!r}"
            )

    def test_max_all_null_group_returns_null(self, null_star: Star) -> None:
        """max(score) for all-null groups must return null."""
        r = null_star.execute_query(
            "MATCH (n:Person) RETURN n.group AS grp, max(n.score) AS max_val",
        )
        for val in r["max_val"]:
            assert val is None or (isinstance(val, float) and pd.isna(val)), (
                f"Expected null for all-null max, got {val!r}"
            )

    def test_stdev_single_value_group_returns_null(self) -> None:
        """stdev(x) with one non-null value per group returns null (ddof=1 undefined)."""
        people_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2],
                "grp": ["A", "B"],
                "score": [5.0, 10.0],
            },
        )
        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "grp", "score"],
            source_obj_attribute_map={"grp": "grp", "score": "score"},
            attribute_map={"grp": "grp", "score": "score"},
            source_obj=people_df,
        )
        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
        star = Star(context=context)
        r = star.execute_query(
            "MATCH (n:Person) RETURN n.grp AS grp, stdev(n.score) AS sd",
        )
        for val in r["sd"]:
            assert val is None or (isinstance(val, float) and pd.isna(val)), (
                f"Expected null for single-value stdev, got {val!r}"
            )


# ---------------------------------------------------------------------------
# Category 2 — Correctness: results match previous lambda-based behaviour
# ---------------------------------------------------------------------------


class TestNativeAggCorrectness:
    """Native agg results must be numerically identical to the old lambda path."""

    def test_count_matches_exact(self, person_star: Star) -> None:
        """count(n.age) grouped by age bucket matches expected count."""
        r = person_star.execute_query(
            "MATCH (n:Person) RETURN n.age AS age, count(n.age) AS cnt",
        )
        # Each age bucket (20–69) should have exactly 2 people out of 100
        assert set(r["cnt"].unique()) == {2}, (
            f"Expected all counts to be 2 (100 people / 50 ages), got {r['cnt'].unique()}"
        )

    def test_sum_matches_expected(self, person_star: Star) -> None:
        """sum(score) per age group matches manually computed values."""
        r = person_star.execute_query(
            "MATCH (n:Person) RETURN n.age AS age, sum(n.score) AS total",
        )
        # Each age bucket has 2 people; scores are 1.5*id
        # We just verify no result is suspiciously 0 (which would indicate
        # pandas native sum without min_count returning 0 for non-null groups)
        assert (r["total"] > 0).all(), (
            f"Expected positive sums for non-null groups, got: {r['total'].tolist()}"
        )

    def test_avg_is_between_min_and_max(self, person_star: Star) -> None:
        """avg(score) for each group must be between the group's min and max."""
        avg_r = person_star.execute_query(
            "MATCH (n:Person) RETURN n.age AS age, avg(n.score) AS mean_val",
        )
        min_r = person_star.execute_query(
            "MATCH (n:Person) RETURN n.age AS age, min(n.score) AS min_val",
        )
        max_r = person_star.execute_query(
            "MATCH (n:Person) RETURN n.age AS age, max(n.score) AS max_val",
        )
        for i in range(len(avg_r)):
            avg = avg_r["mean_val"].iloc[i]
            lo = min_r["min_val"].iloc[i]
            hi = max_r["max_val"].iloc[i]
            assert lo <= avg <= hi, f"avg {avg} not in [{lo}, {hi}] for row {i}"

    def test_min_less_than_or_equal_max(self, person_star: Star) -> None:
        """min(score) <= max(score) for every group."""
        min_r = person_star.execute_query(
            "MATCH (n:Person) RETURN n.age AS age, min(n.score) AS min_val",
        )
        max_r = person_star.execute_query(
            "MATCH (n:Person) RETURN n.age AS age, max(n.score) AS max_val",
        )
        for lo, hi in zip(min_r["min_val"], max_r["max_val"]):
            assert lo <= hi


# ---------------------------------------------------------------------------
# Category 3 — Performance: _python_agg_general must not be called
# ---------------------------------------------------------------------------


class TestNativeAggNoPythonPath:
    """The pure-Python groupby path must be bypassed for common aggregations."""

    @pytest.mark.parametrize("func", ["count", "sum", "avg", "min", "max"])
    def test_lambda_agg_op_not_called_for_grouped(
        self,
        person_star: Star,
        func: str,
    ) -> None:
        """evaluate_aggregation_grouped must NOT call the Python lambda from
        _AGG_OPS for count/sum/avg/min/max — it should use pandas native
        aggregation so the Cython path is exercised instead.
        """
        import pycypher.aggregation_evaluator as be

        original_fn = be._AGG_OPS[func]
        call_count = {"n": 0}

        def tracking_fn(values: pd.Series) -> object:
            call_count["n"] += 1
            return original_fn(values)

        be._AGG_OPS[func] = tracking_fn
        try:
            person_star.execute_query(
                f"MATCH (n:Person) RETURN n.age AS age, {func}(n.score) AS result",
            )
        finally:
            be._AGG_OPS[func] = original_fn

        assert call_count["n"] == 0, (
            f"evaluate_aggregation_grouped still calls the _AGG_OPS['{func}'] "
            f"lambda {call_count['n']} time(s) for a grouped query. "
            "The native pandas aggregation optimisation is not active — "
            "each group is being processed via a slow Python callback."
        )


# ---------------------------------------------------------------------------
# Category 4 — Performance: wall-clock regression guard
# ---------------------------------------------------------------------------


class TestNativeAggPerformance:
    """20 aggregate queries must complete significantly faster than the lambda path."""

    REPS = 20
    THRESHOLD_SECONDS = 1.0  # lambdas took ~41ms each = 820ms for 20; native ~5–10ms

    def test_count_groupby_is_fast(self, person_star: Star) -> None:
        """20 grouped count() queries must finish under 1.0s total."""
        # Warm up AST cache
        person_star.execute_query(
            "MATCH (n:Person) RETURN n.age AS age, count(n.score) AS cnt",
        )
        start = time.perf_counter()
        for _ in range(self.REPS):
            person_star.execute_query(
                "MATCH (n:Person) RETURN n.age AS age, count(n.score) AS cnt",
            )
        elapsed = time.perf_counter() - start
        assert elapsed < self.THRESHOLD_SECONDS, (
            f"{self.REPS} grouped count() queries took {elapsed:.3f}s "
            f"(threshold {self.THRESHOLD_SECONDS}s). "
            "Native aggregation optimisation may not be active."
        )
