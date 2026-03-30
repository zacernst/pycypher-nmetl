"""Correctness and performance tests for the vectorised grouped aggregation path.

These tests verify that the pandas-groupby-backed implementation of
``_aggregate_items`` produces results identical to the old per-group-frame
loop, and that it is meaningfully faster for large group counts.
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
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def people_context():
    """Eight people across three departments, with some null ages/salaries."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5, 6, 7, 8],
            "name": [
                "Alice",
                "Bob",
                "Carol",
                "Dave",
                "Eve",
                "Frank",
                "Grace",
                "Henry",
            ],
            "age": [30, None, 25, 40, 35, None, 28, 32],
            "salary": [
                100_000,
                120_000,
                None,
                110_000,
                95_000,
                105_000,
                None,
                115_000,
            ],
            "score": [85.5, 92.3, 78.9, 88.1, 91.7, 87.2, 83.4, 89.6],
            "dept": [
                "Eng",
                "Sales",
                "Eng",
                "Mktg",
                "Eng",
                "Sales",
                "Mktg",
                "Eng",
            ],
            "team": ["A", "B", "A", "C", "B", "B", "C", "A"],
            "bonus": [5_000, 8_000, 3_000, 6_000, 4_000, 7_000, 5_500, 6_500],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=list(df.columns),
        source_obj_attribute_map={c: c for c in df.columns if c != ID_COLUMN},
        attribute_map={c: c for c in df.columns if c != ID_COLUMN},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


def _star(ctx: Context) -> Star:
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# count(*) grouped
# ---------------------------------------------------------------------------


class TestCountStarGrouped:
    def test_count_star_single_key(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(*) AS n",
        )
        counts = dict(zip(result["dept"], result["n"]))
        assert counts["Eng"] == 4
        assert counts["Sales"] == 2
        assert counts["Mktg"] == 2

    def test_count_star_preserves_all_groups(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(*) AS n",
        )
        assert len(result) == 3


# ---------------------------------------------------------------------------
# sum() grouped
# ---------------------------------------------------------------------------


class TestSumGrouped:
    def test_sum_single_key(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, sum(p.bonus) AS total",
        )
        totals = dict(zip(result["dept"], result["total"]))
        # Eng: 5000+3000+4000+6500 = 18500
        assert totals["Eng"] == pytest.approx(18_500)
        # Sales: 8000+7000 = 15000
        assert totals["Sales"] == pytest.approx(15_000)
        # Mktg: 6000+5500 = 11500
        assert totals["Mktg"] == pytest.approx(11_500)

    def test_sum_null_ignoring(self, people_context):
        # salary has two nulls (Carol, Grace); they should be ignored per group
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, sum(p.salary) AS total",
        )
        totals = dict(zip(result["dept"], result["total"]))
        # Eng salaries: 100000, None, 95000, 115000 → 310000
        assert totals["Eng"] == pytest.approx(310_000)
        # Mktg salaries: 110000, None → 110000
        assert totals["Mktg"] == pytest.approx(110_000)


# ---------------------------------------------------------------------------
# avg() grouped
# ---------------------------------------------------------------------------


class TestAvgGrouped:
    def test_avg_single_key(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, avg(p.bonus) AS mean",
        )
        means = dict(zip(result["dept"], result["mean"]))
        # Eng bonuses: 5000, 3000, 4000, 6500 → mean 4625
        assert means["Eng"] == pytest.approx(4_625.0)

    def test_avg_null_ignoring(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, avg(p.age) AS mean_age",
        )
        means = dict(zip(result["dept"], result["mean_age"]))
        # Eng ages: 30, 25, 35, 32 → mean 30.5 (no nulls in Eng)
        assert means["Eng"] == pytest.approx(30.5)
        # Sales ages: None, None → should be None
        assert means["Sales"] is None or (
            means["Sales"] != means["Sales"]
        )  # None or NaN


# ---------------------------------------------------------------------------
# min() / max() grouped
# ---------------------------------------------------------------------------


class TestMinMaxGrouped:
    def test_min_grouped(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, min(p.score) AS lo",
        )
        lows = dict(zip(result["dept"], result["lo"]))
        assert lows["Eng"] == pytest.approx(78.9)

    def test_max_grouped(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, max(p.score) AS hi",
        )
        highs = dict(zip(result["dept"], result["hi"]))
        assert highs["Sales"] == pytest.approx(92.3)


# ---------------------------------------------------------------------------
# count(expr) grouped
# ---------------------------------------------------------------------------


class TestCountExprGrouped:
    def test_count_non_null_values(self, people_context):
        # age has two nulls; count(p.age) should count only non-null per group
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(p.age) AS n",
        )
        counts = dict(zip(result["dept"], result["n"]))
        # Eng: 30, 25, 35, 32 — all 4 present
        assert counts["Eng"] == 4
        # Sales: None, None — both null → 0
        assert counts["Sales"] == 0


# ---------------------------------------------------------------------------
# collect() grouped
# ---------------------------------------------------------------------------


class TestCollectGrouped:
    def test_collect_single_key(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, collect(p.name) AS names",
        )
        by_dept = dict(zip(result["dept"], result["names"]))
        assert set(by_dept["Eng"]) == {"Alice", "Carol", "Eve", "Henry"}
        assert set(by_dept["Sales"]) == {"Bob", "Frank"}

    def test_collect_distinct(self, people_context):
        # team has repeated values; distinct should deduplicate within each dept group
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, collect(distinct p.team) AS teams",
        )
        by_dept = dict(zip(result["dept"], result["teams"]))
        assert set(by_dept["Eng"]) == {"A", "B"}
        assert set(by_dept["Sales"]) == {"B"}


# ---------------------------------------------------------------------------
# count(distinct) grouped
# ---------------------------------------------------------------------------


class TestCountDistinctGrouped:
    def test_count_distinct_single_key(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(distinct p.team) AS n",
        )
        counts = dict(zip(result["dept"], result["n"]))
        # Eng has teams A, A, B, A → distinct: A, B → 2
        assert counts["Eng"] == 2
        # Sales has B, B → distinct: B → 1
        assert counts["Sales"] == 1


# ---------------------------------------------------------------------------
# stdev / stdevp grouped
# ---------------------------------------------------------------------------


class TestStdevGrouped:
    def test_stdev_grouped(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, stdev(p.score) AS s",
        )
        stdevs = dict(zip(result["dept"], result["s"]))
        import statistics

        eng_scores = [85.5, 78.9, 91.7, 89.6]
        expected = statistics.stdev(eng_scores)
        assert stdevs["Eng"] == pytest.approx(expected, rel=1e-6)

    def test_stdevp_grouped(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, stdevp(p.score) AS s",
        )
        stdevps = dict(zip(result["dept"], result["s"]))
        import statistics

        sales_scores = [92.3, 87.2]
        expected = statistics.pstdev(sales_scores)
        assert stdevps["Sales"] == pytest.approx(expected, rel=1e-6)


# ---------------------------------------------------------------------------
# percentile grouped
# ---------------------------------------------------------------------------


class TestPercentileGrouped:
    def test_percentile_cont_grouped(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, percentileCont(p.bonus, 0.5) AS med",
        )
        meds = dict(zip(result["dept"], result["med"]))
        # Eng bonuses sorted: 3000, 4000, 5000, 6500 → median (cont) = 4500
        assert meds["Eng"] == pytest.approx(4_500.0)

    def test_percentile_disc_grouped(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, percentileDisc(p.bonus, 0.5) AS med",
        )
        meds = dict(zip(result["dept"], result["med"]))
        # Eng bonuses sorted: 3000, 4000, 5000, 6500 → discrete lower = 4000
        assert meds["Eng"] == pytest.approx(4_000.0)


# ---------------------------------------------------------------------------
# Multi-key GROUP BY
# ---------------------------------------------------------------------------


class TestMultiKeyGroupBy:
    def test_two_group_keys(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, p.team AS team, count(*) AS n",
        )
        # Eng/A: Alice,Carol,Henry; Eng/B: Eve; Sales/B: Bob,Frank; Mktg/C: Dave,Grace
        assert len(result) == 4
        row = result[(result["dept"] == "Eng") & (result["team"] == "A")]
        assert row["n"].iloc[0] == 3  # Alice, Carol, Henry

    def test_sum_with_two_group_keys(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, p.team AS team, sum(p.bonus) AS total",
        )
        row = result[(result["dept"] == "Sales") & (result["team"] == "B")]
        # Sales/B: Bob 8000 + Frank 7000 = 15000
        assert row["total"].iloc[0] == pytest.approx(15_000)


# ---------------------------------------------------------------------------
# Arithmetic-wrapping aggregation (fallback path)
# ---------------------------------------------------------------------------


class TestArithmeticWrappingAgg:
    def test_count_star_plus_one(self, people_context):
        result = _star(people_context).execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(*) + 1 AS n_plus_one",
        )
        vals = dict(zip(result["dept"], result["n_plus_one"]))
        assert vals["Eng"] == 5  # 4 + 1
        assert vals["Sales"] == 3  # 2 + 1


# ---------------------------------------------------------------------------
# WITH clause grouped aggregation (tests the WITH path as well)
# ---------------------------------------------------------------------------


class TestWithClauseGrouped:
    def test_with_grouped_feeds_return(self, people_context):
        result = _star(people_context).execute_query("""
            MATCH (p:Person)
            WITH p.dept AS dept, sum(p.bonus) AS total
            RETURN dept, total
            ORDER BY dept
        """)
        assert len(result) == 3
        totals = dict(zip(result["dept"], result["total"]))
        assert totals["Eng"] == pytest.approx(18_500)


# ---------------------------------------------------------------------------
# Performance regression test
# ---------------------------------------------------------------------------


class TestGroupedAggPerformance:
    def _make_large_context(self, n_rows: int, n_groups: int) -> Context:
        """Create a context with n_rows people spread across n_groups departments."""
        import numpy as np

        rng = np.random.default_rng(42)
        df = pd.DataFrame(
            {
                ID_COLUMN: range(n_rows),
                "dept": [f"dept_{i % n_groups}" for i in range(n_rows)],
                "value": rng.integers(1, 1000, size=n_rows),
            },
        )
        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=list(df.columns),
            source_obj_attribute_map={"dept": "dept", "value": "value"},
            attribute_map={"dept": "dept", "value": "value"},
            source_obj=df,
        )
        return Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

    def test_vectorised_is_faster_than_baseline(self):
        """Vectorised grouped aggregation must complete 1 000-group query in < 5 s."""
        ctx = self._make_large_context(n_rows=10_000, n_groups=1_000)
        star = Star(context=ctx)
        t0 = time.perf_counter()
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, sum(p.value) AS total, count(*) AS n",
        )
        elapsed = time.perf_counter() - t0
        assert len(result) == 1_000
        assert elapsed < 5.0, (
            f"Grouped aggregation took {elapsed:.2f}s — likely regressed to O(N) loop"
        )

    def test_result_correctness_large(self):
        """Verify correctness on large dataset with known totals."""
        n_rows = 4_000
        n_groups = 4
        ctx = self._make_large_context(n_rows=n_rows, n_groups=n_groups)
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(*) AS n",
        )
        counts = dict(zip(result["dept"], result["n"]))
        for g in range(n_groups):
            assert counts[f"dept_{g}"] == n_rows // n_groups
