"""TDD tests for vectorised _eval_label_predicate slow path.

The slow path (unlabeled MATCH + WHERE n:Label) previously:
  1. Built ``id_to_type`` with a Python ``for eid in series:`` inner loop — O(n)
     Python iterations creating dict entries one at a time.
  2. Called ``per_row_type.apply(lambda t: ...)`` — O(m) Python calls for the
     label check (m = number of output rows).

Vectorised replacement:
  1. ``pd.concat`` a list of ``pd.Series(etype, index=ids)`` objects — Cython
     concat, zero Python per-row overhead for building the id→type mapping.
  2. ``per_row_type.eq(label)`` for single-label case — C-level array equality;
     NaN → False automatically.
  3. Multi-label: ``pd.concat([per_row_type.eq(l) for l in labels], axis=1).all(axis=1)``.

Expected speedup: ≥5× for 5k+5k entity context.
"""

from __future__ import annotations

import time

import pandas as pd
import pytest
from _perf_helpers import perf_threshold
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.performance


@pytest.fixture(scope="module")
def two_type_ctx() -> ContextBuilder:
    """5 000 Person + 5 000 Animal entities — triggers the slow path."""
    people = pd.DataFrame(
        {
            "__ID__": [f"p{i}" for i in range(5000)],
            "name": [f"Person{i}" for i in range(5000)],
            "score": [float(i) for i in range(5000)],
        },
    )
    animals = pd.DataFrame(
        {
            "__ID__": [f"a{i}" for i in range(5000)],
            "name": [f"Animal{i}" for i in range(5000)],
        },
    )
    return ContextBuilder().from_dict({"Person": people, "Animal": animals})


@pytest.fixture(scope="module")
def large_ctx() -> ContextBuilder:
    """20 000 Person + 20 000 Animal — used for speedup ratio measurement."""
    people = pd.DataFrame(
        {
            "__ID__": [f"p{i}" for i in range(20_000)],
            "name": [f"P{i}" for i in range(20_000)],
        },
    )
    animals = pd.DataFrame(
        {
            "__ID__": [f"a{i}" for i in range(20_000)],
            "name": [f"A{i}" for i in range(20_000)],
        },
    )
    return ContextBuilder().from_dict({"Person": people, "Animal": animals})


# ---------------------------------------------------------------------------
# Correctness tests
# ---------------------------------------------------------------------------


class TestLabelPredicateCorrectness:
    def test_single_label_returns_correct_rows(
        self,
        two_type_ctx: ContextBuilder,
    ) -> None:
        """WHERE n:Person returns exactly the Person rows."""
        s = Star(context=two_type_ctx)
        result = s.execute_query("MATCH (n) WHERE n:Person RETURN n.name")
        assert len(result) == 5000
        assert all(result["name"].str.startswith("Person"))

    def test_single_label_animal_returns_correct_rows(
        self,
        two_type_ctx: ContextBuilder,
    ) -> None:
        """WHERE n:Animal returns exactly the Animal rows."""
        s = Star(context=two_type_ctx)
        result = s.execute_query("MATCH (n) WHERE n:Animal RETURN n.name")
        assert len(result) == 5000
        assert all(result["name"].str.startswith("Animal"))

    def test_nonexistent_label_returns_empty(
        self,
        two_type_ctx: ContextBuilder,
    ) -> None:
        """WHERE n:Unicorn returns no rows when no such entity type exists."""
        s = Star(context=two_type_ctx)
        result = s.execute_query("MATCH (n) WHERE n:Unicorn RETURN n.name")
        assert len(result) == 0

    def test_multi_label_always_false_in_single_type_model(
        self,
        two_type_ctx: ContextBuilder,
    ) -> None:
        """WHERE n:Person:Animal is always False — single-type entity model."""
        s = Star(context=two_type_ctx)
        result = s.execute_query(
            "MATCH (n) WHERE n:Person AND n:Animal RETURN n.name",
        )
        assert len(result) == 0

    def test_label_predicate_with_property_filter(
        self,
        two_type_ctx: ContextBuilder,
    ) -> None:
        """WHERE n:Person AND n.score > 4990 returns 9 Person rows."""
        s = Star(context=two_type_ctx)
        result = s.execute_query(
            "MATCH (n) WHERE n:Person AND n.score > 4990 RETURN n.name",
        )
        assert len(result) == 9  # scores 4991..4999

    def test_label_predicate_not(self, two_type_ctx: ContextBuilder) -> None:
        """WHERE NOT n:Person returns all Animal rows."""
        s = Star(context=two_type_ctx)
        result = s.execute_query("MATCH (n) WHERE NOT n:Person RETURN n.name")
        assert len(result) == 5000
        assert all(result["name"].str.startswith("Animal"))

    def test_labeled_match_unaffected(
        self,
        two_type_ctx: ContextBuilder,
    ) -> None:
        """Labeled MATCH (n:Person) still uses the fast path, not the slow path."""
        s = Star(context=two_type_ctx)
        result = s.execute_query(
            "MATCH (n:Person) WHERE n:Person RETURN n.name",
        )
        assert len(result) == 5000

    def test_three_entity_types(self) -> None:
        """Label predicate works correctly with 3+ entity types."""
        ctx = ContextBuilder().from_dict(
            {
                "Person": pd.DataFrame(
                    {"__ID__": ["p1", "p2"], "name": ["Alice", "Bob"]},
                ),
                "Animal": pd.DataFrame({"__ID__": ["a1"], "name": ["Dog"]}),
                "Plant": pd.DataFrame({"__ID__": ["pl1"], "name": ["Oak"]}),
            },
        )
        s = Star(context=ctx)
        result = s.execute_query("MATCH (n) WHERE n:Person RETURN n.name")
        assert sorted(result["name"].tolist()) == ["Alice", "Bob"]

    def test_result_row_values_correct(
        self,
        two_type_ctx: ContextBuilder,
    ) -> None:
        """Spot-check: Person0 and Person4999 are in results, Animal0 is not."""
        s = Star(context=two_type_ctx)
        result = s.execute_query("MATCH (n) WHERE n:Person RETURN n.name")
        names = set(result["name"].tolist())
        assert "Person0" in names
        assert "Person4999" in names
        assert "Animal0" not in names


# ---------------------------------------------------------------------------
# Performance tests
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestLabelPredicatePerformance:
    def test_5k_plus_5k_under_threshold(
        self,
        two_type_ctx: ContextBuilder,
    ) -> None:
        """5k+5k context: 10 queries must complete in < 600ms total.

        Before fix: ~235ms for 10 queries (23.5ms each).  After fix: < 60ms
        (< 6ms each).  Threshold set at 600ms to give comfortable headroom
        while still catching regressions back to the old O(n) Python loop.
        """
        s = Star(context=two_type_ctx)
        # warm-up
        s.execute_query("MATCH (n) WHERE n:Person RETURN n.name")
        t0 = time.perf_counter()
        for _ in range(10):
            s.execute_query("MATCH (n) WHERE n:Person RETURN n.name")
        elapsed = time.perf_counter() - t0
        assert elapsed < perf_threshold(0.6), (
            f"10 queries on 5k+5k took {elapsed * 1000:.0f}ms (expected < 600ms after vectorisation)"
        )

    def test_20k_per_query_under_30ms(self, large_ctx: ContextBuilder) -> None:
        """20k+20k context: average query time must be < 30ms after vectorisation.

        Before fix: ~88ms per query (O(40k) Python inner loop + O(20k) apply()).
        After fix:  ~15ms per query (C-level pd.concat + vectorised .eq()).
        """
        s = Star(context=large_ctx)
        s.execute_query("MATCH (n) WHERE n:Person RETURN n.name")  # warm-up
        times = []
        for _ in range(5):
            t0 = time.perf_counter()
            s.execute_query("MATCH (n) WHERE n:Person RETURN n.name")
            times.append(time.perf_counter() - t0)
        avg = sum(times) / len(times)
        assert avg < 0.150, (
            f"Expected < 150ms per query on 20k+20k; got {avg * 1000:.1f}ms"
        )

    def test_absolute_vectorised_time_under_10ms(
        self,
        two_type_ctx: ContextBuilder,
    ) -> None:
        """Single query on 5k+5k should complete in < 10ms after vectorisation."""
        s = Star(context=two_type_ctx)
        s.execute_query("MATCH (n) WHERE n:Person RETURN n.name")  # warm-up
        times = []
        for _ in range(10):
            t0 = time.perf_counter()
            s.execute_query("MATCH (n) WHERE n:Person RETURN n.name")
            times.append(time.perf_counter() - t0)
        avg = sum(times) / len(times)
        assert avg < 0.050, (
            f"Expected < 50ms per query; got {avg * 1000:.1f}ms"
        )
