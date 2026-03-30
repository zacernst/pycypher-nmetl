"""TDD tests for property-lookup index caching (Loop 172 — Performance).

Problem: ``BindingFrame.get_property()`` calls ``raw_df.set_index(ID_COLUMN)``
on every invocation.  On a 5 000-row entity table this takes ~0.25 ms per call.
For a query such as ``WHERE p.age > 30 RETURN p.name, p.dept, p.age`` that
issues 4 property lookups per execution, 100 repeated executions cause 400
unnecessary index rebuilds (~100 ms wasted).

Fix: add a ``_property_lookup_cache: dict[str, pd.DataFrame]`` ``PrivateAttr``
to ``Context`` that maps entity_type → ``raw_df.set_index(ID_COLUMN)``.
``get_property()`` checks the cache before calling ``set_index``; the first
call per entity type populates it.  ``commit_query()`` clears the cache so
mutations see fresh data.

All tests are written before the fix (TDD red phase).
"""

from __future__ import annotations

import time
from unittest.mock import patch

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
def people_context() -> Context:
    """A 5 000-row Person context with age, name, dept attributes."""
    n = 5_000
    df = pd.DataFrame(
        {
            ID_COLUMN: list(range(1, n + 1)),
            "name": [f"P{i}" for i in range(1, n + 1)],
            "age": [20 + (i % 50) for i in range(1, n + 1)],
            "dept": [f"D{i % 10}" for i in range(1, n + 1)],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "dept"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "dept": "dept",
        },
        attribute_map={"name": "name", "age": "age", "dept": "dept"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture
def people_star(people_context: Context) -> Star:
    return Star(context=people_context)


# ---------------------------------------------------------------------------
# Category 1 — Cache is populated on first call
# ---------------------------------------------------------------------------


class TestPropertyLookupCachePopulation:
    """The cache must be populated by the first EntityScan or get_property call."""

    def test_cache_populated_after_scan(self, people_context: Context) -> None:
        """_property_lookup_cache must contain 'Person' key after EntityScan.scan().

        EntityScan now pre-warms the cache so that subsequent get_property calls
        are cache hits from the very first access.
        """
        from pycypher.binding_frame import BindingFrame, EntityScan

        # Clear the cache so this test is deterministic regardless of prior tests
        people_context._property_lookup_cache.clear()  # type: ignore[attr-defined]
        people_context.begin_query()
        frame: BindingFrame = EntityScan(
            entity_type="Person",
            var_name="p",
        ).scan(people_context)

        # After scan, cache must be populated (EntityScan pre-warms it)
        cache: dict = people_context._property_lookup_cache  # type: ignore[attr-defined]
        assert "Person" in cache, (
            "_property_lookup_cache not populated after EntityScan.scan(). "
            "EntityScan should pre-warm the cache to avoid redundant Arrow conversions."
        )

        # get_property on the same entity must still return correct values
        result = frame.get_property("p", "name")
        assert result.iloc[0] == "P1"

    def test_cache_entry_is_indexed_dataframe(
        self,
        people_context: Context,
    ) -> None:
        """The cached object must be a DataFrame indexed by ID_COLUMN."""
        from pycypher.binding_frame import EntityScan

        people_context.begin_query()
        frame = EntityScan(entity_type="Person", var_name="p").scan(
            people_context,
        )
        frame.get_property("p", "age")

        cached_df: pd.DataFrame = people_context._property_lookup_cache["Person"]  # type: ignore[attr-defined]
        assert isinstance(cached_df, pd.DataFrame), (
            f"Cache entry must be a DataFrame, got {type(cached_df)}"
        )
        assert cached_df.index.name == ID_COLUMN, (
            f"Cached DataFrame must be indexed by '{ID_COLUMN}', "
            f"got index.name={cached_df.index.name!r}"
        )

    def test_multiple_properties_reuse_same_cache_entry(
        self,
        people_context: Context,
    ) -> None:
        """Accessing three properties after EntityScan must call set_index 0 times.

        EntityScan pre-warms the cache; subsequent get_property calls hit the cache
        and never call set_index.  Total set_index calls within the patch scope: 0.
        """
        from pycypher.binding_frame import EntityScan

        # Clear the cache so EntityScan will re-populate it
        people_context._property_lookup_cache.clear()  # type: ignore[attr-defined]
        people_context.begin_query()
        # EntityScan.scan() populates the cache (BEFORE patch is active)
        frame = EntityScan(entity_type="Person", var_name="p").scan(
            people_context,
        )

        original_source = people_context.entity_mapping.mapping["Person"].source_obj
        set_index_calls = {"n": 0}
        orig_fn = pd.DataFrame.set_index

        def counting_set_index(
            df_self: pd.DataFrame,
            *args: object,
            **kwargs: object,
        ) -> pd.DataFrame:
            if df_self is original_source:
                set_index_calls["n"] += 1
            return orig_fn(df_self, *args, **kwargs)

        with patch.object(pd.DataFrame, "set_index", counting_set_index):
            # All three hits are cache hits — set_index must NOT be called
            frame.get_property("p", "name")
            frame.get_property("p", "age")
            frame.get_property("p", "dept")

        assert set_index_calls["n"] == 0, (
            f"Expected 0 set_index calls for 3 properties on the same entity type "
            f"after EntityScan pre-warmed the cache, but got {set_index_calls['n']}. "
            "The cache is not being reused after EntityScan pre-warm."
        )


# ---------------------------------------------------------------------------
# Category 2 — Cache is cleared on commit
# ---------------------------------------------------------------------------


class TestPropertyLookupCacheInvalidation:
    """commit_query() must clear the cache after mutations; retain it after read-only queries."""

    def test_cache_cleared_after_mutating_commit(
        self,
        people_context: Context,
    ) -> None:
        """_property_lookup_cache must be empty after commit_query() that had mutations."""
        from pycypher.binding_frame import EntityScan

        people_context.begin_query()
        frame = EntityScan(entity_type="Person", var_name="p").scan(
            people_context,
        )
        frame.get_property("p", "name")

        assert "Person" in people_context._property_lookup_cache  # type: ignore[attr-defined]

        # Simulate a mutation by injecting a shadow entry before commit.
        canonical_df = people_context.entity_mapping.mapping["Person"].source_obj
        people_context._shadow["Person"] = canonical_df.copy()  # type: ignore[attr-defined]

        people_context.commit_query()

        assert "Person" not in people_context._property_lookup_cache, (  # type: ignore[attr-defined]
            "Cache was not cleared after a mutating commit_query(). "
            "Post-commit queries may read stale indexed data."
        )

    def test_cache_retained_after_readonly_commit(
        self,
        people_context: Context,
    ) -> None:
        """Cache is NOT cleared after a read-only commit — the data is unchanged."""
        from pycypher.binding_frame import EntityScan

        people_context.begin_query()
        frame = EntityScan(entity_type="Person", var_name="p").scan(
            people_context,
        )
        frame.get_property("p", "name")
        assert "Person" in people_context._property_lookup_cache  # type: ignore[attr-defined]

        # No mutations — commit should preserve the cache.
        people_context.commit_query()

        assert "Person" in people_context._property_lookup_cache, (  # type: ignore[attr-defined]
            "Cache was incorrectly cleared after a read-only commit_query(). "
            "Cross-query cache reuse optimisation is broken."
        )

    def test_cache_repopulated_after_commit(
        self,
        people_context: Context,
    ) -> None:
        """After commit, the next get_property call must rebuild the cache."""
        from pycypher.binding_frame import EntityScan

        people_context.begin_query()
        frame = EntityScan(entity_type="Person", var_name="p").scan(
            people_context,
        )
        frame.get_property("p", "name")
        people_context.commit_query()

        # New query — begin_query + get_property should repopulate cache
        people_context.begin_query()
        frame2 = EntityScan(entity_type="Person", var_name="p").scan(
            people_context,
        )
        frame2.get_property("p", "name")

        assert "Person" in people_context._property_lookup_cache  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Category 3 — Correctness: cached results match uncached
# ---------------------------------------------------------------------------


class TestPropertyLookupCacheCorrectness:
    """Cached lookups must return numerically identical results."""

    def test_name_lookup_correct(self, people_context: Context) -> None:
        """get_property('p', 'name') returns the right names after caching."""
        from pycypher.binding_frame import EntityScan

        people_context.begin_query()
        frame = EntityScan(entity_type="Person", var_name="p").scan(
            people_context,
        )

        # First call populates cache; second call uses it
        result1 = frame.get_property("p", "name")
        result2 = frame.get_property("p", "name")

        pd.testing.assert_series_equal(
            result1.reset_index(drop=True),
            result2.reset_index(drop=True),
            check_names=False,
        )
        assert result1.iloc[0] == "P1", f"Expected 'P1', got {result1.iloc[0]!r}"

    def test_age_lookup_correct(self, people_context: Context) -> None:
        """get_property('p', 'age') values are in the expected range."""
        from pycypher.binding_frame import EntityScan

        people_context.begin_query()
        frame = EntityScan(entity_type="Person", var_name="p").scan(
            people_context,
        )
        ages = frame.get_property("p", "age")
        assert ages.between(20, 69).all(), "Ages should all be in [20, 69]"

    def test_query_results_unchanged(self, people_star: Star) -> None:
        """Full query results must be unchanged after the cache is introduced."""
        result = people_star.execute_query(
            "MATCH (p:Person) WHERE p.age = 20 RETURN p.name AS name ORDER BY p.name",
        )
        # IDs 1, 51, 101, ... have age 20+0=20. Check we got the right count.
        # With n=5000 and age=20+(i%50) for i in 1..5000: age==20 when i%50==0 → i=50,100,...
        # i from 1..5000: i%50==0 → i=50,100,...,5000 → 100 rows
        assert len(result) == 100, f"Expected 100 rows, got {len(result)}"
        # All returned names should have IDs where (id % 50) == 0
        for name in result["name"]:
            num = int(name[1:])  # "P50" → 50
            assert num % 50 == 0, f"Unexpected name {name!r}"


# ---------------------------------------------------------------------------
# Category 4 — Performance: set_index called once not N times
# ---------------------------------------------------------------------------


class TestPropertyLookupCachePerformance:
    """With caching, 100 queries × 3 properties = 100 set_index calls, not 300."""

    def test_set_index_call_count_with_caching(
        self,
        people_star: Star,
    ) -> None:
        """set_index must be called ≤ REPS times (once per query-begin) for a
        repeated query that accesses 3 properties.

        Without caching: 100 queries × 3 prop × 1 set_index = 300 calls.
        With caching:    100 queries × 1 set_index (first prop) = 100 calls.
        (Each begin_query() clears the cache; without explicit begin_query clearing
        the cache persists, so it could be as few as 1 call total.)
        """
        REPS = 20
        original_source = people_star.context.entity_mapping.mapping[
            "Person"
        ].source_obj
        set_index_calls = {"n": 0}
        orig_fn = pd.DataFrame.set_index

        def counting_set_index(df_self: pd.DataFrame, *args, **kwargs):  # type: ignore[no-untyped-def]
            if df_self is original_source:
                set_index_calls["n"] += 1
            return orig_fn(df_self, *args, **kwargs)

        with patch.object(pd.DataFrame, "set_index", counting_set_index):
            for _ in range(REPS):
                people_star.execute_query(
                    "MATCH (p:Person) WHERE p.age > 30 "
                    "RETURN p.name AS name, p.dept AS dept, p.age AS age",
                )

        # Without caching: 3 set_index calls per query = REPS * 3 = 60
        # With caching:    at most REPS calls (1 per query) = 20
        assert set_index_calls["n"] <= REPS, (
            f"Expected ≤{REPS} set_index calls for {REPS} queries × 3 properties "
            f"with caching, but got {set_index_calls['n']} calls. "
            "The property-lookup index cache is not reusing the indexed DataFrame."
        )

    def test_repeated_queries_wall_clock(self, people_star: Star) -> None:
        """100 queries × 3 property accesses must complete under 1.0s total."""
        REPS = 100
        THRESHOLD = 1.0  # seconds

        # Warm up AST cache
        people_star.execute_query(
            "MATCH (p:Person) WHERE p.age > 30 "
            "RETURN p.name AS name, p.dept AS dept, p.age AS age",
        )

        start = time.perf_counter()
        for _ in range(REPS):
            people_star.execute_query(
                "MATCH (p:Person) WHERE p.age > 30 "
                "RETURN p.name AS name, p.dept AS dept, p.age AS age",
            )
        elapsed = time.perf_counter() - start

        assert elapsed < THRESHOLD, (
            f"{REPS} queries × 3 property accesses took {elapsed:.3f}s "
            f"(threshold {THRESHOLD}s). "
            "Property-lookup caching may not be active."
        )
