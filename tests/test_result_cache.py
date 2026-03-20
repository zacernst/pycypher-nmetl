"""Tests for query result caching with smart invalidation.

Verifies:
1. Cache hits on repeated read-only queries.
2. Cache miss after mutation (SET/CREATE/DELETE).
3. LRU eviction when cache is full.
4. TTL expiry.
5. Parameterised queries get distinct cache entries.
6. Cache stats are accurate.
7. Cache can be disabled (max_size_bytes=0).

Run with:
    uv run pytest tests/test_result_cache.py -v
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
from pycypher.star import ResultCache, Star, get_cache_stats

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def people_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        }
    )


@pytest.fixture()
def star_with_cache(people_df: pd.DataFrame) -> Star:
    """Star with result cache enabled (default 100 MB)."""
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
        result_cache_max_mb=10,
    )


@pytest.fixture()
def star_no_cache(people_df: pd.DataFrame) -> Star:
    """Star with result cache disabled."""
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
        result_cache_max_mb=0,
    )


# ---------------------------------------------------------------------------
# ResultCache unit tests
# ---------------------------------------------------------------------------


class TestResultCacheUnit:
    """Unit tests for the ResultCache class in isolation."""

    def test_put_and_get(self) -> None:
        cache = ResultCache(max_size_bytes=1024 * 1024)
        df = pd.DataFrame({"x": [1, 2, 3]})
        cache.put("MATCH (n) RETURN n", None, df)
        result = cache.get("MATCH (n) RETURN n", None)
        assert result is not None
        pd.testing.assert_frame_equal(result, df)

    def test_get_returns_copy(self) -> None:
        cache = ResultCache(max_size_bytes=1024 * 1024)
        df = pd.DataFrame({"x": [1, 2, 3]})
        cache.put("q", None, df)
        result = cache.get("q", None)
        assert result is not None
        result["x"] = [99, 99, 99]
        # Original cache entry should be unaffected
        fresh = cache.get("q", None)
        assert fresh is not None
        pd.testing.assert_frame_equal(fresh, df)

    def test_miss_on_unknown_key(self) -> None:
        cache = ResultCache(max_size_bytes=1024 * 1024)
        assert cache.get("unknown", None) is None

    def test_disabled_cache(self) -> None:
        cache = ResultCache(max_size_bytes=0)
        assert not cache.enabled
        df = pd.DataFrame({"x": [1]})
        cache.put("q", None, df)
        assert cache.get("q", None) is None

    def test_different_parameters_different_entries(self) -> None:
        cache = ResultCache(max_size_bytes=1024 * 1024)
        df1 = pd.DataFrame({"x": [1]})
        df2 = pd.DataFrame({"x": [2]})
        cache.put("q", {"a": 1}, df1)
        cache.put("q", {"a": 2}, df2)
        r1 = cache.get("q", {"a": 1})
        r2 = cache.get("q", {"a": 2})
        assert r1 is not None and r2 is not None
        assert r1["x"].iloc[0] == 1
        assert r2["x"].iloc[0] == 2

    def test_invalidate_causes_miss(self) -> None:
        cache = ResultCache(max_size_bytes=1024 * 1024)
        df = pd.DataFrame({"x": [1]})
        cache.put("q", None, df)
        assert cache.get("q", None) is not None
        cache.invalidate()
        assert cache.get("q", None) is None

    def test_clear_removes_all_entries(self) -> None:
        cache = ResultCache(max_size_bytes=1024 * 1024)
        for i in range(5):
            cache.put(f"q{i}", None, pd.DataFrame({"x": [i]}))
        cache.clear()
        stats = cache.stats()
        assert stats["result_cache_entries"] == 0
        assert stats["result_cache_size_bytes"] == 0

    def test_lru_eviction(self) -> None:
        # Tiny cache: fits ~1 small DataFrame
        small_df = pd.DataFrame({"x": [1]})
        entry_bytes = int(small_df.memory_usage(deep=True).sum())
        cache = ResultCache(max_size_bytes=entry_bytes + 10)

        cache.put("q1", None, small_df)
        cache.put("q2", None, small_df)  # Should evict q1

        assert cache.get("q1", None) is None
        assert cache.get("q2", None) is not None
        assert cache.stats()["result_cache_evictions"] >= 1

    def test_ttl_expiry(self) -> None:
        cache = ResultCache(max_size_bytes=1024 * 1024, ttl_seconds=0.05)
        df = pd.DataFrame({"x": [1]})
        cache.put("q", None, df)
        assert cache.get("q", None) is not None
        time.sleep(0.1)
        assert cache.get("q", None) is None

    def test_stats_accuracy(self) -> None:
        cache = ResultCache(max_size_bytes=1024 * 1024)
        df = pd.DataFrame({"x": [1]})

        cache.get("miss", None)  # miss
        cache.put("hit", None, df)
        cache.get("hit", None)  # hit
        cache.get("miss2", None)  # miss

        stats = cache.stats()
        assert stats["result_cache_hits"] == 1
        assert stats["result_cache_misses"] == 2
        assert stats["result_cache_entries"] == 1

    def test_oversized_entry_not_cached(self) -> None:
        cache = ResultCache(max_size_bytes=10)  # Tiny
        big_df = pd.DataFrame({"x": list(range(1000))})
        cache.put("big", None, big_df)
        assert cache.get("big", None) is None


# ---------------------------------------------------------------------------
# Integration tests via Star.execute_query
# ---------------------------------------------------------------------------


class TestResultCacheIntegration:
    """End-to-end tests through Star.execute_query."""

    def test_cache_hit_on_repeated_query(self, star_with_cache: Star) -> None:
        query = "MATCH (p:Person) RETURN p.name AS name"
        r1 = star_with_cache.execute_query(query)
        r2 = star_with_cache.execute_query(query)
        pd.testing.assert_frame_equal(r1, r2)

        stats = star_with_cache._result_cache.stats()
        assert stats["result_cache_hits"] >= 1

    def test_cache_invalidated_after_set(self, star_with_cache: Star) -> None:
        query = "MATCH (p:Person) RETURN p.name AS name, p.age AS age"
        star_with_cache.execute_query(query)

        # Mutation invalidates cache
        star_with_cache.execute_query(
            "MATCH (p:Person {name: 'Alice'}) SET p.age = 99 RETURN p.name"
        )

        # Should be a cache miss now
        stats_before = star_with_cache._result_cache.stats()
        misses_before = stats_before["result_cache_misses"]

        star_with_cache.execute_query(query)

        stats_after = star_with_cache._result_cache.stats()
        assert stats_after["result_cache_misses"] > misses_before

    def test_cache_invalidated_after_create(
        self, star_with_cache: Star
    ) -> None:
        query = "MATCH (p:Person) RETURN p.name AS name"
        star_with_cache.execute_query(query)

        star_with_cache.execute_query(
            "CREATE (p:Person {name: 'Dave', age: 40})"
        )

        # Cache generation bumped — next get of the old query should miss
        gen_after = star_with_cache._result_cache._generation
        assert gen_after >= 1

    def test_parameterized_queries_cached_separately(
        self,
        star_with_cache: Star,
    ) -> None:
        query = "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name AS name"

        r1 = star_with_cache.execute_query(query, parameters={"min_age": 20})
        r2 = star_with_cache.execute_query(query, parameters={"min_age": 34})

        # Different parameters should produce different results
        assert len(r1) != len(r2) or not r1.equals(r2)

    def test_disabled_cache_still_works(self, star_no_cache: Star) -> None:
        query = "MATCH (p:Person) RETURN p.name AS name"
        r1 = star_no_cache.execute_query(query)
        r2 = star_no_cache.execute_query(query)
        pd.testing.assert_frame_equal(r1, r2)

        stats = star_no_cache._result_cache.stats()
        assert stats["result_cache_hits"] == 0

    def test_get_cache_stats_includes_result_cache(
        self,
        star_with_cache: Star,
    ) -> None:
        star_with_cache.execute_query("MATCH (p:Person) RETURN p.name AS name")
        stats = get_cache_stats(star=star_with_cache)

        # Should include both parse cache and result cache keys
        assert "lru_hits" in stats
        assert "result_cache_hits" in stats
        assert "result_cache_misses" in stats
        assert "result_cache_hit_rate" in stats
        assert "result_cache_size_mb" in stats
        assert "result_cache_entries" in stats
        assert "result_cache_evictions" in stats
        assert "result_cache_max_mb" in stats

    def test_mutation_query_not_cached(self, star_with_cache: Star) -> None:
        # Mutation queries should NOT be stored in the cache
        star_with_cache.execute_query(
            "MATCH (p:Person {name: 'Alice'}) SET p.age = 31 RETURN p.name"
        )
        stats = star_with_cache._result_cache.stats()
        # The cache entry count should be 0 (mutation queries aren't cached)
        assert stats["result_cache_entries"] == 0
