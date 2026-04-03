"""Tests for cache hit/miss metrics collection.

Verifies that:
1. ``get_cache_stats()`` returns expected keys from all cache sources.
2. LRU cache stats reflect actual parse activity.
3. Lark parser cache stats are accessible.
4. Cache hit rate is non-zero after repeated queries.

Run with:
    uv run pytest tests/test_cache_metrics.py -v
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import ASTConverter, _parse_cypher_cached
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star, get_cache_stats


@pytest.fixture
def simple_star() -> Star:
    """Three-person context: Alice (30), Bob (25), Carol (35)."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


class TestGetCacheStats:
    """Verify get_cache_stats() returns well-formed metrics."""

    def test_returns_expected_keys(self) -> None:
        stats = get_cache_stats()
        expected_keys = {
            "lru_hits",
            "lru_misses",
            "lru_size",
            "lru_maxsize",
            "lru_hit_rate",
            "lark_cache_hits",
            "lark_cache_misses",
            "lru_at_capacity",
            "eviction_estimate",
        }
        assert expected_keys == set(stats.keys())

    def test_lru_maxsize_is_512(self) -> None:
        stats = get_cache_stats()
        assert stats["lru_maxsize"] == 512

    def test_values_are_numeric(self) -> None:
        stats = get_cache_stats()
        for key, value in stats.items():
            assert isinstance(value, (int, float)), (
                f"{key} should be numeric, got {type(value)}"
            )

    def test_hit_rate_between_0_and_1(self) -> None:
        stats = get_cache_stats()
        assert 0.0 <= stats["lru_hit_rate"] <= 1.0


class TestCacheStatsAfterParsing:
    """Verify cache stats reflect actual parsing activity."""

    def test_lru_miss_on_first_parse(self) -> None:
        # Clear cache to get a clean state
        _parse_cypher_cached.cache_clear()
        stats_before = get_cache_stats()
        before_misses = stats_before["lru_misses"]

        # Parse a unique query
        ASTConverter.from_cypher(
            "MATCH (uniqueNode1:Person) RETURN uniqueNode1.name AS n",
        )

        stats_after = get_cache_stats()
        assert stats_after["lru_misses"] > before_misses

    def test_lru_hit_on_repeated_parse(self) -> None:
        query = "MATCH (repeatTest:Person) RETURN repeatTest.name AS n"
        # First parse (miss)
        ASTConverter.from_cypher(query)
        stats_before = get_cache_stats()
        before_hits = stats_before["lru_hits"]

        # Second parse of same query (hit)
        ASTConverter.from_cypher(query)
        stats_after = get_cache_stats()
        assert stats_after["lru_hits"] > before_hits

    def test_lru_size_increases_with_unique_queries(self) -> None:
        _parse_cypher_cached.cache_clear()
        stats_before = get_cache_stats()
        before_size = stats_before["lru_size"]

        ASTConverter.from_cypher("MATCH (sizeTest1:Person) RETURN sizeTest1")
        ASTConverter.from_cypher("MATCH (sizeTest2:Person) RETURN sizeTest2")

        stats_after = get_cache_stats()
        assert stats_after["lru_size"] >= before_size + 2


class TestCacheStatsEndToEnd:
    """Verify cache stats work through Star.execute_query()."""

    def test_execute_query_populates_cache(self, simple_star: Star) -> None:
        stats_before = get_cache_stats()
        total_before = stats_before["lru_hits"] + stats_before["lru_misses"]

        simple_star.execute_query("MATCH (e2e:Person) RETURN e2e.name AS name")

        stats_after = get_cache_stats()
        total_after = stats_after["lru_hits"] + stats_after["lru_misses"]
        assert total_after > total_before

    def test_repeated_execute_gets_cache_hit(self, simple_star: Star) -> None:
        query = "MATCH (cacheHit:Person) RETURN cacheHit.name AS name"
        simple_star.execute_query(query)

        stats_before = get_cache_stats(star=simple_star)
        lru_hits_before = stats_before["lru_hits"]
        result_hits_before = stats_before.get("result_cache_hits", 0)

        simple_star.execute_query(query)

        stats_after = get_cache_stats(star=simple_star)
        # The second call hits either the result cache (fast path) or the
        # LRU parse cache — both are valid cache-hit outcomes.
        lru_hit = stats_after["lru_hits"] > lru_hits_before
        result_hit = (
            stats_after.get("result_cache_hits", 0) > result_hits_before
        )
        assert lru_hit or result_hit, (
            f"Expected a cache hit on repeated query. "
            f"LRU hits: {lru_hits_before} -> {stats_after['lru_hits']}, "
            f"Result cache hits: {result_hits_before} -> {stats_after.get('result_cache_hits', 0)}"
        )
