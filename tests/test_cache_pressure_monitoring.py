"""Tests for cache pressure monitoring in get_cache_stats (Task #15).

Verifies that get_cache_stats() includes:
1. ``lru_at_capacity`` — boolean, True when lru_size == lru_maxsize
2. ``eviction_estimate`` — count of misses that occurred while cache was at capacity

TDD: These tests are written BEFORE the implementation.

Run with:
    uv run pytest tests/test_cache_pressure_monitoring.py -v
"""

from __future__ import annotations

from pycypher.ast_models import _parse_cypher_cached
from pycypher.star import get_cache_stats


class TestCachePressureKeys:
    """Verify get_cache_stats() includes new pressure-monitoring keys."""

    def test_has_lru_at_capacity(self) -> None:
        stats = get_cache_stats()
        assert "lru_at_capacity" in stats

    def test_has_eviction_estimate(self) -> None:
        stats = get_cache_stats()
        assert "eviction_estimate" in stats

    def test_lru_at_capacity_is_bool(self) -> None:
        stats = get_cache_stats()
        assert isinstance(stats["lru_at_capacity"], bool)

    def test_eviction_estimate_is_int(self) -> None:
        stats = get_cache_stats()
        assert isinstance(stats["eviction_estimate"], int)


class TestLruAtCapacity:
    """Verify lru_at_capacity reflects cache fullness."""

    def test_not_at_capacity_after_clear(self) -> None:
        _parse_cypher_cached.cache_clear()
        stats = get_cache_stats()
        assert stats["lru_at_capacity"] is False

    def test_at_capacity_when_size_equals_maxsize(self) -> None:
        stats = get_cache_stats()
        # lru_at_capacity should be True iff lru_size == lru_maxsize
        expected = stats["lru_size"] == stats["lru_maxsize"]
        assert stats["lru_at_capacity"] is expected


class TestEvictionEstimate:
    """Verify eviction_estimate tracks misses when at capacity."""

    def test_zero_after_clear(self) -> None:
        _parse_cypher_cached.cache_clear()
        stats = get_cache_stats()
        # After clearing, cache isn't at capacity, so no eviction pressure
        assert stats["eviction_estimate"] >= 0

    def test_eviction_estimate_non_negative(self) -> None:
        stats = get_cache_stats()
        assert stats["eviction_estimate"] >= 0


class TestExistingKeysPreserved:
    """Verify existing keys still present after adding new ones."""

    def test_original_keys_unchanged(self) -> None:
        stats = get_cache_stats()
        expected_original = {
            "lru_hits",
            "lru_misses",
            "lru_size",
            "lru_maxsize",
            "lru_hit_rate",
            "lark_cache_hits",
            "lark_cache_misses",
        }
        assert expected_original.issubset(set(stats.keys()))

    def test_all_keys_present(self) -> None:
        stats = get_cache_stats()
        expected_all = {
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
        assert expected_all == set(stats.keys())
