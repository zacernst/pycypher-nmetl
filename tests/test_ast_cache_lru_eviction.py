"""Security regression tests: AST cache LRU eviction.

Verifies that the AST cache in ``GrammarParser`` enforces a size limit
and evicts least-recently-used entries to prevent unbounded memory growth
from unique queries.
"""

from __future__ import annotations

import os

import pytest
from pycypher.grammar_parser import GrammarParser


class TestASTCacheLRUEviction:
    """AST cache must enforce a maximum size with LRU eviction."""

    def test_cache_respects_max_size(self) -> None:
        """Cache must not grow beyond the configured maximum."""
        parser = GrammarParser()
        parser._ast_cache_max = 5

        # Parse 10 unique queries
        for i in range(10):
            parser.parse_to_ast(f"RETURN {i}")

        assert len(parser._ast_cache) <= 5

    def test_lru_eviction_removes_oldest(self) -> None:
        """Least-recently-used entries must be evicted first."""
        parser = GrammarParser()
        parser._ast_cache_max = 3

        # Fill the cache with 3 entries
        parser.parse_to_ast("RETURN 1")
        parser.parse_to_ast("RETURN 2")
        parser.parse_to_ast("RETURN 3")

        # Access RETURN 1 to make it recently used
        parser.parse_to_ast("RETURN 1")

        # Add a 4th entry — should evict RETURN 2 (oldest unused)
        parser.parse_to_ast("RETURN 4")

        assert "RETURN 1" in parser._ast_cache
        assert "RETURN 2" not in parser._ast_cache
        assert "RETURN 3" in parser._ast_cache
        assert "RETURN 4" in parser._ast_cache

    def test_eviction_counter_tracked(self) -> None:
        """Eviction count must be tracked in cache stats."""
        parser = GrammarParser()
        parser._ast_cache_max = 2

        parser.parse_to_ast("RETURN 1")
        parser.parse_to_ast("RETURN 2")
        parser.parse_to_ast("RETURN 3")  # Triggers 1 eviction
        parser.parse_to_ast("RETURN 4")  # Triggers 1 eviction

        stats = parser.cache_stats
        assert stats["ast_evictions"] == 2
        assert stats["ast_max_size"] == 2
        assert stats["ast_size"] == 2

    def test_cache_stats_includes_max_and_evictions(self) -> None:
        """cache_stats must include ast_max_size and ast_evictions."""
        parser = GrammarParser()
        stats = parser.cache_stats
        assert "ast_max_size" in stats
        assert "ast_evictions" in stats
        assert stats["ast_max_size"] == parser._ast_cache_max
        assert stats["ast_evictions"] == 0

    def test_default_cache_size(self) -> None:
        """Default cache size must be 1024."""
        parser = GrammarParser()
        assert parser._ast_cache_max == 1024

    def test_config_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """AST_CACHE_MAX_ENTRIES config constant must control cache size."""
        import pycypher.config

        monkeypatch.setattr(pycypher.config, "AST_CACHE_MAX_ENTRIES", 256)
        parser = GrammarParser()
        assert parser._ast_cache_max == 256

    def test_cache_hit_updates_lru_order(self) -> None:
        """Accessing a cached entry must move it to most-recently-used."""
        parser = GrammarParser()
        parser._ast_cache_max = 3

        parser.parse_to_ast("RETURN 1")
        parser.parse_to_ast("RETURN 2")
        parser.parse_to_ast("RETURN 3")

        # Hit RETURN 1 — moves it to end (most recent)
        parser.parse_to_ast("RETURN 1")

        # The LRU order should now be: RETURN 2, RETURN 3, RETURN 1
        keys = list(parser._ast_cache.keys())
        assert keys == ["RETURN 2", "RETURN 3", "RETURN 1"]
