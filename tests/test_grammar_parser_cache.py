"""TDD tests for GrammarParser caching (Loop 165 — Performance loop).

Red-phase tests verify that the parser is not re-instantiated on every
``from_cypher()`` / ``execute_query()`` call.  After the fix, all tests pass
green.

The module-level cached factory ``get_default_parser()`` must return the same
object instance on repeated calls, and ``GrammarParser.__init__`` must be
called at most once per process (not once per query).
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest
from pycypher.ast_models import ASTConverter
from pycypher.grammar_parser import GrammarParser, get_default_parser


class TestGetDefaultParser:
    """Unit tests for the cached factory function."""

    def test_returns_grammar_parser_instance(self) -> None:
        """get_default_parser() returns a GrammarParser."""
        parser = get_default_parser()
        assert isinstance(parser, GrammarParser)

    def test_same_object_on_repeated_calls(self) -> None:
        """get_default_parser() returns the exact same object every time."""
        p1 = get_default_parser()
        p2 = get_default_parser()
        assert p1 is p2, (
            "Expected cached singleton; got two different instances"
        )

    def test_no_new_init_on_second_call(self) -> None:
        """Calling get_default_parser() twice does not call __init__ twice."""
        # Prime the cache so we start from a warm state
        get_default_parser()
        init_call_count = 0

        original_init = GrammarParser.__init__

        def counting_init(self, *args, **kwargs):  # type: ignore[override]
            nonlocal init_call_count
            init_call_count += 1
            return original_init(self, *args, **kwargs)

        with patch.object(GrammarParser, "__init__", counting_init):
            # Even if __init__ were somehow called again, the cached object
            # should be returned without calling __init__ at all.
            get_default_parser()
            get_default_parser()
            get_default_parser()

        assert init_call_count == 0, (
            f"GrammarParser.__init__ was called {init_call_count} time(s) "
            "after the cache was primed; expected 0 calls"
        )

    def test_parser_can_parse_a_query(self) -> None:
        """Cached parser is functional — can parse a real query."""
        parser = get_default_parser()
        tree = parser.parse("MATCH (n:Person) RETURN n.name")
        assert tree is not None


class TestFromCypherUsesCache:
    """Verify ASTConverter.from_cypher() delegates to the cached parser."""

    def test_from_cypher_does_not_construct_new_parser_each_call(self) -> None:
        """from_cypher() called N times must not call GrammarParser() N times."""
        init_calls: list[bool] = []
        original_init = GrammarParser.__init__

        def tracking_init(self, *args, **kwargs):  # type: ignore[override]
            init_calls.append(True)
            return original_init(self, *args, **kwargs)

        with patch.object(GrammarParser, "__init__", tracking_init):
            for _ in range(5):
                ASTConverter.from_cypher("MATCH (n:Person) RETURN n.name")

        assert len(init_calls) == 0, (
            f"GrammarParser.__init__ was called {len(init_calls)} time(s) "
            "across 5 from_cypher() invocations; expected 0 (cache should be warm)"
        )

    def test_execute_query_does_not_reconstruct_parser(self) -> None:
        """execute_query() called N times must not call GrammarParser() N times."""
        from pycypher import Star
        from pycypher.ingestion import ContextBuilder

        ctx = ContextBuilder.from_dict(
            {
                "Person": pd.DataFrame(
                    {
                        "__ID__": [1, 2, 3],
                        "name": ["Alice", "Bob", "Carol"],
                        "age": [30, 25, 35],
                    }
                )
            }
        )
        star = Star(context=ctx)

        init_calls: list[bool] = []
        original_init = GrammarParser.__init__

        def tracking_init(self, *args, **kwargs):  # type: ignore[override]
            init_calls.append(True)
            return original_init(self, *args, **kwargs)

        with patch.object(GrammarParser, "__init__", tracking_init):
            for _ in range(5):
                star.execute_query("MATCH (p:Person) RETURN p.name AS name")

        assert len(init_calls) == 0, (
            f"GrammarParser.__init__ called {len(init_calls)} time(s) "
            "across 5 execute_query() calls; expected 0 (cache should be warm)"
        )


class TestParserCacheCorrectness:
    """Regression: caching must not break query results."""

    @pytest.fixture()
    def star(self) -> "Star":  # type: ignore[name-defined]
        from pycypher import Star
        from pycypher.ingestion import ContextBuilder

        ctx = ContextBuilder.from_dict(
            {
                "Person": pd.DataFrame(
                    {
                        "__ID__": [1, 2, 3],
                        "name": ["Alice", "Bob", "Carol"],
                        "age": [30, 25, 35],
                    }
                )
            }
        )
        return Star(context=ctx)

    def test_repeated_queries_same_result(self, star) -> None:  # type: ignore[override]
        """Same query executed twice returns identical results."""
        r1 = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        r2 = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert list(r1["name"].sort_values()) == list(r2["name"].sort_values())

    def test_different_queries_correct_results(self, star) -> None:  # type: ignore[override]
        """Different queries interleaved still return correct independent results."""
        r_alice = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age AS age"
        )
        r_bob = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.age AS age"
        )
        assert r_alice["age"].iloc[0] == 30
        assert r_bob["age"].iloc[0] == 25
