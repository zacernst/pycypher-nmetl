"""Coverage-gap tests for pycypher.star module.

Targets uncovered paths in _literal_from_python_value, _query_has_mutations,
available_functions, explain_query, _execute_union_query, _extract_limit_hint,
_execute_query_binding_frame, _coerce_join, and _merge_frames_for_match.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import ContextBuilder, Star, get_cache_stats
from pycypher.star import ResultCache, _literal_from_python_value


@pytest.fixture
def star() -> Star:
    """Star with Person entities and KNOWS relationships."""
    people = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    rels = pd.DataFrame(
        {
            "__ID__": [100, 101],
            "__SOURCE__": [1, 2],
            "__TARGET__": [2, 3],
            "since": [2020, 2021],
        },
    )
    ctx = (
        ContextBuilder()
        .add_entity("Person", people)
        .add_relationship(
            "KNOWS", rels, source_col="__SOURCE__", target_col="__TARGET__",
        )
        .build()
    )
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# _literal_from_python_value
# ---------------------------------------------------------------------------


class TestLiteralFromPythonValue:
    """Tests for _literal_from_python_value helper."""

    def test_bool_true(self) -> None:
        from pycypher.ast_models import BooleanLiteral

        result = _literal_from_python_value(True)
        assert isinstance(result, BooleanLiteral)
        assert result.value is True

    def test_bool_false(self) -> None:
        from pycypher.ast_models import BooleanLiteral

        result = _literal_from_python_value(False)
        assert isinstance(result, BooleanLiteral)
        assert result.value is False

    def test_int(self) -> None:
        from pycypher.ast_models import IntegerLiteral

        result = _literal_from_python_value(42)
        assert isinstance(result, IntegerLiteral)
        assert result.value == 42

    def test_float(self) -> None:
        from pycypher.ast_models import FloatLiteral

        result = _literal_from_python_value(3.14)
        assert isinstance(result, FloatLiteral)
        assert result.value == pytest.approx(3.14)

    def test_string_fallback(self) -> None:
        from pycypher.ast_models import StringLiteral

        result = _literal_from_python_value("hello")
        assert isinstance(result, StringLiteral)
        assert result.value == "hello"

    def test_non_primitive_converts_to_string(self) -> None:
        from pycypher.ast_models import StringLiteral

        result = _literal_from_python_value([1, 2, 3])
        assert isinstance(result, StringLiteral)
        assert result.value == "[1, 2, 3]"

    def test_bool_before_int(self) -> None:
        """Bool must be checked before int since bool is subclass of int."""
        from pycypher.ast_models import BooleanLiteral

        result = _literal_from_python_value(True)
        assert isinstance(result, BooleanLiteral)  # NOT IntegerLiteral


# ---------------------------------------------------------------------------
# ResultCache
# ---------------------------------------------------------------------------


class TestResultCache:
    """Tests for ResultCache class."""

    def test_cache_disabled_by_default(self) -> None:
        cache = ResultCache(max_size_bytes=0)
        assert not cache.enabled

    def test_cache_enabled_with_size(self) -> None:
        cache = ResultCache(max_size_bytes=1024)
        assert cache.enabled

    def test_put_and_get(self) -> None:
        cache = ResultCache(max_size_bytes=10 * 1024 * 1024)
        df = pd.DataFrame({"a": [1, 2, 3]})
        cache.put("test_query", None, df)
        result = cache.get("test_query", None)
        assert result is not None
        assert len(result) == 3

    def test_cache_miss(self) -> None:
        cache = ResultCache(max_size_bytes=10 * 1024 * 1024)
        assert cache.get("nonexistent", None) is None

    def test_invalidate(self) -> None:
        cache = ResultCache(max_size_bytes=10 * 1024 * 1024)
        df = pd.DataFrame({"a": [1]})
        cache.put("q1", None, df)
        cache.invalidate()
        assert cache.get("q1", None) is None

    def test_clear(self) -> None:
        cache = ResultCache(max_size_bytes=10 * 1024 * 1024)
        df = pd.DataFrame({"a": [1]})
        cache.put("q1", None, df)
        cache.clear()
        assert cache.get("q1", None) is None

    def test_stats(self) -> None:
        cache = ResultCache(max_size_bytes=10 * 1024 * 1024)
        stats = cache.stats()
        assert isinstance(stats, dict)
        assert len(stats) > 0


# ---------------------------------------------------------------------------
# get_cache_stats
# ---------------------------------------------------------------------------


class TestGetCacheStats:
    """Tests for get_cache_stats function."""

    def test_without_star(self) -> None:
        stats = get_cache_stats()
        assert isinstance(stats, dict)

    def test_with_star(self, star: Star) -> None:
        stats = get_cache_stats(star)
        assert isinstance(stats, dict)


# ---------------------------------------------------------------------------
# Star._query_has_mutations
# ---------------------------------------------------------------------------


class TestQueryHasMutations:
    """Tests for Star._query_has_mutations static method."""

    def test_select_query_has_no_mutations(self) -> None:
        from pycypher.ast_models import _parse_cypher_cached

        parsed = _parse_cypher_cached("MATCH (n:Person) RETURN n")
        assert Star._query_has_mutations(parsed) is False

    def test_create_query_has_mutations(self) -> None:
        from pycypher.ast_models import _parse_cypher_cached

        parsed = _parse_cypher_cached("CREATE (n:Person {name: 'Alice'})")
        assert Star._query_has_mutations(parsed) is True

    def test_set_query_has_mutations(self) -> None:
        from pycypher.ast_models import _parse_cypher_cached

        parsed = _parse_cypher_cached("MATCH (n:Person) SET n.age = 30 RETURN n")
        assert Star._query_has_mutations(parsed) is True


# ---------------------------------------------------------------------------
# Star.available_functions
# ---------------------------------------------------------------------------


class TestAvailableFunctions:
    """Tests for Star.available_functions()."""

    def test_returns_sorted_list(self, star: Star) -> None:
        funcs = star.available_functions()
        assert isinstance(funcs, list)
        assert len(funcs) > 0
        assert funcs == sorted(funcs)

    def test_contains_common_functions(self, star: Star) -> None:
        funcs = star.available_functions()
        # Should include standard Cypher functions
        assert "toupper" in funcs or "toUpper" in funcs or "upper" in funcs


# ---------------------------------------------------------------------------
# Star.explain_query
# ---------------------------------------------------------------------------


class TestExplainQuery:
    """Tests for Star.explain_query()."""

    def test_explain_simple_match(self, star: Star) -> None:
        plan = star.explain_query("MATCH (n:Person) RETURN n.name")
        assert isinstance(plan, str)
        assert len(plan) > 0

    def test_explain_with_where(self, star: Star) -> None:
        plan = star.explain_query(
            "MATCH (n:Person) WHERE n.age > 25 RETURN n.name",
        )
        assert isinstance(plan, str)

    def test_explain_with_relationship(self, star: Star) -> None:
        plan = star.explain_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
        )
        assert isinstance(plan, str)


# ---------------------------------------------------------------------------
# UNION queries (_execute_union_query)
# ---------------------------------------------------------------------------


class TestUnionQuery:
    """Tests for UNION query execution."""

    def test_union_all(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name AS name "
            "UNION ALL "
            "MATCH (n:Person) WHERE n.age < 30 RETURN n.name AS name",
        )
        names = set(result["name"])
        assert "Carol" in names
        assert "Bob" in names

    def test_union_dedup(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (n:Person) RETURN n.name AS name "
            "UNION "
            "MATCH (n:Person) RETURN n.name AS name",
        )
        # UNION removes duplicates
        assert len(result) == 3


# ---------------------------------------------------------------------------
# LIMIT and SKIP (_extract_limit_hint, _apply_projection_modifiers)
# ---------------------------------------------------------------------------


class TestLimitSkip:
    """Tests for LIMIT and SKIP processing."""

    def test_limit(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name LIMIT 2",
        )
        assert len(result) == 2

    def test_skip(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name SKIP 1",
        )
        assert len(result) == 2
        assert result["name"].iloc[0] == "Bob"

    def test_limit_and_skip(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name SKIP 1 LIMIT 1",
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "Bob"


# ---------------------------------------------------------------------------
# Star.__repr__
# ---------------------------------------------------------------------------


class TestStarRepr:
    """Tests for Star.__repr__()."""

    def test_repr_contains_backend(self, star: Star) -> None:
        r = repr(star)
        assert "Star(" in r
        assert "backend=" in r

    def test_repr_contains_entities(self, star: Star) -> None:
        r = repr(star)
        assert "entities=" in r


# ---------------------------------------------------------------------------
# Multi-MATCH and _merge_frames_for_match
# ---------------------------------------------------------------------------


class TestMultiMatch:
    """Tests for multi-MATCH clauses that exercise _merge_frames_for_match."""

    def test_two_match_clauses(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (a:Person) "
            "MATCH (b:Person) "
            "WHERE a.name = 'Alice' AND b.name = 'Bob' "
            "RETURN a.name AS a_name, b.name AS b_name",
        )
        assert len(result) >= 1
        assert result["a_name"].iloc[0] == "Alice"
        assert result["b_name"].iloc[0] == "Bob"


# ---------------------------------------------------------------------------
# UNWIND (_unwind_binding_frame, _process_unwind_clause)
# ---------------------------------------------------------------------------


class TestUnwind:
    """Tests for UNWIND clause processing."""

    def test_unwind_simple(self, star: Star) -> None:
        result = star.execute_query(
            "UNWIND [1, 2, 3] AS x RETURN x ORDER BY x",
        )
        assert list(result["x"]) == [1, 2, 3]

    def test_unwind_with_match(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS name "
            "UNWIND [1, 2] AS x "
            "RETURN name, x "
            "ORDER BY name, x",
        )
        assert len(result) == 6  # 3 people × 2 elements


# ---------------------------------------------------------------------------
# WITH clause (_with_to_binding_frame)
# ---------------------------------------------------------------------------


class TestWithClause:
    """Tests for WITH clause processing."""

    def test_with_alias(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS n, p.age AS a RETURN n, a ORDER BY n",
        )
        assert list(result["n"]) == ["Alice", "Bob", "Carol"]

    def test_with_where(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS n, p.age AS a "
            "WHERE a > 25 "
            "RETURN n ORDER BY n",
        )
        assert "Bob" not in list(result["n"])


# ---------------------------------------------------------------------------
# WHERE filter (_apply_where_filter)
# ---------------------------------------------------------------------------


class TestWhereFilter:
    """Tests for WHERE clause processing."""

    def test_where_comparison(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age >= 30 RETURN p.name AS name ORDER BY name",
        )
        assert set(result["name"]) == {"Alice", "Carol"}

    def test_where_string_comparison(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.age AS age",
        )
        assert result["age"].iloc[0] == 25
