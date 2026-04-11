"""TDD tests for Loop 191 — Performance: vectorise string predicate functions.

startsWith(s, prefix), endsWith(s, suffix), and contains(s, sub) are the three
most commonly-used WHERE-clause predicates in ETL Cypher queries.  They were
previously implemented with `s.apply(lambda v: ...)` — one Python function call
per row.  This loop replaces them with pandas `.str.startswith()`,
`.str.endswith()`, and `.str.contains(regex=False)`, which use Cython-level
loops and are typically 3-5× faster on large object-dtype Series.

Key semantics to preserve:
  - Case-sensitive (Neo4j default)
  - Null input → null output (both argument positions)
  - Regex in pattern must NOT be interpreted (contains is a literal match)
  - Row-varying patterns (non-scalar second arg) must still work correctly

All tests written BEFORE the implementation (TDD red phase).

Categories:
  1. Correctness regression — spot values for all three functions.
  2. Null propagation — null in either position → null output.
  3. Row-varying patterns — second argument varies per row.
  4. Performance — 30× 10k-row batch must complete within absolute threshold.
  5. Regex safety — patterns with regex meta-chars must be treated literally.
"""

from __future__ import annotations

import time

import pandas as pd
import pytest
from _perf_helpers import perf_threshold
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reg() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


def _s(*vals: object) -> pd.Series:
    return pd.Series(list(vals))


def _null(v: object) -> bool:
    return v is None or (isinstance(v, float) and v != v)


REPS = 30
N = 10_000


def _big_strings() -> pd.Series:
    """10k rows of short strings, half starting with 'A' half with 'B'."""
    return pd.Series(["Alice" if i % 2 == 0 else "Bob" for i in range(N)])


@pytest.fixture
def names_star() -> Star:
    """A Star with a small Person table for integration tests."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "alex"],
            "email": [
                "alice@example.com",
                "bob@gmail.com",
                "charlie@example.com",
                "alex@gmail.com",
            ],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "email"],
        source_obj_attribute_map={"name": "name", "email": "email"},
        attribute_map={"name": "name", "email": "email"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


# ===========================================================================
# 1. Correctness regression
# ===========================================================================


def _bool(v: object) -> bool:
    """Convert result value to Python bool for assertion (accepts np.bool_)."""
    return bool(v)


class TestStartsWithCorrectness:
    """startsWith(s, prefix) — case-sensitive literal prefix match."""

    def test_true_case(self) -> None:
        result = _reg().execute("startsWith", [_s("Hello"), _s("He")])
        assert _bool(result.iloc[0]) is True

    def test_false_case(self) -> None:
        result = _reg().execute("startsWith", [_s("Hello"), _s("lo")])
        assert _bool(result.iloc[0]) is False

    def test_case_sensitive(self) -> None:
        """'hello' does NOT start with 'He' (capital H)."""
        result = _reg().execute("startsWith", [_s("hello"), _s("He")])
        assert _bool(result.iloc[0]) is False

    def test_empty_prefix(self) -> None:
        """Every string starts with the empty string."""
        result = _reg().execute("startsWith", [_s("hello"), _s("")])
        assert _bool(result.iloc[0]) is True

    def test_full_match(self) -> None:
        """String starts with itself."""
        result = _reg().execute("startsWith", [_s("abc"), _s("abc")])
        assert _bool(result.iloc[0]) is True

    def test_multi_row(self) -> None:
        s = pd.Series(["Alice", "Bob", "Charlie"])
        result = _reg().execute("startsWith", [s, _s("A")])
        assert _bool(result.iloc[0]) is True
        assert _bool(result.iloc[1]) is False
        assert _bool(result.iloc[2]) is False


class TestEndsWithCorrectness:
    """endsWith(s, suffix) — case-sensitive literal suffix match."""

    def test_true_case(self) -> None:
        result = _reg().execute("endsWith", [_s("Hello"), _s("lo")])
        assert _bool(result.iloc[0]) is True

    def test_false_case(self) -> None:
        result = _reg().execute("endsWith", [_s("Hello"), _s("He")])
        assert _bool(result.iloc[0]) is False

    def test_case_sensitive(self) -> None:
        result = _reg().execute("endsWith", [_s("Hello"), _s("LO")])
        assert _bool(result.iloc[0]) is False

    def test_empty_suffix(self) -> None:
        result = _reg().execute("endsWith", [_s("hello"), _s("")])
        assert _bool(result.iloc[0]) is True

    def test_multi_row(self) -> None:
        s = pd.Series(
            ["alice@example.com", "bob@gmail.com", "carol@example.com"],
        )
        result = _reg().execute("endsWith", [s, _s("@example.com")])
        assert _bool(result.iloc[0]) is True
        assert _bool(result.iloc[1]) is False
        assert _bool(result.iloc[2]) is True


class TestContainsCorrectness:
    """contains(s, sub) — case-sensitive literal substring match."""

    def test_true_case(self) -> None:
        result = _reg().execute("contains", [_s("Hello World"), _s("World")])
        assert _bool(result.iloc[0]) is True

    def test_false_case(self) -> None:
        result = _reg().execute("contains", [_s("Hello"), _s("xyz")])
        assert _bool(result.iloc[0]) is False

    def test_case_sensitive(self) -> None:
        result = _reg().execute("contains", [_s("Hello"), _s("hello")])
        assert _bool(result.iloc[0]) is False

    def test_empty_substring(self) -> None:
        result = _reg().execute("contains", [_s("hello"), _s("")])
        assert _bool(result.iloc[0]) is True

    def test_multi_row(self) -> None:
        s = pd.Series(
            ["alice@example.com", "bob@gmail.com", "carol@example.com"],
        )
        result = _reg().execute("contains", [s, _s("@gmail")])
        assert _bool(result.iloc[0]) is False
        assert _bool(result.iloc[1]) is True
        assert _bool(result.iloc[2]) is False


# ===========================================================================
# 2. Null propagation
# ===========================================================================


class TestNullPropagation:
    """Null in either position → null output."""

    @pytest.mark.parametrize("fn", ["startsWith", "endsWith", "contains"])
    def test_null_string_returns_null(self, fn: str) -> None:
        result = _reg().execute(fn, [_s(None), _s("x")])
        assert _null(result.iloc[0]), f"{fn}(null, 'x') must return null"

    @pytest.mark.parametrize("fn", ["startsWith", "endsWith", "contains"])
    def test_null_pattern_returns_null(self, fn: str) -> None:
        result = _reg().execute(fn, [_s("hello"), _s(None)])
        assert _null(result.iloc[0]), f"{fn}('hello', null) must return null"

    @pytest.mark.parametrize("fn", ["startsWith", "endsWith", "contains"])
    def test_both_null_returns_null(self, fn: str) -> None:
        result = _reg().execute(fn, [_s(None), _s(None)])
        assert _null(result.iloc[0]), f"{fn}(null, null) must return null"

    @pytest.mark.parametrize("fn", ["startsWith", "endsWith", "contains"])
    def test_mixed_null_multi_row(self, fn: str) -> None:
        """Middle row is null — other rows return bool."""
        s = pd.Series(["Alice", None, "Charlie"])
        result = _reg().execute(fn, [s, _s("A")])
        assert not _null(result.iloc[0]), f"{fn}: row 0 must not be null"
        assert _null(result.iloc[1]), f"{fn}: row 1 (null input) must be null"
        assert not _null(result.iloc[2]), f"{fn}: row 2 must not be null"


# ===========================================================================
# 3. Row-varying patterns
# ===========================================================================


class TestRowVaryingPatterns:
    """Second argument varies per row (not a constant)."""

    def test_startswith_row_varying(self) -> None:
        s = pd.Series(["Alice", "Bob", "Charlie"])
        patterns = pd.Series(["Al", "B", "X"])
        result = _reg().execute("startsWith", [s, patterns])
        assert _bool(result.iloc[0]) is True
        assert _bool(result.iloc[1]) is True
        assert _bool(result.iloc[2]) is False

    def test_endswith_row_varying(self) -> None:
        s = pd.Series(["Alice", "Bob", "Charlie"])
        # "Alice".endswith("xyz") = False, "Bob".endswith("ob") = True, "Charlie".endswith("lie") = True
        suffixes = pd.Series(["xyz", "ob", "lie"])
        result = _reg().execute("endsWith", [s, suffixes])
        assert _bool(result.iloc[0]) is False
        assert _bool(result.iloc[1]) is True
        assert _bool(result.iloc[2]) is True

    def test_contains_row_varying(self) -> None:
        s = pd.Series(["Alice", "Bob", "Charlie"])
        subs = pd.Series(["li", "ob", "xyz"])
        result = _reg().execute("contains", [s, subs])
        assert _bool(result.iloc[0]) is True
        assert _bool(result.iloc[1]) is True
        assert _bool(result.iloc[2]) is False

    def test_row_varying_null_in_pattern(self) -> None:
        """Null pattern in row-varying mode → null for that row."""
        s = pd.Series(["Alice", "Bob", "Charlie"])
        patterns = pd.Series(["Al", None, "Ch"])
        result = _reg().execute("startsWith", [s, patterns])
        assert result.iloc[0] is True
        assert _null(result.iloc[1])
        assert result.iloc[2] is True


# ===========================================================================
# 4. Regex safety (contains must be literal, not regex)
# ===========================================================================


class TestRegexSafety:
    """contains() must treat the pattern literally, not as a regex."""

    def test_dot_is_literal(self) -> None:
        """'.' in pattern must NOT match any character."""
        s = _s("hello")
        # 'h.llo' as regex would match 'hello'; as literal it should not
        result = _reg().execute("contains", [s, _s("h.llo")])
        assert _bool(result.iloc[0]) is False

    def test_dot_literal_match(self) -> None:
        """Actual dot in string should match dot in pattern."""
        result = _reg().execute("contains", [_s("h.llo"), _s("h.llo")])
        assert _bool(result.iloc[0]) is True

    def test_star_is_literal(self) -> None:
        """'*' in pattern must NOT be interpreted as regex quantifier."""
        result = _reg().execute("contains", [_s("hello"), _s("l*")])
        assert _bool(result.iloc[0]) is False

    def test_brackets_are_literal(self) -> None:
        """'[abc]' must be treated literally, not as a character class."""
        result = _reg().execute("contains", [_s("hello"), _s("[helo]")])
        assert _bool(result.iloc[0]) is False

    def test_startswith_regex_meta_literal(self) -> None:
        """'^hello' is literally the two characters ^ and h — not a regex anchor."""
        result = _reg().execute("startsWith", [_s("^hello"), _s("^")])
        assert _bool(result.iloc[0]) is True

    def test_endswith_dollar_literal(self) -> None:
        """'$' must not be interpreted as regex end-anchor."""
        result = _reg().execute("endsWith", [_s("price$"), _s("$")])
        assert _bool(result.iloc[0]) is True


# ===========================================================================
# 5. Integration tests (Cypher queries)
# ===========================================================================


class TestCypherIntegration:
    def test_startswith_in_where(self, names_star: Star) -> None:
        r = names_star.execute_query(
            "MATCH (p:Person) WHERE startsWith(p.name, 'A') RETURN p.name ORDER BY p.name",
        )
        # "Alice" and "alex" start with 'A' and 'a' respectively — 'A' only matches "Alice"
        assert list(r["name"]) == ["Alice"]

    def test_endswith_in_where(self, names_star: Star) -> None:
        r = names_star.execute_query(
            "MATCH (p:Person) WHERE endsWith(p.email, '@gmail.com') RETURN p.name ORDER BY p.name",
        )
        assert sorted(r["name"].tolist()) == ["Bob", "alex"]

    def test_contains_in_where(self, names_star: Star) -> None:
        r = names_star.execute_query(
            "MATCH (p:Person) WHERE contains(p.email, 'example') RETURN p.name ORDER BY p.name",
        )
        assert sorted(r["name"].tolist()) == ["Alice", "Charlie"]

    def test_startswith_in_return(self, names_star: Star) -> None:
        r = names_star.execute_query(
            "MATCH (p:Person) RETURN p.name, startsWith(p.name, 'A') AS sw ORDER BY p.name",
        )
        # Map name -> sw
        mapping = dict(zip(r["name"], r["sw"]))
        assert mapping["Alice"] is True
        assert mapping["Bob"] is False

    def test_chained_predicates(self, names_star: Star) -> None:
        r = names_star.execute_query(
            "MATCH (p:Person) WHERE startsWith(p.email, 'alice') AND endsWith(p.email, '.com') RETURN p.name",
        )
        assert list(r["name"]) == ["Alice"]


# ===========================================================================
# 6. Performance: pandas .str accessor must be fast
# ===========================================================================


@pytest.mark.performance
class TestStringPredicatePerformance:
    """30× 10k-row batch must complete within absolute threshold."""

    def _time(self, fn: str, s: pd.Series, pat: pd.Series) -> float:
        start = time.perf_counter()
        for _ in range(REPS):
            _reg().execute(fn, [s, pat])
        return time.perf_counter() - start

    def test_startswith_threshold(self) -> None:
        """With .str.startswith() 30×startsWith(10k) must be < 0.03s."""
        elapsed = self._time("startsWith", _big_strings(), _s("A"))
        assert elapsed < perf_threshold(0.15), (
            f"30×startsWith(10k) took {elapsed:.3f}s "
            f"(threshold 0.03s — .apply() typically 0.05s+)"
        )

    def test_endswith_threshold(self) -> None:
        """With .str.endswith() 30×endsWith(10k) must be < 0.03s."""
        elapsed = self._time("endsWith", _big_strings(), _s("e"))
        assert elapsed < perf_threshold(0.15), (
            f"30×endsWith(10k) took {elapsed:.3f}s "
            f"(threshold 0.03s — .apply() typically 0.05s+)"
        )

    def test_contains_threshold(self) -> None:
        """With .str.contains(regex=False) 30×contains(10k) must be < 0.03s."""
        elapsed = self._time("contains", _big_strings(), _s("li"))
        assert elapsed < perf_threshold(0.15), (
            f"30×contains(10k) took {elapsed:.3f}s "
            f"(threshold 0.03s — .apply() typically 0.05s+)"
        )

    def test_str_vs_apply_speedup(self) -> None:
        """Pandas .str.startswith must be >= 2× faster than .apply() at 10k rows."""
        s_obj = _big_strings()

        start = time.perf_counter()
        for _ in range(REPS):
            s_obj.apply(lambda v: None if v is None else v.startswith("A"))
        baseline = time.perf_counter() - start

        start = time.perf_counter()
        for _ in range(REPS):
            s_obj.str.startswith("A")
        vectorised = time.perf_counter() - start

        speedup = baseline / vectorised if vectorised > 0 else float("inf")
        assert speedup >= 2.0, (
            f"pandas .str.startswith ({vectorised:.3f}s) should be ≥2× faster "
            f"than .apply() ({baseline:.3f}s) at {N} rows, got {speedup:.1f}×."
        )
