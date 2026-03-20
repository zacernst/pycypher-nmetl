"""Tests targeting uncovered lines in collection_evaluator.py.

Exercises null slicing, TypeError in slicing, empty map literals,
quantifiers with null lists, reduce with varying-length lists,
and list comprehension null handling -- all through Star.execute_query().
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def star() -> Star:
    """Star with a single-row entity for standalone RETURN queries."""
    df = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "nums": [[10, 20, 30], [1, 2], [100]],
            "tags": [["a", "b"], None, ["c"]],
        }
    )
    return Star(context=ContextBuilder.from_dict({"Person": df}))


@pytest.fixture()
def minimal_star() -> Star:
    """Minimal star for standalone RETURN queries without MATCH."""
    df = pd.DataFrame({"__ID__": [1], "x": [1]})
    return Star(context=ContextBuilder.from_dict({"N": df}))


# ===========================================================================
# 1. Null collection slicing (line 243)
# ===========================================================================


class TestNullSlicing:
    """Slicing a null value should return null (line 243)."""

    def test_null_slice_returns_null(self, minimal_star: Star) -> None:
        """RETURN null[0..2] should yield null."""
        result = minimal_star.execute_query(
            "UNWIND [1] AS _ RETURN null[0..2] AS result"
        )
        assert pd.isna(result["result"].iloc[0])

    def test_null_slice_open_end(self, minimal_star: Star) -> None:
        """RETURN null[1..] should yield null."""
        result = minimal_star.execute_query(
            "UNWIND [1] AS _ RETURN null[1..] AS result"
        )
        assert pd.isna(result["result"].iloc[0])

    def test_null_slice_open_start(self, minimal_star: Star) -> None:
        """RETURN null[..3] should yield null."""
        result = minimal_star.execute_query(
            "UNWIND [1] AS _ RETURN null[..3] AS result"
        )
        assert pd.isna(result["result"].iloc[0])

    def test_null_property_slice(self, star: Star) -> None:
        """Slicing a property that is null for some rows."""
        # Bob's tags are null, so Bob's slice should be null
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.tags[0..1] AS sl "
            "ORDER BY name ASC"
        )
        alice_sl = result.loc[result["name"] == "Alice", "sl"].iloc[0]
        bob_sl = result.loc[result["name"] == "Bob", "sl"].iloc[0]
        assert alice_sl == ["a"]
        assert bob_sl is None or pd.isna(bob_sl)


# ===========================================================================
# 2. TypeError in slicing (lines 250-255)
# ===========================================================================


class TestSliceTypeError:
    """Slicing a non-list/non-string should return null (lines 250-255)."""

    def test_slice_integer_returns_null(self, star: Star) -> None:
        """Slicing an integer should silently return null."""
        # UNWIND an integer, then try to slice it
        result = star.execute_query(
            "UNWIND [42] AS val RETURN val[0..2] AS result"
        )
        # An integer cannot be sliced; should get null
        val = result["result"].iloc[0]
        assert val is None or pd.isna(val)


# ===========================================================================
# 3. Empty map literal (lines 791-795)
# ===========================================================================


class TestEmptyMapLiteral:
    """RETURN {} AS m should produce an empty dict (lines 791-795)."""

    def test_empty_map_literal(self, minimal_star: Star) -> None:
        """Empty map literal returns an empty dict."""
        result = minimal_star.execute_query("UNWIND [1] AS _ RETURN {} AS m")
        val = result["m"].iloc[0]
        assert val == {}

    def test_non_empty_map_literal(self, minimal_star: Star) -> None:
        """Contrast: non-empty map literal returns populated dict."""
        result = minimal_star.execute_query(
            "UNWIND [1] AS _ RETURN {a: 1, b: 2} AS m"
        )
        val = result["m"].iloc[0]
        assert val["a"] == 1
        assert val["b"] == 2


# ===========================================================================
# 4. Quantifier with null list (lines 476-482)
# ===========================================================================


class TestQuantifierNullList:
    """Quantifiers on null/empty lists hit the null detection paths."""

    def test_any_null_list_returns_false(self, star: Star) -> None:
        """ANY on a null list should return false."""
        # Bob has tags=null
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN any(t IN p.tags WHERE t = 'a') AS result"
        )
        val = result["result"].iloc[0]
        assert val is False or val == False  # noqa: E712

    def test_all_null_list_returns_true(self, star: Star) -> None:
        """ALL on a null list should return true (vacuous truth)."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN all(t IN p.tags WHERE t = 'a') AS result"
        )
        val = result["result"].iloc[0]
        assert val is True or val == True  # noqa: E712

    def test_none_null_list_returns_true(self, star: Star) -> None:
        """NONE on a null list should return true."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN none(t IN p.tags WHERE t = 'a') AS result"
        )
        val = result["result"].iloc[0]
        assert val is True or val == True  # noqa: E712

    def test_any_empty_list_returns_false(self, minimal_star: Star) -> None:
        """ANY on an empty literal list returns false."""
        result = minimal_star.execute_query(
            "UNWIND [1] AS _ RETURN any(x IN [] WHERE x > 0) AS result"
        )
        val = result["result"].iloc[0]
        assert val is False or val == False  # noqa: E712

    def test_all_empty_list_returns_true(self, minimal_star: Star) -> None:
        """ALL on an empty literal list returns true (vacuous truth)."""
        result = minimal_star.execute_query(
            "UNWIND [1] AS _ RETURN all(x IN [] WHERE x > 0) AS result"
        )
        val = result["result"].iloc[0]
        assert val is True or val == True  # noqa: E712

    def test_none_empty_list_returns_true(self, minimal_star: Star) -> None:
        """NONE on an empty literal list returns true."""
        result = minimal_star.execute_query(
            "UNWIND [1] AS _ RETURN none(x IN [] WHERE x > 0) AS result"
        )
        val = result["result"].iloc[0]
        assert val is True or val == True  # noqa: E712


# ===========================================================================
# 5. Reduce with short/varying-length lists (line 670)
# ===========================================================================


class TestReduceVaryingLengths:
    """Reduce with lists of varying lengths to exercise early termination."""

    def test_reduce_empty_list(self, minimal_star: Star) -> None:
        """reduce() on an empty list returns the initial value."""
        result = minimal_star.execute_query(
            "UNWIND [1] AS _ RETURN reduce(s = 0, x IN [] | s + x) AS result"
        )
        assert result["result"].iloc[0] == 0

    def test_reduce_single_element(self, minimal_star: Star) -> None:
        """reduce() on a single-element list."""
        result = minimal_star.execute_query(
            "UNWIND [1] AS _ RETURN reduce(s = 10, x IN [5] | s + x) AS result"
        )
        assert result["result"].iloc[0] == 15

    def test_reduce_varying_lengths_per_row(self, star: Star) -> None:
        """reduce() across rows with different list lengths (3, 2, 1).

        This exercises the batch-per-step loop where some rows terminate
        earlier than others (line 670 early break path).
        """
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, reduce(s = 0, x IN p.nums | s + x) AS total "
            "ORDER BY name ASC"
        )
        # Alice: 10+20+30=60, Bob: 1+2=3, Carol: 100
        alice_total = result.loc[result["name"] == "Alice", "total"].iloc[0]
        bob_total = result.loc[result["name"] == "Bob", "total"].iloc[0]
        carol_total = result.loc[result["name"] == "Carol", "total"].iloc[0]
        assert alice_total == 60
        assert bob_total == 3
        assert carol_total == 100

    def test_reduce_null_list(self, star: Star) -> None:
        """reduce() on a null list returns the initial value."""
        # Bob has tags=null
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN reduce(s = 'init', t IN p.tags | s + t) AS result"
        )
        assert result["result"].iloc[0] == "init"


# ===========================================================================
# 6. List comprehension null handling (lines 309-325)
# ===========================================================================


class TestListComprehensionNullHandling:
    """List comprehension where the source list is null."""

    def test_list_comp_null_list(self, star: Star) -> None:
        """[x IN null_list | x] should return empty list for null source."""
        # Bob has tags=null
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN [t IN p.tags | t] AS result"
        )
        val = result["result"].iloc[0]
        assert val == [] or val is None or pd.isna(val)

    def test_list_comp_normal_list(self, star: Star) -> None:
        """[x IN list | x] with a normal list returns the elements."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN [t IN p.tags | t] AS result"
        )
        val = result["result"].iloc[0]
        assert val == ["a", "b"]

    def test_list_comp_mixed_null_and_normal(self, star: Star) -> None:
        """List comprehension across rows with mixed null and non-null lists."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, [t IN p.tags | t] AS result "
            "ORDER BY name ASC"
        )
        alice_val = result.loc[result["name"] == "Alice", "result"].iloc[0]
        bob_val = result.loc[result["name"] == "Bob", "result"].iloc[0]
        carol_val = result.loc[result["name"] == "Carol", "result"].iloc[0]
        assert alice_val == ["a", "b"]
        # Bob's tags are null, so the comprehension yields empty list
        assert bob_val == [] or bob_val is None or pd.isna(bob_val)
        assert carol_val == ["c"]

    def test_list_comp_with_filter_null_list(self, star: Star) -> None:
        """[x IN null_list WHERE cond | x] with null source list."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN [t IN p.tags WHERE t = 'a' | t] AS result"
        )
        val = result["result"].iloc[0]
        assert val == [] or val is None or pd.isna(val)

    def test_list_comp_empty_literal_list(self, minimal_star: Star) -> None:
        """[x IN [] | x * 2] with empty literal list returns empty list."""
        result = minimal_star.execute_query(
            "UNWIND [1] AS _ RETURN [x IN [] | x * 2] AS result"
        )
        val = result["result"].iloc[0]
        assert val == []
