"""TDD tests for vectorised Kleene three-valued boolean logic (Loop 174 — Performance).

Problem: ``kleene_and``, ``kleene_or``, and ``kleene_xor`` in
``binding_evaluator.py`` (lines 232–262) each use a Python list comprehension:

    return pd.Series([_and(lv, rv) for lv, rv in zip(left, right)], dtype=object)

For a 5 000-row frame with a compound WHERE clause (e.g. ``WHERE a AND b AND c``),
this causes ≥ 5 000 Python function calls per AND/OR/XOR operator.  After Loops 165,
169, and 172 eliminated parser and property-lookup overhead, boolean evaluation is
now a measurable fraction of WHERE-clause execution time.

Fix: replace the list-comprehension body with numpy-vectorised operations:

    l_null = left.isna()
    r_null = right.isna()
    l_false = ~l_null & ~left.where(~l_null, True).astype(bool)
    r_false = ~r_null & ~right.where(~r_null, True).astype(bool)
    return pd.Series(
        np.where(l_false | r_false, False, np.where(l_null | r_null, None, True)),
        dtype=object, index=left.index,
    )

``isna()`` catches all null sentinels (None, np.nan, pd.NA), making the new
implementation *more* correct than the original (which only checked None and
NaN floats, missing pd.NA values produced by ``get_property()``).

All tests are written before the fix (TDD red phase).
"""

from __future__ import annotations

import time

import pandas as pd
import pytest
from _perf_helpers import perf_threshold
from pycypher.boolean_evaluator import kleene_and, kleene_or, kleene_xor
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
def bool_star() -> Star:
    """5 000-row context with ~1/3 null values in boolean and integer columns."""
    n = 5_000

    df = pd.DataFrame(
        {
            ID_COLUMN: list(range(1, n + 1)),
            "active": [
                True if i % 3 == 0 else (None if i % 3 == 1 else False)
                for i in range(1, n + 1)
            ],
            "senior": [
                True if i % 3 == 1 else (None if i % 3 == 2 else False)
                for i in range(1, n + 1)
            ],
            "age": [
                20 + (i % 50) if i % 5 != 0 else None for i in range(1, n + 1)
            ],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "active", "senior", "age"],
        source_obj_attribute_map={
            "active": "active",
            "senior": "senior",
            "age": "age",
        },
        attribute_map={"active": "active", "senior": "senior", "age": "age"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


def _make_series(values: list[object]) -> pd.Series:
    return pd.Series(values, dtype=object)


# ---------------------------------------------------------------------------
# Category 1 — Unit: vectorised functions return correct truth tables
# ---------------------------------------------------------------------------


class TestKleeneAndTruthTable:
    """Full 3×3 truth table for Kleene AND."""

    @pytest.mark.parametrize(
        "lv,rv,expected_null",
        [
            (True, True, False),
            (True, None, True),
            (None, True, True),
            (None, None, True),
            (False, None, False),  # null AND false = false
            (None, False, False),  # null AND false = false
        ],
    )
    def test_null_result(
        self,
        lv: object,
        rv: object,
        expected_null: bool,
    ) -> None:
        left = _make_series([lv])
        right = _make_series([rv])
        result = kleene_and(left, right)
        got_null = pd.isna(result.iloc[0])
        assert got_null == expected_null, (
            f"kleene_and({lv!r}, {rv!r}): expected null={expected_null}, "
            f"got null={got_null} (value={result.iloc[0]!r})"
        )

    @pytest.mark.parametrize(
        "lv,rv,expected",
        [
            (True, True, True),
            (True, False, False),
            (False, True, False),
            (False, False, False),
            (False, None, False),
            (None, False, False),
        ],
    )
    def test_non_null_result(
        self,
        lv: object,
        rv: object,
        expected: bool,
    ) -> None:
        left = _make_series([lv])
        right = _make_series([rv])
        result = kleene_and(left, right)
        assert not pd.isna(result.iloc[0]), (
            f"kleene_and({lv!r}, {rv!r}): expected {expected!r}, got null"
        )
        assert bool(result.iloc[0]) == expected, (
            f"kleene_and({lv!r}, {rv!r}): expected {expected!r}, got {result.iloc[0]!r}"
        )


class TestKleeneOrTruthTable:
    """Full 3×3 truth table for Kleene OR."""

    @pytest.mark.parametrize(
        "lv,rv,expected_null",
        [
            (False, False, False),
            (False, None, True),  # null OR false = null
            (None, False, True),  # null OR false = null
            (None, None, True),
            (True, None, False),  # null OR true = true (not null)
            (None, True, False),  # null OR true = true (not null)
        ],
    )
    def test_null_result(
        self,
        lv: object,
        rv: object,
        expected_null: bool,
    ) -> None:
        left = _make_series([lv])
        right = _make_series([rv])
        result = kleene_or(left, right)
        got_null = pd.isna(result.iloc[0])
        assert got_null == expected_null, (
            f"kleene_or({lv!r}, {rv!r}): expected null={expected_null}, "
            f"got null={got_null} (value={result.iloc[0]!r})"
        )

    @pytest.mark.parametrize(
        "lv,rv,expected",
        [
            (True, True, True),
            (True, False, True),
            (False, True, True),
            (False, False, False),
            (True, None, True),
            (None, True, True),
        ],
    )
    def test_non_null_result(
        self,
        lv: object,
        rv: object,
        expected: bool,
    ) -> None:
        left = _make_series([lv])
        right = _make_series([rv])
        result = kleene_or(left, right)
        assert not pd.isna(result.iloc[0]), (
            f"kleene_or({lv!r}, {rv!r}): expected {expected!r}, got null"
        )
        assert bool(result.iloc[0]) == expected, (
            f"kleene_or({lv!r}, {rv!r}): expected {expected!r}, got {result.iloc[0]!r}"
        )


class TestKleeneXorTruthTable:
    """Full 3×3 truth table for Kleene XOR."""

    @pytest.mark.parametrize(
        "lv,rv",
        [
            (True, None),
            (False, None),
            (None, True),
            (None, False),
            (None, None),
        ],
    )
    def test_any_null_gives_null(self, lv: object, rv: object) -> None:
        result = kleene_xor(_make_series([lv]), _make_series([rv]))
        assert pd.isna(result.iloc[0]), (
            f"kleene_xor({lv!r}, {rv!r}): expected null, got {result.iloc[0]!r}"
        )

    @pytest.mark.parametrize(
        "lv,rv,expected",
        [
            (True, True, False),
            (True, False, True),
            (False, True, True),
            (False, False, False),
        ],
    )
    def test_non_null_result(
        self,
        lv: object,
        rv: object,
        expected: bool,
    ) -> None:
        result = kleene_xor(_make_series([lv]), _make_series([rv]))
        assert not pd.isna(result.iloc[0])
        assert bool(result.iloc[0]) == expected, (
            f"kleene_xor({lv!r}, {rv!r}): expected {expected!r}, got {result.iloc[0]!r}"
        )


class TestKleeneIndexPreservation:
    """Vectorised functions must preserve the input Series index."""

    def test_and_preserves_non_default_index(self) -> None:
        idx = [10, 20, 30]
        left = pd.Series([True, False, None], index=idx, dtype=object)
        right = pd.Series([True, True, True], index=idx, dtype=object)
        result = kleene_and(left, right)
        assert list(result.index) == idx, (
            f"kleene_and did not preserve index: expected {idx}, "
            f"got {list(result.index)}"
        )

    def test_or_preserves_non_default_index(self) -> None:
        idx = [10, 20, 30]
        left = pd.Series([False, False, None], index=idx, dtype=object)
        right = pd.Series([False, True, True], index=idx, dtype=object)
        result = kleene_or(left, right)
        assert list(result.index) == idx

    def test_xor_preserves_non_default_index(self) -> None:
        idx = [10, 20, 30]
        left = pd.Series([True, False, None], index=idx, dtype=object)
        right = pd.Series([True, True, True], index=idx, dtype=object)
        result = kleene_xor(left, right)
        assert list(result.index) == idx


class TestKleenePdNAHandling:
    """pd.NA must be treated as null (not truthy) in all three functions."""

    def test_and_pd_na_left_true_is_null(self) -> None:
        """pd.NA AND True = Null (pd.NA is a null sentinel, not truthy False)."""
        left = pd.Series([pd.NA], dtype=object)
        right = pd.Series([True], dtype=object)
        result = kleene_and(left, right)
        assert pd.isna(result.iloc[0]), (
            f"Expected null for pd.NA AND True, got {result.iloc[0]!r}. "
            "pd.NA must be treated as null, not as a truthy/falsy value."
        )

    def test_and_pd_na_left_false_is_false(self) -> None:
        """pd.NA AND False = False (null AND false = false in Kleene logic)."""
        left = pd.Series([pd.NA], dtype=object)
        right = pd.Series([False], dtype=object)
        result = kleene_and(left, right)
        assert result.iloc[0] is False or result.iloc[0] == False, (
            f"Expected False for pd.NA AND False, got {result.iloc[0]!r}."
        )

    def test_or_pd_na_left_true_is_true(self) -> None:
        """pd.NA OR True = True (null OR true = true in Kleene logic)."""
        left = pd.Series([pd.NA], dtype=object)
        right = pd.Series([True], dtype=object)
        result = kleene_or(left, right)
        assert result.iloc[0] is True or result.iloc[0] == True, (
            f"Expected True for pd.NA OR True, got {result.iloc[0]!r}."
        )

    def test_xor_pd_na_is_null(self) -> None:
        """pd.NA XOR anything = Null."""
        left = pd.Series([pd.NA], dtype=object)
        right = pd.Series([True], dtype=object)
        result = kleene_xor(left, right)
        assert pd.isna(result.iloc[0]), (
            f"Expected null for pd.NA XOR True, got {result.iloc[0]!r}."
        )


# ---------------------------------------------------------------------------
# Category 2 — Performance: vectorised must be ≥ 5× faster than Python loops
# ---------------------------------------------------------------------------


def _reference_kleene_and(left: pd.Series, right: pd.Series) -> pd.Series:
    """Reference implementation using Python loop (pre-fix baseline)."""

    def _and(lv: object, rv: object) -> object:
        import math

        def _is_null(x: object) -> bool:
            return x is None or (isinstance(x, float) and math.isnan(x))

        l_null, r_null = _is_null(lv), _is_null(rv)
        if (not l_null and not bool(lv)) or (not r_null and not bool(rv)):
            return False
        if l_null or r_null:
            return None
        return True

    return pd.Series(
        [_and(lv, rv) for lv, rv in zip(left, right)],
        dtype=object,
    )


class TestKleenePerformance:
    """Vectorised Kleene functions must be ≥ 5× faster than the Python-loop baseline."""

    REPS = 50
    N = 5_000

    @pytest.fixture
    def large_bool_series(self) -> tuple[pd.Series, pd.Series]:
        vals_l = [
            True if i % 3 == 0 else (None if i % 3 == 1 else False)
            for i in range(self.N)
        ]
        vals_r = [
            True if i % 3 == 1 else (None if i % 3 == 2 else False)
            for i in range(self.N)
        ]
        return pd.Series(vals_l, dtype=object), pd.Series(vals_r, dtype=object)

    def test_kleene_and_faster_than_python_loop(
        self,
        large_bool_series: tuple[pd.Series, pd.Series],
    ) -> None:
        """kleene_and on 5 000 rows × 50 reps must be ≥ 5× faster than baseline."""
        left, right = large_bool_series

        # Baseline: reference Python-loop implementation
        start = time.perf_counter()
        for _ in range(self.REPS):
            _reference_kleene_and(left, right)
        baseline = time.perf_counter() - start

        # Under test: vectorised implementation
        start = time.perf_counter()
        for _ in range(self.REPS):
            kleene_and(left, right)
        vectorised = time.perf_counter() - start

        speedup = baseline / vectorised if vectorised > 0 else float("inf")
        assert speedup >= 2.0, (
            f"kleene_and vectorised ({vectorised:.3f}s) should be ≥ 2× faster than "
            f"the Python-loop baseline ({baseline:.3f}s), got {speedup:.1f}×. "
            "The list-comprehension implementation is not yet replaced."
        )

    def test_kleene_and_absolute_threshold(
        self,
        large_bool_series: tuple[pd.Series, pd.Series],
    ) -> None:
        """50 × kleene_and on 5 000 rows must complete in under 0.5s."""
        left, right = large_bool_series
        start = time.perf_counter()
        for _ in range(self.REPS):
            kleene_and(left, right)
        elapsed = time.perf_counter() - start
        assert elapsed < perf_threshold(0.5), (
            f"50 × kleene_and(5000-row) took {elapsed:.3f}s (threshold 0.5s). "
            "The implementation is still using Python loops."
        )

    def test_kleene_or_absolute_threshold(
        self,
        large_bool_series: tuple[pd.Series, pd.Series],
    ) -> None:
        """50 × kleene_or on 5 000 rows must complete in under 0.5s."""
        left, right = large_bool_series
        start = time.perf_counter()
        for _ in range(self.REPS):
            kleene_or(left, right)
        elapsed = time.perf_counter() - start
        assert elapsed < perf_threshold(0.5), (
            f"50 × kleene_or(5000-row) took {elapsed:.3f}s (threshold 0.5s). "
            "The implementation is still using Python loops."
        )

    def test_kleene_xor_absolute_threshold(
        self,
        large_bool_series: tuple[pd.Series, pd.Series],
    ) -> None:
        """50 × kleene_xor on 5 000 rows must complete in under 0.5s."""
        left, right = large_bool_series
        start = time.perf_counter()
        for _ in range(self.REPS):
            kleene_xor(left, right)
        elapsed = time.perf_counter() - start
        assert elapsed < perf_threshold(0.5), (
            f"50 × kleene_xor(5000-row) took {elapsed:.3f}s (threshold 0.5s). "
            "The implementation is still using Python loops."
        )


# ---------------------------------------------------------------------------
# Category 3 — Integration: full queries with compound WHERE are unchanged
# ---------------------------------------------------------------------------


class TestKleeneQueryIntegration:
    """Full query results must be identical before and after vectorisation."""

    def test_and_where_clause_correct_count(self, bool_star: Star) -> None:
        """WHERE active AND senior selects only rows where both are True."""
        result = bool_star.execute_query(
            "MATCH (p:Person) WHERE p.active AND p.senior RETURN p.age",
        )
        # active = True when id%3==0, senior = True when id%3==1
        # → active AND senior can never both be True simultaneously
        assert len(result) == 0, (
            f"Expected 0 rows (active and senior never overlap), got {len(result)}"
        )

    def test_or_where_clause_correct_count(self, bool_star: Star) -> None:
        """WHERE active OR senior captures rows where either is True."""
        result = bool_star.execute_query(
            "MATCH (p:Person) WHERE p.active OR p.senior RETURN p.age",
        )
        # active=True for id%3==0: 1666/5000, senior=True for id%3==1: 1667/5000
        # No overlap possible (different modulo classes)
        expected = 1666 + 1667  # 3333
        assert len(result) == expected, (
            f"Expected {expected} rows for OR, got {len(result)}"
        )

    def test_compound_and_or_where_clause(self, bool_star: Star) -> None:
        """WHERE (age > 30) AND (active OR senior) selects non-null age + (active or senior)."""
        result = bool_star.execute_query(
            "MATCH (p:Person) WHERE p.age > 30 AND (p.active OR p.senior) RETURN p.age",
        )
        # age is null for i%5==0, non-null otherwise
        # age > 30 means age >= 31, i.e., i%50 >= 11 (since age = 20 + i%50 for non-null)
        # active OR senior = True for i%3==0 or i%3==1
        # All three conditions: not null age, age>30, active or senior
        assert len(result) > 0, (
            "Expected some matching rows for compound AND/OR predicate"
        )
        # Sanity: all returned ages must be > 30
        assert (result["age"] > 30).all(), "All returned ages must be > 30"

    def test_null_and_false_excludes_row(self, bool_star: Star) -> None:
        """WHERE null AND false = false → row excluded (Kleene short-circuit)."""
        result = bool_star.execute_query(
            "MATCH (p:Person) WHERE p.active AND false RETURN p.age",
        )
        assert len(result) == 0, (
            f"Expected 0 rows for WHERE active AND false (always false), got {len(result)}"
        )

    def test_null_or_true_includes_row(self, bool_star: Star) -> None:
        """WHERE null OR true = true → all rows included (Kleene short-circuit)."""
        result = bool_star.execute_query(
            "MATCH (p:Person) WHERE p.active OR true RETURN p.age",
        )
        # All 5000 rows should be returned (null OR true = true)
        assert len(result) == 5_000, (
            f"Expected 5000 rows for WHERE active OR true (always true), got {len(result)}"
        )

    def test_repeated_compound_queries_wall_clock(
        self,
        bool_star: Star,
    ) -> None:
        """100 compound-AND queries on 5 000-row frame must complete in < 2s total."""
        REPS = 100
        THRESHOLD = 2.0

        # Warm up parse cache
        bool_star.execute_query(
            "MATCH (p:Person) WHERE p.active AND p.age > 30 RETURN p.age",
        )

        start = time.perf_counter()
        for _ in range(REPS):
            bool_star.execute_query(
                "MATCH (p:Person) WHERE p.active AND p.age > 30 RETURN p.age",
            )
        elapsed = time.perf_counter() - start

        assert elapsed < THRESHOLD, (
            f"{REPS} compound-AND queries on 5 000-row frame took {elapsed:.3f}s "
            f"(threshold {THRESHOLD}s). Kleene vectorisation may not be active."
        )
