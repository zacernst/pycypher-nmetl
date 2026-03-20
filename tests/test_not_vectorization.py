"""TDD tests for vectorised Kleene NOT (Loop 182 — Performance).

Problem: ``_eval_not`` in ``binding_evaluator.py`` uses ``Series.apply(lambda)``::

    return s.apply(
        lambda x: None if (x is None or (isinstance(x, float) and pd.isna(x))) else not bool(x)
    )

This means every ``NOT`` in every ``WHERE`` clause causes O(n_rows) Python
function calls.  Loop 174 vectorised AND/OR/XOR but missed NOT.  For a
5 000-row frame, ``WHERE NOT (a AND b)`` pays 5 000 Python calls.

Fix: add a ``kleene_not(s)`` function alongside the existing
``_kleene_and/or/xor`` functions, using the same numpy-vectorised idiom::

    null = s.isna()
    s_bool = s.where(~null, False).astype(bool)
    return pd.Series(
        np.where(null, None, ~s_bool.values),
        dtype=object, index=s.index,
    )

Then ``_eval_not`` becomes::

    return kleene_not(self.evaluate(operand_expr))

All tests written before the implementation (TDD red phase).
"""

from __future__ import annotations

import time

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _star_empty():
    from pycypher.relational_models import (
        Context,
        EntityMapping,
        RelationshipMapping,
    )
    from pycypher.star import Star

    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


def _q(cypher: str):
    return _star_empty().execute_query(cypher)


def _star_with_people():
    import pandas as pd
    from pycypher.relational_models import (
        ID_COLUMN,
        Context,
        EntityMapping,
        EntityTable,
        RelationshipMapping,
    )
    from pycypher.star import Star

    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dan"],
            "age": [30, 17, 25, 40],
            "active": [True, False, True, False],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "active"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "active": "active",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "active": "active",
        },
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


# ---------------------------------------------------------------------------
# Reference (pre-fix) implementation for speedup comparison
# ---------------------------------------------------------------------------


def _reference_kleene_not(s: pd.Series) -> pd.Series:
    """Baseline: one Python function call per element (pre-fix)."""
    import math

    return s.apply(
        lambda x: (
            None
            if (x is None or (isinstance(x, float) and math.isnan(x)))
            else not bool(x)
        )
    )


# ===========================================================================
# Category 1 — Three-valued truth table for NOT
# ===========================================================================


class TestKleeneNotTruthTable:
    """NOT x must implement three-valued Kleene logic."""

    def test_not_true_is_false(self) -> None:
        from pycypher.boolean_evaluator import kleene_not

        s = pd.Series([True], dtype=object)
        result = kleene_not(s)
        assert result.iloc[0] is False or result.iloc[0] == False  # noqa: E712

    def test_not_false_is_true(self) -> None:
        from pycypher.boolean_evaluator import kleene_not

        s = pd.Series([False], dtype=object)
        result = kleene_not(s)
        assert result.iloc[0] is True or result.iloc[0] == True  # noqa: E712

    def test_not_none_is_none(self) -> None:
        from pycypher.boolean_evaluator import kleene_not

        s = pd.Series([None], dtype=object)
        result = kleene_not(s)
        assert pd.isna(result.iloc[0])

    def test_not_nan_is_none(self) -> None:
        """NaN float must be treated as null → NOT nan = null."""
        from pycypher.boolean_evaluator import kleene_not

        s = pd.Series([float("nan")], dtype=object)
        result = kleene_not(s)
        assert pd.isna(result.iloc[0])

    def test_not_pd_na_is_null(self) -> None:
        """pd.NA must be treated as null — not swallowed into False."""
        from pycypher.boolean_evaluator import kleene_not

        s = pd.Series([pd.NA], dtype=object)
        result = kleene_not(s)
        assert pd.isna(result.iloc[0]), (
            f"Expected null for NOT pd.NA, got {result.iloc[0]!r}. "
            "pd.NA must be treated as null by kleene_not."
        )

    def test_mixed_series(self) -> None:
        """NOT on mixed True/False/None series returns the correct inversions."""
        from pycypher.boolean_evaluator import kleene_not

        s = pd.Series([True, False, None, True, None, False], dtype=object)
        result = kleene_not(s)
        assert result.iloc[0] is False or result.iloc[0] == False  # noqa: E712
        assert result.iloc[1] is True or result.iloc[1] == True  # noqa: E712
        assert pd.isna(result.iloc[2])
        assert result.iloc[3] is False or result.iloc[3] == False  # noqa: E712
        assert pd.isna(result.iloc[4])
        assert result.iloc[5] is True or result.iloc[5] == True  # noqa: E712

    def test_all_true_series(self) -> None:
        from pycypher.boolean_evaluator import kleene_not

        s = pd.Series([True, True, True], dtype=object)
        result = kleene_not(s)
        assert all(v is False or v == False for v in result)  # noqa: E712

    def test_all_false_series(self) -> None:
        from pycypher.boolean_evaluator import kleene_not

        s = pd.Series([False, False, False], dtype=object)
        result = kleene_not(s)
        assert all(v is True or v == True for v in result)  # noqa: E712

    def test_all_null_series(self) -> None:
        from pycypher.boolean_evaluator import kleene_not

        s = pd.Series([None, None, None], dtype=object)
        result = kleene_not(s)
        assert all(pd.isna(v) for v in result)

    def test_double_negation_roundtrips(self) -> None:
        """NOT NOT x == x for all three-valued inputs."""
        from pycypher.boolean_evaluator import kleene_not

        s = pd.Series([True, False, None], dtype=object)
        result = kleene_not(kleene_not(s))
        # True→False→True
        assert result.iloc[0] is True or result.iloc[0] == True  # noqa: E712
        # False→True→False
        assert result.iloc[1] is False or result.iloc[1] == False  # noqa: E712
        # None→None→None
        assert pd.isna(result.iloc[2])


# ===========================================================================
# Category 2 — Index preservation
# ===========================================================================


class TestKleeneNotIndexPreservation:
    """kleene_not must preserve the original Series index."""

    def test_preserves_non_default_index(self) -> None:
        from pycypher.boolean_evaluator import kleene_not

        idx = [10, 20, 30]
        s = pd.Series([True, False, None], index=idx, dtype=object)
        result = kleene_not(s)
        assert list(result.index) == idx, (
            f"kleene_not did not preserve index: expected {idx}, "
            f"got {list(result.index)}"
        )

    def test_output_dtype_is_object(self) -> None:
        """The result must be dtype=object (not bool) to hold None values."""
        from pycypher.boolean_evaluator import kleene_not

        s = pd.Series([True, False, None], dtype=object)
        result = kleene_not(s)
        assert result.dtype == object, (
            f"Expected dtype=object, got {result.dtype}. "
            "bool dtype cannot represent None."
        )


# ===========================================================================
# Category 3 — Performance: vectorised must beat .apply(lambda)
# ===========================================================================


class TestKleeneNotPerformance:
    """kleene_not must be measurably faster than the .apply(lambda) baseline."""

    REPS = 50
    N = 5_000

    @pytest.fixture()
    def large_bool_series(self) -> pd.Series:
        vals = [
            True if i % 3 == 0 else (None if i % 3 == 1 else False)
            for i in range(self.N)
        ]
        return pd.Series(vals, dtype=object)

    def test_kleene_not_faster_than_apply(
        self, large_bool_series: pd.Series
    ) -> None:
        """kleene_not(5 000 rows × 50 reps) must be faster than apply.

        Threshold is 1.05× to avoid flaky failures under CPU contention
        (e.g. parallel test runs, CI, multi-agent sessions).  Typical
        speedup is 2-4× on an idle machine.
        """
        from pycypher.boolean_evaluator import kleene_not

        s = large_bool_series

        # Baseline: reference Python-loop / apply implementation
        start = time.perf_counter()
        for _ in range(self.REPS):
            _reference_kleene_not(s)
        baseline = time.perf_counter() - start

        # Under test: vectorised implementation
        start = time.perf_counter()
        for _ in range(self.REPS):
            kleene_not(s)
        vectorised = time.perf_counter() - start

        speedup = baseline / vectorised if vectorised > 0 else float("inf")
        assert speedup >= 1.05, (
            f"kleene_not vectorised ({vectorised:.3f}s) should be ≥ 1.05× faster "
            f"than .apply(lambda) baseline ({baseline:.3f}s), got {speedup:.2f}×. "
            "The .apply() implementation is not yet replaced."
        )

    def test_kleene_not_absolute_threshold(
        self, large_bool_series: pd.Series
    ) -> None:
        """50 × kleene_not on 5 000 rows must complete in under 0.5s."""
        from pycypher.boolean_evaluator import kleene_not

        s = large_bool_series
        start = time.perf_counter()
        for _ in range(self.REPS):
            kleene_not(s)
        elapsed = time.perf_counter() - start
        assert elapsed < 0.5, (
            f"50 × kleene_not(5000-row) took {elapsed:.3f}s (threshold 0.5s). "
            "The .apply() implementation must be replaced with numpy operations."
        )


# ===========================================================================
# Category 4 — Integration: NOT in WHERE clause via execute_query
# ===========================================================================


class TestNotInWhereClause:
    """NOT must work correctly inside WHERE clauses via execute_query."""

    def test_not_filters_correctly(self) -> None:
        """WHERE NOT p.active returns only inactive people."""
        star = _star_with_people()
        result = star.execute_query(
            "MATCH (p:Person) WHERE NOT p.active RETURN p.name"
        )
        names = set(result["name"])
        assert "Bob" in names
        assert "Dan" in names
        assert "Alice" not in names
        assert "Carol" not in names

    def test_not_of_comparison(self) -> None:
        """WHERE NOT (p.age > 30) returns people 30 or younger."""
        star = _star_with_people()
        result = star.execute_query(
            "MATCH (p:Person) WHERE NOT (p.age > 30) RETURN p.name"
        )
        names = set(result["name"])
        assert "Alice" in names  # age=30: NOT (30 > 30) = NOT false = true
        assert "Bob" in names  # age=17: NOT (17 > 30) = NOT false = true
        assert "Carol" in names  # age=25: NOT (25 > 30) = NOT false = true
        assert "Dan" not in names  # age=40: NOT (40 > 30) = NOT true = false

    def test_not_and_combination(self) -> None:
        """WHERE NOT p.active AND p.age < 30."""
        star = _star_with_people()
        result = star.execute_query(
            "MATCH (p:Person) WHERE NOT p.active AND p.age < 30 RETURN p.name"
        )
        names = set(result["name"])
        # Bob: NOT active=False → True; age=17 < 30 → True; both → True
        assert "Bob" in names
        # Dan: NOT active=False → True; age=40 < 30 → False; → False
        assert "Dan" not in names

    def test_not_not_roundtrip(self) -> None:
        """WHERE NOT NOT p.active == WHERE p.active."""
        star = _star_with_people()
        with_double_not = star.execute_query(
            "MATCH (p:Person) WHERE NOT NOT p.active RETURN p.name ORDER BY p.name"
        )
        without_not = star.execute_query(
            "MATCH (p:Person) WHERE p.active RETURN p.name ORDER BY p.name"
        )
        assert list(with_double_not["name"]) == list(without_not["name"])

    def test_not_in_standalone_return(self) -> None:
        """NOT in a standalone RETURN expression."""
        result = _q("RETURN NOT true AS a, NOT false AS b")
        assert result["a"].iloc[0] == False  # noqa: E712
        assert result["b"].iloc[0] == True  # noqa: E712

    def test_not_null_is_null(self) -> None:
        """RETURN NOT null AS v — must return null, not True or False."""
        result = _q("RETURN NOT null AS v")
        assert pd.isna(result["v"].iloc[0]), (
            f"Expected null for NOT null, got {result['v'].iloc[0]!r}."
        )

    def test_not_with_in_operator(self) -> None:
        """WHERE NOT p.name IN ['Bob', 'Dan'] returns Alice and Carol."""
        star = _star_with_people()
        result = star.execute_query(
            "MATCH (p:Person) WHERE NOT p.name IN ['Bob', 'Dan'] RETURN p.name"
        )
        names = set(result["name"])
        assert "Alice" in names
        assert "Carol" in names
        assert "Bob" not in names
        assert "Dan" not in names
