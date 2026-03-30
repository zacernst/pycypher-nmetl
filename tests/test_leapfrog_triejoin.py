"""Tests for the LeapfrogTriejoin worst-case optimal join algorithm."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pycypher.binding_frame import BindingFrame
from pycypher.leapfrog_triejoin import (
    LeapfrogIterator,
    _leapfrog_intersect,
    can_use_leapfrog,
    leapfrog_triejoin,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_context():
    """Build a minimal Context for BindingFrame construction."""
    from pycypher.relational_models import Context, EntityMapping, RelationshipMapping

    return Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


def _bf(data: dict[str, list], type_registry: dict[str, str] | None = None) -> BindingFrame:
    """Shorthand to build a BindingFrame from column data."""
    return BindingFrame(
        bindings=pd.DataFrame(data),
        type_registry=type_registry or {},
        context=_make_context(),
    )


# ---------------------------------------------------------------------------
# LeapfrogIterator tests
# ---------------------------------------------------------------------------


class TestLeapfrogIterator:
    def test_basic_seek(self):
        values = np.array([1, 3, 5, 7, 9])
        indices = np.arange(5)
        it = LeapfrogIterator(values, indices)

        assert not it.at_end
        assert it.key == 1

        it.seek(5)
        assert it.key == 5

        it.seek(6)
        assert it.key == 7

    def test_seek_past_end(self):
        values = np.array([1, 2, 3])
        indices = np.arange(3)
        it = LeapfrogIterator(values, indices)

        it.seek(10)
        assert it.at_end

    def test_next(self):
        values = np.array([10, 20, 30])
        indices = np.arange(3)
        it = LeapfrogIterator(values, indices)

        assert it.key == 10
        it.next()
        assert it.key == 20
        it.next()
        assert it.key == 30
        it.next()
        assert it.at_end

    def test_get_rows_for_key_with_duplicates(self):
        values = np.array([1, 1, 1, 2, 2, 3])
        indices = np.array([0, 1, 2, 3, 4, 5])
        it = LeapfrogIterator(values, indices)

        rows = it.get_rows_for_key(1)
        np.testing.assert_array_equal(rows, [0, 1, 2])

    def test_empty_iterator(self):
        values = np.array([], dtype=int)
        indices = np.array([], dtype=int)
        it = LeapfrogIterator(values, indices)
        assert it.at_end

    def test_seek_already_at_target(self):
        values = np.array([5, 10, 15])
        indices = np.arange(3)
        it = LeapfrogIterator(values, indices)
        it.seek(5)
        assert it.key == 5  # Should stay at same position


# ---------------------------------------------------------------------------
# Leapfrog intersection tests
# ---------------------------------------------------------------------------


class TestLeapfrogIntersect:
    def test_basic_intersection(self):
        it1 = LeapfrogIterator(np.array([1, 2, 3, 4, 5]), np.arange(5))
        it2 = LeapfrogIterator(np.array([2, 4, 6, 8]), np.arange(4))
        it3 = LeapfrogIterator(np.array([1, 2, 4, 7]), np.arange(4))

        result = _leapfrog_intersect([it1, it2, it3])
        assert result == [2, 4]

    def test_no_intersection(self):
        it1 = LeapfrogIterator(np.array([1, 3, 5]), np.arange(3))
        it2 = LeapfrogIterator(np.array([2, 4, 6]), np.arange(3))

        result = _leapfrog_intersect([it1, it2])
        assert result == []

    def test_full_intersection(self):
        values = np.array([1, 2, 3])
        it1 = LeapfrogIterator(values.copy(), np.arange(3))
        it2 = LeapfrogIterator(values.copy(), np.arange(3))
        it3 = LeapfrogIterator(values.copy(), np.arange(3))

        result = _leapfrog_intersect([it1, it2, it3])
        assert result == [1, 2, 3]

    def test_empty_iterator_returns_empty(self):
        it1 = LeapfrogIterator(np.array([1, 2, 3]), np.arange(3))
        it2 = LeapfrogIterator(np.array([], dtype=int), np.array([], dtype=int))

        result = _leapfrog_intersect([it1, it2])
        assert result == []

    def test_single_element_intersection(self):
        it1 = LeapfrogIterator(np.array([5]), np.array([0]))
        it2 = LeapfrogIterator(np.array([5]), np.array([0]))
        it3 = LeapfrogIterator(np.array([5]), np.array([0]))

        result = _leapfrog_intersect([it1, it2, it3])
        assert result == [5]

    def test_no_iterators(self):
        result = _leapfrog_intersect([])
        assert result == []


# ---------------------------------------------------------------------------
# Full LeapfrogTriejoin tests
# ---------------------------------------------------------------------------


class TestLeapfrogTriejoin:
    def test_three_way_join(self):
        """Basic 3-way join: frames share variable 'x'."""
        f1 = _bf({"x": [1, 2, 3], "a": ["a1", "a2", "a3"]})
        f2 = _bf({"x": [2, 3, 4], "b": ["b2", "b3", "b4"]})
        f3 = _bf({"x": [1, 3, 5], "c": ["c1", "c3", "c5"]})

        result = leapfrog_triejoin([f1, f2, f3], "x")

        assert len(result.bindings) == 1  # Only x=3 is in all three
        assert set(result.bindings.columns) == {"x", "a", "b", "c"}
        row = result.bindings.iloc[0]
        assert row["x"] == 3
        assert row["a"] == "a3"
        assert row["b"] == "b3"
        assert row["c"] == "c3"

    def test_two_way_join(self):
        """2-way join should also work."""
        f1 = _bf({"x": [1, 2, 3], "a": [10, 20, 30]})
        f2 = _bf({"x": [2, 3, 4], "b": [200, 300, 400]})

        result = leapfrog_triejoin([f1, f2], "x")

        assert len(result.bindings) == 2
        xs = sorted(result.bindings["x"].tolist())
        assert xs == [2, 3]

    def test_empty_intersection(self):
        """When no keys are shared, result is empty."""
        f1 = _bf({"x": [1, 2], "a": [10, 20]})
        f2 = _bf({"x": [3, 4], "b": [30, 40]})
        f3 = _bf({"x": [5, 6], "c": [50, 60]})

        result = leapfrog_triejoin([f1, f2, f3], "x")

        assert len(result.bindings) == 0

    def test_duplicate_keys_cross_product(self):
        """Duplicate keys in different frames produce cross-products per key."""
        f1 = _bf({"x": [1, 1], "a": ["a1", "a2"]})
        f2 = _bf({"x": [1, 1], "b": ["b1", "b2"]})

        result = leapfrog_triejoin([f1, f2], "x")

        # 2 rows from f1 × 2 rows from f2 for key=1 → 4 rows
        assert len(result.bindings) == 4
        assert all(result.bindings["x"] == 1)

    def test_type_registry_merge(self):
        """Type registries from all frames are merged."""
        f1 = _bf({"x": [1, 2]}, {"x": "Person"})
        f2 = _bf({"x": [1, 2], "y": [10, 20]}, {"y": "Age"})
        f3 = _bf({"x": [1, 2], "z": [100, 200]}, {"z": "Score"})

        result = leapfrog_triejoin([f1, f2, f3], "x")

        assert result.type_registry["x"] == "Person"
        assert result.type_registry["y"] == "Age"
        assert result.type_registry["z"] == "Score"

    def test_error_on_single_frame(self):
        """Should raise ValueError with fewer than 2 frames."""
        f1 = _bf({"x": [1, 2]})
        with pytest.raises(ValueError, match="at least 2 frames"):
            leapfrog_triejoin([f1], "x")

    def test_error_on_missing_join_var(self):
        """Should raise ValueError if join var is missing from a frame."""
        f1 = _bf({"x": [1, 2]})
        f2 = _bf({"y": [1, 2]})  # no 'x' column
        with pytest.raises(ValueError, match="does not contain join variable"):
            leapfrog_triejoin([f1, f2], "x")

    def test_triangle_pattern(self):
        """Simulate a triangle query (a)->(b)->(c)->(a).

        Three relationship tables each with source/target columns.
        The leapfrog join finds common node IDs across all three.
        """
        # R1: (a)->(b) where a and b are nodes
        # R2: (b)->(c)
        # R3: (c)->(a)
        # Shared variable: node IDs that participate in triangles

        # Nodes: 1, 2, 3, 4, 5
        # Edges: 1->2, 2->3, 3->1 (triangle), 4->5
        r1 = _bf({"src": [1, 4], "tgt": [2, 5]})   # a->b edges
        r2 = _bf({"src": [2], "tgt": [3]})           # b->c edges
        r3 = _bf({"src": [3], "tgt": [1]})           # c->a edges

        # To find triangles, we'd typically join on:
        # r1.tgt = r2.src (b), r2.tgt = r3.src (c), r3.tgt = r1.src (a)
        # For the leapfrog test, join r1 and r2 on shared var 'tgt'/'src'
        # Here we simplify: join frames that share the 'b' node
        f1 = _bf({"b": [2, 5]})         # b values from r1.tgt
        f2 = _bf({"b": [2]})            # b values from r2.src
        f3 = _bf({"b": [2, 7, 8]})      # b values from some other relation

        result = leapfrog_triejoin([f1, f2, f3], "b")
        assert len(result.bindings) == 1
        assert result.bindings["b"].iloc[0] == 2

    def test_large_intersection(self):
        """Performance test with larger data."""
        n = 10000
        # Disjoint ranges ensure only shared prefix overlaps
        shared = list(range(1000))
        f1_data = shared + list(range(100000, 100000 + n))
        f2_data = shared + list(range(200000, 200000 + n))
        f3_data = shared + list(range(300000, 300000 + n))

        f1 = _bf({"x": f1_data, "a": list(range(len(f1_data)))})
        f2 = _bf({"x": f2_data, "b": list(range(len(f2_data)))})
        f3 = _bf({"x": f3_data, "c": list(range(len(f3_data)))})

        result = leapfrog_triejoin([f1, f2, f3], "x")
        assert len(result.bindings) == 1000

    def test_string_keys(self):
        """Join on string-typed keys."""
        f1 = _bf({"name": ["alice", "bob", "charlie"], "age": [30, 25, 35]})
        f2 = _bf({"name": ["bob", "charlie", "dave"], "city": ["NY", "LA", "SF"]})
        f3 = _bf({"name": ["charlie", "eve", "bob"], "score": [90, 85, 70]})

        result = leapfrog_triejoin([f1, f2, f3], "name")

        assert len(result.bindings) == 2  # bob and charlie
        names = sorted(result.bindings["name"].tolist())
        assert names == ["bob", "charlie"]

    def test_four_way_join(self):
        """4-way join on shared variable."""
        f1 = _bf({"x": [1, 2, 3, 4, 5]})
        f2 = _bf({"x": [2, 3, 4, 5, 6]})
        f3 = _bf({"x": [3, 4, 5, 6, 7]})
        f4 = _bf({"x": [4, 5, 6, 7, 8]})

        result = leapfrog_triejoin([f1, f2, f3, f4], "x")
        assert sorted(result.bindings["x"].tolist()) == [4, 5]

    def test_preserves_all_columns(self):
        """All non-join columns from every frame appear in the result."""
        f1 = _bf({"x": [1, 2], "a1": [10, 20], "a2": [100, 200]})
        f2 = _bf({"x": [1, 2], "b1": [30, 40]})
        f3 = _bf({"x": [1, 2], "c1": [50, 60], "c2": [500, 600], "c3": [5000, 6000]})

        result = leapfrog_triejoin([f1, f2, f3], "x")

        expected_cols = {"x", "a1", "a2", "b1", "c1", "c2", "c3"}
        assert set(result.bindings.columns) == expected_cols
        assert len(result.bindings) == 2


# ---------------------------------------------------------------------------
# can_use_leapfrog tests
# ---------------------------------------------------------------------------


class TestCanUseLeapfrog:
    def test_three_frames_shared_var(self):
        f1 = _bf({"x": [1, 2], "a": [10, 20]})
        f2 = _bf({"x": [1, 2], "b": [30, 40]})
        f3 = _bf({"x": [1, 2], "c": [50, 60]})

        applicable, var = can_use_leapfrog([f1, f2, f3])
        assert applicable is True
        assert var == "x"

    def test_two_frames_not_applicable(self):
        f1 = _bf({"x": [1, 2]})
        f2 = _bf({"x": [1, 2]})

        applicable, var = can_use_leapfrog([f1, f2])
        assert applicable is False
        assert var is None

    def test_no_shared_variable(self):
        f1 = _bf({"a": [1, 2]})
        f2 = _bf({"b": [1, 2]})
        f3 = _bf({"c": [1, 2]})

        applicable, var = can_use_leapfrog([f1, f2, f3])
        assert applicable is False
        assert var is None

    def test_picks_lowest_cardinality_var(self):
        """When multiple shared vars exist, pick the one with lowest cardinality."""
        f1 = _bf({"x": [1, 1, 1, 2, 2, 3], "y": [1, 2, 3, 4, 5, 6]})
        f2 = _bf({"x": [1, 2, 3, 3, 3, 3], "y": [1, 2, 3, 4, 5, 6]})
        f3 = _bf({"x": [1, 2, 2, 2, 3, 3], "y": [1, 2, 3, 4, 5, 6]})

        applicable, var = can_use_leapfrog([f1, f2, f3])
        assert applicable is True
        assert var == "x"  # x has 3 unique values vs y with 6


# ---------------------------------------------------------------------------
# FrameJoiner.multi_way_join integration tests
# ---------------------------------------------------------------------------


class TestFrameJoinerMultiWayJoin:
    def _make_joiner(self):
        from pycypher.frame_joiner import FrameJoiner

        ctx = _make_context()
        return FrameJoiner(
            context=ctx,
            match_fn=lambda *a, **kw: None,
            where_fn=lambda *a, **kw: None,
        )

    def test_multi_way_uses_leapfrog(self):
        """3+ frames with shared var should use leapfrog path."""
        joiner = self._make_joiner()
        f1 = _bf({"x": [1, 2, 3], "a": [10, 20, 30]})
        f2 = _bf({"x": [2, 3, 4], "b": [20, 30, 40]})
        f3 = _bf({"x": [3, 4, 5], "c": [30, 40, 50]})

        result = joiner.multi_way_join([f1, f2, f3])

        assert len(result.bindings) == 1
        assert result.bindings["x"].iloc[0] == 3

    def test_multi_way_fallback_pairwise(self):
        """3 frames without shared var fall back to pairwise joins."""
        joiner = self._make_joiner()
        f1 = _bf({"a": [1, 2]})
        f2 = _bf({"b": [3, 4]})
        f3 = _bf({"c": [5, 6]})

        result = joiner.multi_way_join([f1, f2, f3])

        # Cross joins: 2 × 2 × 2 = 8 rows
        assert len(result.bindings) == 8

    def test_two_frames_pairwise(self):
        """2 frames always use pairwise join."""
        joiner = self._make_joiner()
        f1 = _bf({"x": [1, 2, 3], "a": [10, 20, 30]})
        f2 = _bf({"x": [2, 3, 4], "b": [20, 30, 40]})

        result = joiner.multi_way_join([f1, f2])

        assert len(result.bindings) == 2
        assert sorted(result.bindings["x"].tolist()) == [2, 3]

    def test_single_frame(self):
        """Single frame is returned as-is."""
        joiner = self._make_joiner()
        f1 = _bf({"x": [1, 2, 3]})

        result = joiner.multi_way_join([f1])

        assert len(result.bindings) == 3

    def test_empty_frames_list(self):
        """Empty frames list returns seed frame."""
        joiner = self._make_joiner()
        result = joiner.multi_way_join([])
        assert len(result.bindings) == 1  # seed frame has one row
