"""TDD tests for BindingFrame._cleanup_merged() — Detail loop.

The three join methods (join, left_join, cross_join) all share an identical
post-merge cleanup block. This file tests the _cleanup_merged() static helper
that should be extracted so the logic lives in exactly one place.

Tests for _cleanup_merged() will FAIL (AttributeError) before the helper is
extracted — that is the intentional red phase.

Run with:
    uv run pytest tests/test_binding_frame_cleanup.py -v
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.binding_frame import BindingFrame
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ctx() -> Context:
    """Context with Person nodes and KNOWS edges."""
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        }
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 11, 12],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 1],
            RELATIONSHIP_TARGET_COLUMN: [2, 3, 3],
        }
    )
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=person_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
        ],
        source_col=RELATIONSHIP_SOURCE_COLUMN,
        target_col=RELATIONSHIP_TARGET_COLUMN,
        source_obj=knows_df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )


# ---------------------------------------------------------------------------
# Unit tests for _cleanup_merged() static method
# ---------------------------------------------------------------------------


class TestCleanupMergedStatic:
    """Direct tests for BindingFrame._cleanup_merged() — red before extraction."""

    def test_same_col_names_nothing_dropped(self) -> None:
        """When left_col == right_col, the key column is NOT dropped."""
        df = pd.DataFrame({"x": [1, 2], "y": [10, 20]})
        result = BindingFrame._cleanup_merged(df, left_col="x", right_col="x")
        assert list(result.columns) == ["x", "y"]

    def test_different_col_names_right_dropped(self) -> None:
        """When left_col != right_col, the right key column IS dropped."""
        df = pd.DataFrame({"a": [1, 2], "b": [1, 2], "val": [99, 88]})
        result = BindingFrame._cleanup_merged(df, left_col="a", right_col="b")
        assert "b" not in result.columns
        assert "a" in result.columns
        assert "val" in result.columns

    def test_right_absent_from_merged_no_error(self) -> None:
        """If the right col is already absent (pandas deduped it), no error."""
        df = pd.DataFrame({"a": [1, 2], "val": [99, 88]})
        # right_col 'b' not present — should not raise
        result = BindingFrame._cleanup_merged(df, left_col="a", right_col="b")
        assert list(result.columns) == ["a", "val"]

    def test_right_suffix_collision_dropped(self) -> None:
        """Columns ending in '_right' (pandas collision suffix) are dropped."""
        df = pd.DataFrame(
            {"x": [1, 2], "name": ["Alice", "Bob"], "name_right": ["X", "Y"]}
        )
        result = BindingFrame._cleanup_merged(df, left_col="x", right_col="x")
        assert "name_right" not in result.columns
        assert "name" in result.columns

    def test_multiple_right_suffix_cols_all_dropped(self) -> None:
        """All columns ending in '_right' are dropped, not just the first."""
        df = pd.DataFrame(
            {
                "x": [1],
                "a_right": ["r1"],
                "b_right": ["r2"],
                "c_right": ["r3"],
                "keep": [42],
            }
        )
        result = BindingFrame._cleanup_merged(df, left_col="x", right_col="x")
        for col in ("a_right", "b_right", "c_right"):
            assert col not in result.columns
        assert "keep" in result.columns

    def test_index_is_reset(self) -> None:
        """The returned DataFrame always has a clean 0-based RangeIndex."""
        df = pd.DataFrame({"x": [10, 20, 30]}, index=[5, 10, 15])
        result = BindingFrame._cleanup_merged(df, left_col="x", right_col="x")
        assert list(result.index) == [0, 1, 2]

    def test_no_cols_passed_only_right_suffix_cleanup(self) -> None:
        """Calling with left_col=None, right_col=None skips key drop entirely."""
        df = pd.DataFrame({"a": [1], "a_right": [2]})
        result = BindingFrame._cleanup_merged(df)
        assert "a_right" not in result.columns
        assert "a" in result.columns


# ---------------------------------------------------------------------------
# Integration regression tests — join / left_join / cross_join behaviour
# ---------------------------------------------------------------------------


class TestJoinCleanupIntegration:
    def test_join_different_keys_right_key_absent(self, ctx: Context) -> None:
        """After join(left_col='p', right_col='src'), 'src' is not in result."""
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"src": [1, 2, 1], "r": [10, 11, 12]}),
            type_registry={"r": "KNOWS"},
            context=ctx,
        )
        result = left.join(right, left_col="p", right_col="src")
        assert "src" not in result.bindings.columns
        assert "p" in result.bindings.columns

    def test_join_right_suffix_dropped(self, ctx: Context) -> None:
        """Pandas '_right' suffixed columns are absent after join."""
        left = BindingFrame(
            bindings=pd.DataFrame({"x": [1, 2], "val": ["a", "b"]}),
            type_registry={},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"x": [1, 2], "val": ["c", "d"]}),
            type_registry={},
            context=ctx,
        )
        result = left.join(right, left_col="x", right_col="x")
        assert "val_right" not in result.bindings.columns

    def test_join_index_clean(self, ctx: Context) -> None:
        """Result of join() always has a clean 0-based integer index."""
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"src": [1, 2, 1], "r": [10, 11, 12]}),
            type_registry={"r": "KNOWS"},
            context=ctx,
        )
        result = left.join(right, left_col="p", right_col="src")
        assert list(result.bindings.index) == list(range(len(result.bindings)))

    def test_left_join_preserves_unmatched_rows(self, ctx: Context) -> None:
        """left_join() keeps all rows from left, NaN for unmatched right cols."""
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        # only matches p=1
        right = BindingFrame(
            bindings=pd.DataFrame({"src": [1], "r": [10]}),
            type_registry={"r": "KNOWS"},
            context=ctx,
        )
        result = left.left_join(right, left_col="p", right_col="src")
        assert len(result.bindings) == 3  # all 3 persons preserved
        assert "src" not in result.bindings.columns  # right key dropped

    def test_left_join_right_suffix_dropped(self, ctx: Context) -> None:
        """_right cols are absent after left_join()."""
        left = BindingFrame(
            bindings=pd.DataFrame({"x": [1, 2], "tag": ["a", "b"]}),
            type_registry={},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"x": [1, 2], "tag": ["c", "d"]}),
            type_registry={},
            context=ctx,
        )
        result = left.left_join(right, left_col="x", right_col="x")
        assert "tag_right" not in result.bindings.columns

    def test_cross_join_right_suffix_dropped(self, ctx: Context) -> None:
        """_right cols are absent after cross_join() when column names collide."""
        left = BindingFrame(
            bindings=pd.DataFrame({"x": [1, 2], "shared": ["a", "b"]}),
            type_registry={},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"y": [10, 20], "shared": ["c", "d"]}),
            type_registry={},
            context=ctx,
        )
        result = left.cross_join(right)
        assert "shared_right" not in result.bindings.columns
        assert len(result.bindings) == 4  # 2 × 2

    def test_cross_join_index_clean(self, ctx: Context) -> None:
        """Result of cross_join() always has a clean 0-based integer index."""
        left = BindingFrame(
            bindings=pd.DataFrame({"x": [1, 2]}),
            type_registry={},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"y": [10, 20, 30]}),
            type_registry={},
            context=ctx,
        )
        result = left.cross_join(right)
        assert list(result.bindings.index) == list(range(len(result.bindings)))
