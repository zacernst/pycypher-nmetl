"""TDD tests for the _make_seed_frame() helper method refactoring.

Four separate places in ``star.py`` create an identical synthetic
single-row ``BindingFrame`` with ``{"_row": [0]}`` / ``{"_seed": [0]}`` /
``{"_dummy_": [None]}`` to act as a seed context for evaluating literal
expressions when no prior data frame exists.  The helper
``_make_seed_frame()`` centralises that construction and normalises the
column name to ``_row`` across all four call sites.

The tests verify the helper's contract rather than internal column names,
so they stay green even if the implementation details change.
"""

from __future__ import annotations

import pytest
from pycypher.binding_frame import BindingFrame
from pycypher.relational_models import Context, EntityMapping
from pycypher.star import Star


@pytest.fixture
def empty_ctx() -> Context:
    return Context(entity_mapping=EntityMapping(mapping={}))


class TestMakeSeedFrame:
    """Star._make_seed_frame() returns a valid single-row BindingFrame."""

    def test_returns_binding_frame(self, empty_ctx: Context) -> None:
        """_make_seed_frame() returns a BindingFrame instance."""
        star = Star(context=empty_ctx)
        frame = star._make_seed_frame()
        assert isinstance(frame, BindingFrame)

    def test_single_row(self, empty_ctx: Context) -> None:
        """Frame has exactly one row."""
        star = Star(context=empty_ctx)
        frame = star._make_seed_frame()
        assert len(frame.bindings) == 1

    def test_empty_type_registry(self, empty_ctx: Context) -> None:
        """Frame type_registry is empty (no entity bindings)."""
        star = Star(context=empty_ctx)
        frame = star._make_seed_frame()
        assert frame.type_registry == {}

    def test_context_preserved(self, empty_ctx: Context) -> None:
        """Frame's context is the Star's context."""
        star = Star(context=empty_ctx)
        frame = star._make_seed_frame()
        assert frame.context is empty_ctx

    def test_distinct_calls_produce_independent_frames(
        self,
        empty_ctx: Context,
    ) -> None:
        """Two calls return independent frames (not the same object)."""
        star = Star(context=empty_ctx)
        f1 = star._make_seed_frame()
        f2 = star._make_seed_frame()
        assert f1 is not f2
        assert f1.bindings is not f2.bindings
