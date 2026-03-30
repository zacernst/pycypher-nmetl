"""Tests for backend delegation in BindingFrame.

Verifies that BindingFrame.join(), filter(), cross_join(), left_join(),
and rename() delegate to context.backend when available, rather than
calling raw pandas operations directly.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pycypher.backends.pandas_backend import PandasBackend
from pycypher.binding_frame import BindingFrame
from pycypher.constants import ID_COLUMN


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class RecordingBackend(PandasBackend):
    """PandasBackend subclass that records which methods were called."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[str] = []

    def filter(self, frame, mask):
        self.calls.append("filter")
        return super().filter(frame, mask)

    def join(self, left, right, on, how="inner", strategy="auto"):
        self.calls.append(f"join:{how}")
        return super().join(left, right, on=on, how=how, strategy=strategy)

    def rename(self, frame, columns):
        self.calls.append("rename")
        return super().rename(frame, columns)

    def concat(self, frames, *, ignore_index=True):
        self.calls.append("concat")
        return super().concat(frames, ignore_index=ignore_index)

    def distinct(self, frame):
        self.calls.append("distinct")
        return super().distinct(frame)


class FakeContext:
    """Minimal context stub for testing backend delegation."""

    def __init__(self, backend=None):
        self._backend = backend or PandasBackend()
        self._shadow = {}
        self._shadow_rels = {}
        self._property_lookup_cache = {}

        # Minimal entity/relationship mapping stubs
        self.entity_mapping = type("M", (), {"mapping": {}})()
        self.relationship_mapping = type("M", (), {"mapping": {}})()

    @property
    def backend(self):
        return self._backend


def _make_bf(bindings, type_registry=None, backend=None):
    """Helper to create a BindingFrame with a fake context."""
    ctx = FakeContext(backend=backend)
    return BindingFrame(
        bindings=bindings,
        type_registry=type_registry or {},
        context=ctx,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFilterDelegation:
    """Verify BindingFrame.filter() routes through context.backend."""

    def test_filter_uses_backend(self):
        backend = RecordingBackend()
        bf = _make_bf(
            pd.DataFrame({"p": [1, 2, 3]}),
            backend=backend,
        )
        mask = pd.Series([True, False, True])
        result = bf.filter(mask)
        assert "filter" in backend.calls
        assert len(result) == 2
        assert list(result.bindings["p"]) == [1, 3]

    def test_filter_without_backend_uses_pandas(self):
        """When backend is None, falls back to raw pandas."""

        class NoBackendContext(FakeContext):
            """Context subclass with no backend."""

            @property
            def backend(self):
                return None

        ctx = NoBackendContext()
        bf = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={},
            context=ctx,
        )
        mask = pd.Series([True, False, True])
        result = bf.filter(mask)
        assert len(result) == 2


class TestJoinDelegation:
    """Verify BindingFrame.join() routes through context.backend."""

    def test_inner_join_uses_backend(self):
        backend = RecordingBackend()
        left = _make_bf(
            pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            backend=backend,
        )
        right = _make_bf(
            pd.DataFrame({"p": [2, 3, 4], "q": [10, 20, 30]}),
            type_registry={"q": "Company"},
            backend=backend,
        )
        result = left.join(right, "p", "p")
        assert "join:inner" in backend.calls
        assert len(result) == 2
        assert set(result.bindings["p"]) == {2, 3}

    def test_join_different_columns_uses_backend(self):
        """When left_col != right_col, backend.rename is called first."""
        backend = RecordingBackend()
        left = _make_bf(
            pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            backend=backend,
        )
        right = _make_bf(
            pd.DataFrame({"src": [2, 3, 4], "q": [10, 20, 30]}),
            type_registry={"q": "Company"},
            backend=backend,
        )
        result = left.join(right, "p", "src")
        assert "rename" in backend.calls
        assert "join:inner" in backend.calls
        assert len(result) == 2


class TestLeftJoinDelegation:
    """Verify BindingFrame.left_join() routes through context.backend."""

    def test_left_join_uses_backend(self):
        backend = RecordingBackend()
        left = _make_bf(
            pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            backend=backend,
        )
        right = _make_bf(
            pd.DataFrame({"p": [2], "q": [10]}),
            type_registry={"q": "Company"},
            backend=backend,
        )
        result = left.left_join(right, "p", "p")
        assert "join:left" in backend.calls
        assert len(result) == 3  # All left rows preserved


class TestCrossJoinDelegation:
    """Verify BindingFrame.cross_join() routes through context.backend."""

    def test_cross_join_uses_backend(self):
        backend = RecordingBackend()
        left = _make_bf(
            pd.DataFrame({"p": [1, 2]}),
            backend=backend,
        )
        right = _make_bf(
            pd.DataFrame({"q": [10, 20]}),
            backend=backend,
        )
        result = left.cross_join(right)
        assert "join:cross" in backend.calls
        assert len(result) == 4  # 2 x 2

    def test_cross_join_handles_column_collisions(self):
        """When both frames have same column, right side gets _right suffix."""
        backend = RecordingBackend()
        left = _make_bf(
            pd.DataFrame({"p": [1, 2]}),
            backend=backend,
        )
        right = _make_bf(
            pd.DataFrame({"p": [10, 20]}),
            backend=backend,
        )
        result = left.cross_join(right)
        assert "rename" in backend.calls
        assert "join:cross" in backend.calls


class TestRenameDelegation:
    """Verify BindingFrame.rename() routes through context.backend."""

    def test_rename_uses_backend(self):
        backend = RecordingBackend()
        bf = _make_bf(
            pd.DataFrame({"old_name": [1, 2, 3]}),
            type_registry={"old_name": "Person"},
            backend=backend,
        )
        result = bf.rename("old_name", "new_name")
        assert "rename" in backend.calls
        assert "new_name" in result.bindings.columns
        assert "old_name" not in result.bindings.columns


class TestEndToEndWithDefaultBackend:
    """Verify that operations work correctly with the default PandasBackend.

    These tests ensure backward compatibility — the PandasBackend delegation
    produces identical results to the old raw-pandas path.
    """

    def test_join_produces_correct_results(self):
        bf_left = _make_bf(
            pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
        )
        bf_right = _make_bf(
            pd.DataFrame({"p": [2, 3, 4], "q": [10, 20, 30]}),
            type_registry={"q": "Company"},
        )
        result = bf_left.join(bf_right, "p", "p")
        assert set(result.bindings["p"]) == {2, 3}
        assert set(result.bindings["q"]) == {10, 20}

    def test_asymmetric_join_produces_correct_results(self):
        bf_left = _make_bf(
            pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
        )
        bf_right = _make_bf(
            pd.DataFrame({"src": [1, 2], "r": [100, 200]}),
            type_registry={"r": "KNOWS"},
        )
        result = bf_left.join(bf_right, "p", "src")
        assert len(result) == 2
        assert "p" in result.bindings.columns
        assert "r" in result.bindings.columns

    def test_filter_produces_correct_results(self):
        bf = _make_bf(pd.DataFrame({"p": [1, 2, 3, 4, 5]}))
        mask = pd.Series([True, False, True, False, True])
        result = bf.filter(mask)
        assert list(result.bindings["p"]) == [1, 3, 5]

    def test_cross_join_produces_correct_results(self):
        bf_left = _make_bf(pd.DataFrame({"p": [1, 2]}))
        bf_right = _make_bf(pd.DataFrame({"q": [10, 20]}))
        result = bf_left.cross_join(bf_right)
        assert len(result) == 4

    def test_left_join_preserves_all_left_rows(self):
        bf_left = _make_bf(
            pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
        )
        bf_right = _make_bf(
            pd.DataFrame({"p": [2], "q": [10]}),
            type_registry={"q": "Company"},
        )
        result = bf_left.left_join(bf_right, "p", "p")
        assert len(result) == 3
        assert list(result.bindings["p"]) == [1, 2, 3]
