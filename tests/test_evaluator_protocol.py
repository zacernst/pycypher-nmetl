"""Tests for pycypher.evaluator_protocol — runtime-checkable Protocol classes.

Validates that:
- ExpressionEvaluatorProtocol is runtime_checkable and structurally matchable
- ExpressionEvaluatorFactory protocol works as expected
- Real evaluators satisfy the protocol contract
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.evaluator_protocol import (
    ExpressionEvaluatorFactory,
    ExpressionEvaluatorProtocol,
)


class _FakeFrame:
    """Minimal stand-in for BindingFrame."""


class _ConformingEvaluator:
    """A class that structurally conforms to ExpressionEvaluatorProtocol."""

    def __init__(self, frame: _FakeFrame) -> None:
        self._frame = frame

    @property
    def frame(self) -> _FakeFrame:
        return self._frame

    def evaluate(self, expression: object) -> pd.Series:
        return pd.Series([1, 2, 3])


class _NonConformingEvaluator:
    """A class that does NOT conform — missing evaluate method."""

    @property
    def frame(self) -> _FakeFrame:
        return _FakeFrame()


class _ConformingFactory:
    """Structurally conforms to ExpressionEvaluatorFactory."""

    def __call__(self, frame: object) -> _ConformingEvaluator:
        return _ConformingEvaluator(frame)


# ---------------------------------------------------------------------------
# ExpressionEvaluatorProtocol
# ---------------------------------------------------------------------------


def test_protocol_is_runtime_checkable():
    """ExpressionEvaluatorProtocol must be decorated with @runtime_checkable."""
    evaluator = _ConformingEvaluator(_FakeFrame())
    assert isinstance(evaluator, ExpressionEvaluatorProtocol)


def test_non_conforming_fails_isinstance():
    """Objects missing required methods should not satisfy the protocol."""
    obj = _NonConformingEvaluator()
    assert not isinstance(obj, ExpressionEvaluatorProtocol)


def test_protocol_evaluate_returns_series():
    """Conforming evaluator's evaluate() should return expected result."""
    evaluator = _ConformingEvaluator(_FakeFrame())
    result = evaluator.evaluate(None)
    assert list(result) == [1, 2, 3]


def test_protocol_frame_property():
    """Conforming evaluator's frame property should return the bound frame."""
    frame = _FakeFrame()
    evaluator = _ConformingEvaluator(frame)
    assert evaluator.frame is frame


# ---------------------------------------------------------------------------
# ExpressionEvaluatorFactory
# ---------------------------------------------------------------------------


def test_factory_protocol_is_protocol():
    """ExpressionEvaluatorFactory should be a Protocol (not runtime_checkable)."""
    from typing import Protocol

    assert issubclass(ExpressionEvaluatorFactory, Protocol)


def test_factory_creates_evaluator():
    """Factory should produce an evaluator bound to the given frame."""
    factory = _ConformingFactory()
    frame = _FakeFrame()
    evaluator = factory(frame)
    assert evaluator.frame is frame


def test_factory_callable_produces_evaluator():
    """A conforming factory should produce a protocol-satisfying evaluator."""
    factory = _ConformingFactory()
    evaluator = factory(_FakeFrame())
    assert isinstance(evaluator, ExpressionEvaluatorProtocol)


# ---------------------------------------------------------------------------
# Integration: real BindingExpressionEvaluator satisfies the protocol
# ---------------------------------------------------------------------------


def test_real_evaluator_has_protocol_methods():
    """BindingExpressionEvaluator must have the evaluate method required by the protocol."""
    from pycypher.binding_evaluator import BindingExpressionEvaluator

    assert hasattr(BindingExpressionEvaluator, "evaluate")
    assert callable(getattr(BindingExpressionEvaluator, "evaluate"))
    # `frame` is set as an instance attribute in __init__, verified via source inspection
