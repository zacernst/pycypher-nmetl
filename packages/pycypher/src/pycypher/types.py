"""Backend-agnostic type aliases for the pycypher query engine.

This module defines type aliases that decouple the evaluator stack from
concrete pandas types.  Today they resolve to pandas types (zero overhead);
tomorrow they can be swapped to protocol-based abstractions that support
Dask, Polars, or DuckDB backends.

Usage::

    from pycypher.types import FrameSeries, FrameDataFrame

    def evaluate(self, expr: Expression) -> FrameSeries:
        ...

The aliases live here — not in ``backend_engine.py`` — to avoid circular
imports (evaluators import types; backend_engine imports relational_models
which evaluators also need).
"""

from __future__ import annotations

import pandas as pd

# ---------------------------------------------------------------------------
# Type aliases — Phase 1: literal pandas types (zero runtime overhead)
#
# Phase 2 plan: replace with typing.Protocol-based abstractions that
# accept pd.Series | dask.dataframe.Series | pl.Series transparently.
# ---------------------------------------------------------------------------

FrameSeries = pd.Series
"""A 1-D array of values aligned with a BindingFrame.

Today: literally ``pd.Series``.
Future: a protocol that pd.Series, dask Series, and Polars Series satisfy.
"""

FrameDataFrame = pd.DataFrame
"""A 2-D table of bindings or results.

Today: literally ``pd.DataFrame``.
Future: a protocol that pd.DataFrame, dask DataFrame, and Polars DataFrame
satisfy.
"""
