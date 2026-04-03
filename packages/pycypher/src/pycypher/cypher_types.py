"""Backend-agnostic type aliases for the pycypher query engine.

This module defines type aliases that decouple the evaluator stack from
concrete pandas types.  Today they resolve to pandas types (zero overhead);
tomorrow they can be swapped to protocol-based abstractions that support
Dask, Polars, or DuckDB backends.

Usage::

    from pycypher.cypher_types import FrameSeries, FrameDataFrame

    def evaluate(self, expr: Expression) -> FrameSeries:
        ...

The aliases live here — not in ``backend_engine.py`` — to avoid circular
imports (evaluators import types; backend_engine imports relational_models
which evaluators also need).
"""

from __future__ import annotations

from typing import Any

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

# ---------------------------------------------------------------------------
# Backend-agnostic frame type
# ---------------------------------------------------------------------------

BackendFrame = Any
"""Opaque handle to a backend-specific DataFrame.

Used in the ``BackendEngine`` protocol and ``InstrumentedBackend`` wrapper
to annotate frame parameters.  Each concrete backend returns its own type
(``pd.DataFrame`` for pandas, ``DuckDBLazyFrame`` for DuckDB, etc.) but
callers of the protocol treat frames as opaque handles — they are only
passed back into backend methods or materialised via ``to_pandas()``.

Today: ``Any`` (since the protocol is heterogeneous across backends).
Future: a ``TypeVar`` bound to a ``Frame`` protocol once all backends
conform to a structural frame interface.
"""

BackendMask = Any
"""Opaque handle to a backend-specific boolean mask.

Used in ``BackendEngine.filter()`` to annotate the mask parameter.
Today ``pd.Series[bool]`` for pandas, but other backends may use their
own mask representations.
"""

ColumnValues = Any
"""Values for a new or replaced column in a backend frame.

Passed to ``BackendEngine.assign_column()``.  May be a Series, list,
scalar, or backend-specific column representation.
"""

SourceObject = Any
"""Raw data source passed to ``BackendEngine.scan_entity()``.

Typically a ``pd.DataFrame``, ``pa.Table``, or file path, depending on
ingestion context.
"""
