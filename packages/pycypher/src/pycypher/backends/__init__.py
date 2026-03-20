"""Backend implementations for the BackendEngine protocol.

Each module provides a concrete implementation of the
:class:`~pycypher.backend_engine.BackendEngine` protocol:

- :mod:`.pandas_backend` — Default pandas-based backend (zero-cost wrapper).
- :mod:`.duckdb_backend` — DuckDB OLAP engine for analytical workloads.
- :mod:`.polars_backend` — Polars Arrow-native backend for single-machine scaling.
"""

from __future__ import annotations

from pycypher.backends.duckdb_backend import DuckDBBackend
from pycypher.backends.pandas_backend import PandasBackend
from pycypher.backends.polars_backend import PolarsBackend

__all__ = [
    "DuckDBBackend",
    "PandasBackend",
    "PolarsBackend",
]
