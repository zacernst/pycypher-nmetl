"""Lightweight DataFrame conversion utilities.

This module provides helper functions for converting between DataFrame-like
objects (pandas, PyArrow).  It is intentionally dependency-free with respect
to other ``pycypher`` modules so that it can be imported from any module
without introducing circular dependencies.

Extracted from ``binding_frame.py`` to break the circular import between
``binding_frame`` and ``binding_evaluator`` / ``scalar_function_evaluator``
/ ``mutation_engine``, all of which need ``_source_to_pandas`` but previously
had to use deferred local imports to avoid import cycles.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

# Cache the PyArrow Table type at module level for isinstance() checks
# to avoid repeated try/except in the hot path (_source_to_pandas).
try:
    import pyarrow as _pa

    _PYARROW_TABLE_TYPE: type | None = _pa.Table
except ImportError:
    _PYARROW_TABLE_TYPE = None


def source_to_pandas(obj: Any) -> pd.DataFrame:
    """Convert *obj* to a pandas DataFrame.

    If PyArrow is installed and *obj* is a ``pyarrow.Table``, it is converted
    via ``.to_pandas()``.  Otherwise *obj* is assumed to already be a
    ``pd.DataFrame`` and is returned unchanged.

    Args:
        obj: A ``pd.DataFrame`` or ``pyarrow.Table``.

    Returns:
        A ``pd.DataFrame``.

    """
    if _PYARROW_TABLE_TYPE is not None and isinstance(
        obj,
        _PYARROW_TABLE_TYPE,
    ):
        return obj.to_pandas()
    return obj
