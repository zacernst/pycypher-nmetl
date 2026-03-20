"""Shared helper functions for backend implementations."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

#: Pattern for valid SQL identifiers — alphanumeric plus underscores.
#: Used to prevent SQL injection in DuckDB backend operations.
IDENTIFIER_RE: re.Pattern[str] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(name: str) -> str:
    """Validate that *name* is a safe SQL identifier.

    Column and table names interpolated into DuckDB SQL must not contain
    characters that could alter query semantics.  This is a defence-in-depth
    measure on top of DuckDB's ``"quoted identifier"`` syntax.

    Args:
        name: The identifier to validate.

    Returns:
        *name* unchanged if valid.

    Raises:
        ValueError: If *name* contains characters outside ``[A-Za-z0-9_]``
            or does not start with a letter or underscore.

    """
    if not IDENTIFIER_RE.match(name):
        msg = (
            f"Invalid SQL identifier: {name!r}. "
            "Identifiers must match [A-Za-z_][A-Za-z0-9_]*."
        )
        raise ValueError(msg)
    return name


def _to_pandas(obj: Any) -> pd.DataFrame:
    """Convert *obj* to pandas DataFrame if it isn't already."""
    try:
        import pyarrow as pa

        if isinstance(obj, pa.Table):
            return obj.to_pandas()
    except ImportError:
        pass
    if isinstance(obj, pd.DataFrame):
        return obj
    from pycypher.exceptions import WrongCypherTypeError

    msg = f"Cannot convert {type(obj).__name__} to pandas DataFrame"
    raise WrongCypherTypeError(msg)


def _pandas_agg_to_sql(func: str) -> str:
    """Map pandas aggregation function names to SQL equivalents."""
    mapping: dict[str, str] = {
        "sum": "SUM",
        "count": "COUNT",
        "mean": "AVG",
        "min": "MIN",
        "max": "MAX",
        "std": "STDDEV_SAMP",
        "var": "VAR_SAMP",
        "first": "FIRST",
        "last": "LAST",
    }
    result = mapping.get(func)
    if result is None:
        msg = f"Unsupported aggregation function for SQL: {func!r}"
        raise ValueError(msg)
    return result


def _polars_agg_func(col_expr: Any, func: str) -> Any:
    """Map pandas aggregation function name to a Polars expression."""
    mapping: dict[str, str] = {
        "sum": "sum",
        "count": "count",
        "mean": "mean",
        "min": "min",
        "max": "max",
        "std": "std",
        "var": "var",
        "first": "first",
        "last": "last",
    }
    method_name = mapping.get(func)
    if method_name is None:
        msg = f"Unsupported aggregation function for Polars: {func!r}"
        raise ValueError(msg)
    return getattr(col_expr, method_name)()
