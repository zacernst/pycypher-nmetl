"""Category registration module for scalar functions."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from shared.helpers import is_null_value

from pycypher.constants import _init_null_result

if TYPE_CHECKING:
    from pycypher.scalar_functions import ScalarFunctionRegistry

_is_null = is_null_value


def register(registry: ScalarFunctionRegistry) -> None:
    """Register type conversion functions.

    Cypher usage::

        MATCH (p:Person) RETURN toString(p.age) AS age_str
        MATCH (p:Person) RETURN toInteger(p.salary) AS salary_int
        MATCH (p:Person) RETURN toFloat(p.score) AS score_float
        MATCH (p:Person) RETURN toBoolean('true') AS flag

    Null handling: invalid conversions return null (not an error).
    For strict conversions that raise on invalid input, use the
    base variants (toString, toInteger).  For null-returning variants,
    use the ``OrNull`` suffixed functions (toStringOrNull, etc.).
    """

    # toString(value) -> str
    def _to_string(s: pd.Series) -> pd.Series:
        """Convert value to string, preserving nulls.

        Handles pandas float upcasting: when a Series contains integers
        mixed with None, pandas upcasts to float64 (e.g., 42 -> 42.0).
        Detects this at the Series level: if the Series has float dtype,
        contains nulls, and ALL non-null values are integer-valued, it
        was likely an integer column upcasted by pandas.  In that case,
        integer-valued floats are rendered without decimal (42.0 -> "42").

        Args:
            s: Series to convert

        Returns:
            String series with nulls preserved as None

        """
        # Detect pandas null-upcasted integer columns:
        # float64 dtype + has nulls + every non-null value is integer-valued.
        # After null normalisation, property lookup returns object dtype with
        # Python float values (e.g. [30.0, 40.0, None, 35.0]) instead of
        # float64 dtype with NaN — so we also check for object dtype where
        # all non-null values are float instances with no fractional part.
        _is_upcasted_int = False
        if s.isna().any():
            non_null = s.dropna()
            if len(non_null) > 0:
                if s.dtype == np.float64:
                    # Classic pandas upcast path (integer col + None → float64)
                    _is_upcasted_int = bool(
                        (non_null == np.floor(non_null)).all()
                        and np.isfinite(non_null).all(),
                    )
                elif s.dtype == object:
                    # Post-null-normalisation path: check that every non-null
                    # value is a Python float whose fractional part is zero.
                    numeric = pd.to_numeric(non_null, errors="coerce")
                    if not numeric.isna().any():
                        _is_upcasted_int = bool(
                            (numeric == np.floor(numeric)).all()
                            and np.isfinite(numeric).all()
                            and all(isinstance(v, float) for v in non_null),
                        )

        # Fast path for bool dtype: s.map() uses a Cython dict-lookup path
        # that is ~10× faster than per-element Python dispatch.  This also
        # guarantees openCypher lowercase ('true'/'false') regardless of how
        # the Series was constructed, which str(True) would get wrong.
        if pd.api.types.is_bool_dtype(s):
            return s.map({True: "true", False: "false"})

        # All other dtypes: explicit Python loop over the object-cast array.
        # pandas.astype(str) on int64/float64 is NOT a C-level operation —
        # it calls Python str() per element and is as slow or slower than
        # Series.apply.  Explicit loop avoids pd.Series.__init__ overhead
        # and always emits Python None (not np.nan) for null positions.
        arr = s.to_numpy(dtype=object)
        out: list[object] = []
        for x in arr:
            if (
                x is None
                or x is pd.NA
                or (isinstance(x, float) and math.isnan(x))
            ):
                out.append(None)
            elif isinstance(x, bool):
                # openCypher: toString(true) → 'true', toString(false) → 'false'
                out.append("true" if x else "false")
            elif _is_upcasted_int and isinstance(x, float):
                out.append(str(int(x)))
            else:
                out.append(str(x))
        # dtype=object preserves Python None; pandas 3.x would otherwise
        # infer StringDtype for all-string lists and coerce None to pd.NA.
        return pd.Series(out, index=s.index, dtype=object)

    registry.register_function(
        name="toString",
        callable=_to_string,
        min_args=1,
        max_args=1,
        description="Convert value to string",
        example="toString(42) → '42'",
    )

    # toInteger(value) -> int
    def _to_integer(s: pd.Series) -> pd.Series:
        """Convert to integer via truncation, handling nulls and errors.

        Cypher's toInteger() truncates toward zero (like C/Java int cast):
        - toInteger(3.14)  -> 3
        - toInteger(2.99)  -> 2
        - toInteger(-1.7)  -> -1
        - toInteger("abc") -> null

        Args:
            s: Series to convert

        Returns:
            Integer series (Int64 supports nulls), invalid values become null

        """
        # Convert to numeric first (handles strings like "3.14" -> 3.14)
        numeric = pd.to_numeric(s, errors="coerce")
        # Truncate toward zero (not round) per Cypher semantics
        truncated = pd.Series(np.fix(numeric), index=numeric.index)
        return truncated.astype("Int64")

    registry.register_function(
        name="toInteger",
        callable=_to_integer,
        min_args=1,
        max_args=1,
        description="Convert value to integer (nulls on error)",
        example="toInteger('42') → 42, toInteger('abc') → null",
    )

    # toFloat(value) -> float
    def _to_float(s: pd.Series) -> pd.Series:
        """Convert to float, handling nulls and errors.

        Always returns a float-dtype Series — integer inputs are cast to
        float to match Neo4j semantics (toFloat(30) → 30.0, not 30).

        Args:
            s: Series to convert

        Returns:
            Float series (float64 dtype), invalid values become null

        """
        numeric = pd.to_numeric(s, errors="coerce")
        return numeric.astype("float64")

    registry.register_function(
        name="toFloat",
        callable=_to_float,
        min_args=1,
        max_args=1,
        description="Convert value to float (nulls on error)",
        example="toFloat('3.14') → 3.14, toFloat('abc') → null",
    )

    # toBoolean(value) -> bool
    def _to_boolean(s: pd.Series) -> pd.Series:
        """Convert to boolean following Cypher rules.

        Cypher boolean conversion:
        - Strings: 'true'/'TRUE' → True, 'false'/'FALSE' → False
        - Numbers: 0 → False, non-zero → True
        - Other values: null

        Args:
            s: Series to convert

        Returns:
            Boolean series with nulls for invalid values

        """
        # Convert to string and normalize
        str_series = s.astype(str).str.lower()

        # Map string values to booleans
        bool_map = {"true": True, "false": False, "1": True, "0": False}
        result = str_series.map(bool_map)

        # Preserve original nulls
        result[s.isna()] = None

        return result

    registry.register_function(
        name="toBoolean",
        callable=_to_boolean,
        min_args=1,
        max_args=1,
        description="Convert value to boolean",
        example="toBoolean('true') → true, toBoolean('false') → false",
    )

    # ------------------------------------------------------------------
    # toStringOrNull(value) -> str | null
    # ------------------------------------------------------------------
    def _to_string_or_null(s: pd.Series) -> pd.Series:
        """Convert values to strings, returning null for null inputs.

        Unlike ``toString()``, this function never raises — values that
        cannot be converted (e.g. ``null`` / ``NaN``) are returned as
        ``None`` rather than raising :exc:`ValueError`.

        Args:
            s: Input series.

        Returns:
            Series of string values or ``None`` for null inputs.

        """
        # Explicit Python loop — same semantics as the former Series.apply
        # path but avoids pd.Series.__init__ overhead and preserves None.
        arr = s.to_numpy(dtype=object)
        out: list[object] = []
        for val in arr:
            if (
                val is None
                or val is pd.NA
                or (isinstance(val, float) and math.isnan(val))
            ):
                out.append(None)
            else:
                out.append(str(val))
        # dtype=object preserves Python None; pandas 3.x StringDtype
        # inference would otherwise coerce None to pd.NA.
        return pd.Series(out, index=s.index, dtype=object)

    registry.register_function(
        name="toStringOrNull",
        callable=_to_string_or_null,
        min_args=1,
        max_args=1,
        description="Convert value to string, returning null for null input",
        example="toStringOrNull(42) → '42', toStringOrNull(null) → null",
    )

    # ------------------------------------------------------------------
    # toBooleanOrNull(value) -> bool | null
    # ------------------------------------------------------------------
    def _to_boolean_or_null(s: pd.Series) -> pd.Series:
        """Convert to boolean or null.

        Like ``toBoolean`` but returns null for any invalid input (e.g.
        ``'yes'``, ``'1.5'``) instead of propagating the original value.
        Null input → null output.

        Accepted truthy strings: ``'true'`` (case-insensitive).
        Accepted falsy strings: ``'false'`` (case-insensitive).
        Accepted integers: 1 → True, 0 → False.

        Args:
            s: Series of values to convert.

        Returns:
            Series of ``True``/``False``/``None``.

        """
        # Vectorized implementation replacing .apply(_cvt) anti-pattern

        if len(s) == 0:
            return s.copy()

        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        # Convert all non-null values to string and normalize (vectorized)
        s_str = nr.non_null_vals.astype(str).str.lower()

        # Define accepted boolean mappings
        bool_map = {"true": True, "false": False, "1": True, "0": False}

        # Use pandas vectorized mapping
        mapped_values = s_str.map(bool_map)

        # Only keep values that were successfully mapped (not NaN)
        valid_mask = mapped_values.notna()
        result_mask = nr.non_null_mask.copy()
        result_mask[nr.non_null_mask] = valid_mask

        # Set the valid boolean results
        nr.result[result_mask] = mapped_values[valid_mask]

        # All other values remain None (invalid conversions)
        return nr.result

    registry.register_function(
        name="toBooleanOrNull",
        callable=_to_boolean_or_null,
        min_args=1,
        max_args=1,
        description="Convert to boolean, returning null for null or invalid input",
        example="toBooleanOrNull('true') → true, toBooleanOrNull('yes') → null",
    )

    # ------------------------------------------------------------------
    # toIntegerOrNull(value) -> int | null
    # ------------------------------------------------------------------
    def _to_integer_or_null(s: pd.Series) -> pd.Series:
        """Convert to integer or null.

        Like ``toInteger`` but explicitly documented to return null for
        invalid inputs.  ``toInteger`` already uses ``pd.to_numeric``
        with ``errors='coerce'``, so they are functionally equivalent;
        ``toIntegerOrNull`` is provided as an explicit alias.

        Args:
            s: Series of values to convert.

        Returns:
            Int64 Series (supports nullable integers); invalid → null.

        """
        numeric = pd.to_numeric(s, errors="coerce")
        return pd.Series(np.fix(numeric), index=numeric.index).astype(
            "Int64",
        )

    registry.register_function(
        name="toIntegerOrNull",
        callable=_to_integer_or_null,
        min_args=1,
        max_args=1,
        description="Convert to integer, returning null for null or invalid input",
        example="toIntegerOrNull('42') → 42, toIntegerOrNull('abc') → null",
    )

    # ------------------------------------------------------------------
    # toFloatOrNull(value) -> float | null
    # ------------------------------------------------------------------
    def _to_float_or_null(s: pd.Series) -> pd.Series:
        """Convert to float or null.

        Like ``toFloat`` but explicitly documented to return null for
        invalid inputs.  ``toFloat`` already uses ``pd.to_numeric`` with
        ``errors='coerce'``, so they are functionally equivalent;
        ``toFloatOrNull`` is provided as an explicit alias.

        Args:
            s: Series of values to convert.

        Returns:
            Float Series; invalid values → NaN (null).

        """
        return pd.to_numeric(s, errors="coerce").astype("float64")

    registry.register_function(
        name="toFloatOrNull",
        callable=_to_float_or_null,
        min_args=1,
        max_args=1,
        description="Convert to float, returning null for null or invalid input",
        example="toFloatOrNull('3.14') → 3.14, toFloatOrNull('abc') → null",
    )
