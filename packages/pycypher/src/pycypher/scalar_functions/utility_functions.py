"""Category registration module for scalar functions."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from shared.helpers import is_null_value

from pycypher.constants import _broadcast_series, _scalar_raw

if TYPE_CHECKING:
    from pycypher.scalar_functions import ScalarFunctionRegistry

_is_null = is_null_value


def register(registry: ScalarFunctionRegistry) -> None:
    """Register general utility functions.

    Cypher usage::

        MATCH (p:Person) RETURN coalesce(p.nickname, p.name) AS display_name
        MATCH (p:Person) RETURN id(p) AS node_id
        MATCH (p:Person) RETURN nullIf(p.status, 'N/A') AS status
        MATCH (p:Person) WHERE exists(p.email) RETURN p
        RETURN randomUUID() AS uuid

    Null handling: ``coalesce`` returns the first non-null argument;
    ``nullIf`` returns null when the two arguments are equal.
    """

    # coalesce(value1, value2, ...) -> first non-null
    def _coalesce(*series: pd.Series) -> pd.Series:
        """Return first non-null value across arguments.

        Args:
            *series: Variable number of Series to check

        Returns:
            Series with first non-null value from each position

        Example:
            coalesce([null, 'a'], [null, 'b'], ['c', 'c'])
            → ['c', 'a']

        """
        if not series:
            from pycypher.exceptions import FunctionArgumentError

            raise FunctionArgumentError(
                function_name="coalesce",
                expected_args=1,
                actual_args=0,
                argument_description="at least 1 expression",
            )

        result = series[0]
        for s in series[1:]:
            # Fill nulls with values from next series
            result = result.fillna(s)

        return result

    registry.register_function(
        name="coalesce",
        callable=_coalesce,
        min_args=1,
        max_args=None,  # Unlimited arguments
        description="Return first non-null value from arguments",
        example="coalesce(null, 'default') → 'default'",
    )

    # id(node_or_rel) -> identity value
    # The argument is already the entity ID Series (binding column value),
    # so id() is the identity function — just return the argument unchanged.
    def _id_fn(s: Any) -> Any:
        """Return internal node/relationship ID (identity function)."""
        return s

    registry.register_function(
        name="id",
        callable=_id_fn,
        min_args=1,
        max_args=1,
        description="Return the internal ID of a node or relationship",
        example="id(p) → 1",
    )

    # elementId(node_or_rel) -> same as id(); Neo4j 5.x compatible alias.
    registry.register_function(
        name="elementId",
        callable=_id_fn,
        min_args=1,
        max_args=1,
        description="Return the element ID of a node or relationship (Neo4j 5.x alias for id())",
        example="elementId(p) → 1",
    )

    # ------------------------------------------------------------------
    # nullIf(v1, v2) -> v1 if v1 != v2, else null
    # ------------------------------------------------------------------
    def _null_if(v1: pd.Series, v2: pd.Series) -> pd.Series:
        """Return null where v1 equals v2, otherwise return v1.

        Useful for replacing sentinel "empty" values (e.g. 0 or 'N/A')
        with explicit null so downstream ``coalesce()`` or null-checks work
        as expected.

        Args:
            v1: Primary value series.
            v2: Comparison value series (scalar; first value used).

        Returns:
            Series matching v1 except positions where v1 == v2 are None.

        """
        cmp_val = _scalar_raw(v2)

        # Vectorized implementation: use pandas where() for conditional replacement
        # This replaces the .apply(_check) anti-pattern with vectorized operations
        result = v1.copy()

        # Create mask for values equal to comparison value (vectorized)
        # Handle null comparison properly
        if pd.isna(cmp_val):
            # If comparing to null, nulls should become null (no change)
            # Non-nulls should remain unchanged
            mask = _broadcast_series(False, len(v1), index=v1.index)
        else:
            # Compare v1 to cmp_val (vectorized comparison)
            mask = (v1 == cmp_val) & v1.notna()

        # Replace matching values with None (vectorized)
        result = result.where(~mask, None)

        return result

    registry.register_function(
        name="nullIf",
        callable=_null_if,
        min_args=2,
        max_args=2,
        description="Return null if v1 equals v2, else return v1",
        example="nullIf(0, 0) → null, nullIf(1, 0) → 1",
    )

    # ------------------------------------------------------------------
    # isNaN(x) -> boolean
    # ------------------------------------------------------------------
    def _is_nan(s: pd.Series) -> pd.Series:
        """Return True for IEEE 754 NaN values, null for null, False otherwise.

        Useful for data-quality checks after floating-point arithmetic
        that may produce undefined results (0/0, inf-inf, etc.).

        Fast path for float64/int64 Series delegates to numpy ufuncs
        (C-level, no per-element Python calls).  Object-dtype Series
        falls back to an explicit Python loop for correct None handling.

        Args:
            s: Numeric series.

        Returns:
            Boolean series — True where the value is NaN, None where null.

        """
        if pd.api.types.is_float_dtype(s):
            return pd.Series(np.isnan(s.to_numpy()), index=s.index)
        if pd.api.types.is_integer_dtype(s):
            return pd.Series(np.zeros(len(s), dtype=bool), index=s.index)
        arr = s.to_numpy(dtype=object)
        result: list[object] = []
        for x in arr:
            if x is None:
                result.append(None)
            elif isinstance(x, float) and math.isnan(x):
                result.append(True)
            else:
                result.append(False)
        return pd.Series(result, index=s.index)

    registry.register_function(
        name="isNaN",
        callable=_is_nan,
        min_args=1,
        max_args=1,
        description="Return True if value is IEEE 754 NaN",
        example="isNaN(toFloat('NaN')) → true, isNaN(42) → false",
    )

    # ------------------------------------------------------------------
    # infinity() -> float  (positive infinity constant, no-arg)
    # isInfinite(x) -> boolean
    # isFinite(x)   -> boolean
    # ------------------------------------------------------------------
    registry.register_function(
        name="infinity",
        callable=lambda s: pd.Series(
            [float("inf")] * len(s),
            dtype="float64",
        ),
        min_args=0,
        max_args=0,
        description="Return positive infinity",
        example="infinity() → inf",
    )

    def _is_infinite(s: pd.Series) -> pd.Series:
        """Return True for ±∞, None for null, False otherwise.

        Fast path for float64: ``np.isinf`` at C level.
        Fast path for int64: all-False (integers cannot be infinite).
        Object dtype: explicit Python loop for correct None handling.

        Args:
            s: Numeric series.

        Returns:
            Boolean series — True where the value is ±∞, None where null.

        """
        if pd.api.types.is_float_dtype(s):
            return pd.Series(np.isinf(s.to_numpy()), index=s.index)
        if pd.api.types.is_integer_dtype(s):
            return pd.Series(np.zeros(len(s), dtype=bool), index=s.index)
        arr = s.to_numpy(dtype=object)
        result: list[object] = []
        for x in arr:
            if x is None:
                result.append(None)
            else:
                result.append(isinstance(x, float) and math.isinf(x))
        return pd.Series(result, index=s.index)

    registry.register_function(
        name="isInfinite",
        callable=_is_infinite,
        min_args=1,
        max_args=1,
        description="Return True if value is positive or negative infinity",
        example="isInfinite(1/0.0) → true, isInfinite(42) → false",
    )

    def _is_finite(s: pd.Series) -> pd.Series:
        """Return True for finite numbers, None for null, False otherwise.

        Fast path for float64: ``np.isfinite`` at C level.
        Fast path for int64: all-True (integers are always finite).
        Object dtype: explicit Python loop for correct None and bool handling.

        Booleans are excluded from the finite-number definition, matching
        Neo4j Cypher semantics (``isFinite(true)`` → ``false``).

        Args:
            s: Numeric series.

        Returns:
            Boolean series — True where the value is finite, None where null.

        """
        if pd.api.types.is_float_dtype(s):
            return pd.Series(np.isfinite(s.to_numpy()), index=s.index)
        if pd.api.types.is_integer_dtype(s):
            return pd.Series(np.ones(len(s), dtype=bool), index=s.index)
        arr = s.to_numpy(dtype=object)
        result: list[object] = []
        for x in arr:
            if x is None:
                result.append(None)
            elif isinstance(x, float):
                result.append(math.isfinite(x))
            elif isinstance(x, int) and not isinstance(x, bool):
                result.append(True)
            else:
                result.append(False)
        return pd.Series(result, index=s.index)

    registry.register_function(
        name="isFinite",
        callable=_is_finite,
        min_args=1,
        max_args=1,
        description="Return True if value is finite (not NaN, not infinite)",
        example="isFinite(42) → true, isFinite(infinity()) → false",
    )

    # ------------------------------------------------------------------
    # randomUUID() -> string
    # ------------------------------------------------------------------
    import uuid as _uuid

    def _random_uuid(s: pd.Series) -> pd.Series:
        """Return a fresh RFC 4122 UUID string for every row.

        Args:
            s: Dummy series; length determines output length (injected by
               the evaluator for zero-arg functions).

        Returns:
            Series of UUID strings, one distinct UUID per row.

        """
        return pd.Series([str(_uuid.uuid4()) for _ in range(len(s))])

    registry.register_function(
        name="randomUUID",
        callable=_random_uuid,
        min_args=0,
        max_args=1,
        description="Return a fresh RFC 4122 UUID v4 string",
        example="randomUUID() → '550e8400-e29b-41d4-a716-446655440000'",
    )

    # exists(expr) -> boolean (IS NOT NULL check)
    def _exists(s: pd.Series) -> pd.Series:
        """Return True for each row where the value is not null.

        In Cypher, ``exists(n.prop)`` is equivalent to
        ``n.prop IS NOT NULL``.

        Args:
            s: Series of property values (may contain None / NaN).

        Returns:
            Boolean Series — True where the value is not null.

        """
        return s.notna()

    registry.register_function(
        name="exists",
        callable=_exists,
        min_args=1,
        max_args=1,
        description=(
            "Return True if the expression is not null "
            "(equivalent to IS NOT NULL)"
        ),
        example="exists(p.nickname) → True if nickname is set",
    )
