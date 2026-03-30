"""Category registration module for scalar functions."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pandas as pd
from shared.helpers import is_null_value

from pycypher.constants import (
    _broadcast_series,
    _init_null_result,
    _scalar_int,
    _scalar_int_opt,
)

if TYPE_CHECKING:
    from pycypher.scalar_functions import ScalarFunctionRegistry

_is_null = is_null_value


def register(registry: ScalarFunctionRegistry) -> None:
    """Register list manipulation functions.

    Cypher usage::

        MATCH (p:Person) RETURN head(p.hobbies) AS first_hobby
        MATCH (p:Person) RETURN last(p.scores) AS final_score
        MATCH (p:Person) RETURN tail(p.items) AS all_but_first
        RETURN range(1, 10) AS one_to_ten
        MATCH (p:Person) RETURN sort(p.grades) AS sorted_grades
        MATCH (p:Person) RETURN toStringList(p.ids) AS string_ids

    Null handling: null list inputs return null.  Empty lists return
    null for ``head``/``last``, empty list for ``tail``/``sort``.
    """

    # toList(x) — wrap a scalar in a single-element list; pass lists through;
    # propagate null.
    def _to_list(s: pd.Series) -> pd.Series:
        """Wrap scalar in a list, or return list unchanged; null → null."""
        # Further vectorized implementation eliminating remaining .apply() anti-patterns
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        # Create mask for list types using vectorized operations
        list_indices = []
        scalar_values = []
        scalar_indices = []

        # Process in batch - single pass through non-null values
        for idx, val in nr.non_null_vals.items():
            if isinstance(val, list):
                list_indices.append(idx)
                nr.result[idx] = val  # Pass through unchanged
            else:
                scalar_indices.append(idx)
                scalar_values.append([val])  # Wrap in list

        # Batch assignment of scalar values wrapped in lists
        if scalar_indices:
            nr.result[scalar_indices] = scalar_values

        return nr.result

    registry.register_function(
        name="toList",
        callable=_to_list,
        min_args=1,
        max_args=1,
        description="Wrap a scalar in a single-element list; lists are returned unchanged; null → null",
        example="toList(42) → [42], toList([1,2]) → [1,2]",
    )

    # head(list) -> first element
    def _head(s: pd.Series) -> pd.Series:
        """Return the first element of a list.

        Args:
            s: Series of lists

        Returns:
            Series of first elements (null for empty/null lists)

        """
        # Vectorized implementation replacing .apply(_get_head) anti-pattern
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        list_indices = []
        first_element_values = []

        for idx, val in nr.non_null_vals.items():
            if isinstance(val, list) and len(val) > 0:
                list_indices.append(idx)
                first_element_values.append(val[0])

        if list_indices:
            nr.result[list_indices] = first_element_values

        return nr.result

    registry.register_function(
        name="head",
        callable=_head,
        min_args=1,
        max_args=1,
        description="Return the first element of a list",
        example="head([1, 2, 3]) → 1",
    )

    # last(list) -> last element
    def _last(s: pd.Series) -> pd.Series:
        """Return the last element of a list.

        Args:
            s: Series of lists

        Returns:
            Series of last elements (null for empty/null lists)

        """
        # Vectorized implementation replacing .apply(_get_last) anti-pattern
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        list_indices = []
        last_element_values = []

        for idx, val in nr.non_null_vals.items():
            if isinstance(val, list) and len(val) > 0:
                list_indices.append(idx)
                last_element_values.append(val[-1])

        if list_indices:
            nr.result[list_indices] = last_element_values

        return nr.result

    registry.register_function(
        name="last",
        callable=_last,
        min_args=1,
        max_args=1,
        description="Return the last element of a list",
        example="last([1, 2, 3]) → 3",
    )

    # tail(list) -> list without first element
    def _tail(s: pd.Series) -> pd.Series:
        """Return all elements of a list except the first.

        Args:
            s: Series of lists

        Returns:
            Series of sublists

        """
        # Vectorized implementation replacing .apply(_get_tail) anti-pattern
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        list_indices = []
        tail_element_values = []

        for idx, val in nr.non_null_vals.items():
            if isinstance(val, list):
                list_indices.append(idx)
                tail_element_values.append(val[1:])

        if list_indices:
            nr.result[list_indices] = tail_element_values

        return nr.result

    registry.register_function(
        name="tail",
        callable=_tail,
        min_args=1,
        max_args=1,
        description="Return all but the first element of a list",
        example="tail([1, 2, 3]) → [2, 3]",
    )

    # range(start, end, step?) -> list
    def _range(
        start: pd.Series,
        end: pd.Series,
        step: pd.Series | None = None,
    ) -> pd.Series:
        """Generate an integer range list.

        Args:
            start: Start value (inclusive, uses first row value)
            end: End value (inclusive in Cypher, uses first row value)
            step: Optional step (uses first row value, defaults to 1)

        Returns:
            Series where every row contains the same range list

        """
        from pycypher.config import MAX_COLLECTION_SIZE
        from pycypher.exceptions import SecurityError

        start_val = _scalar_int(start)
        end_val = _scalar_int(end)
        step_val = _scalar_int_opt(step, 1)
        # Check size before materializing to prevent memory exhaustion.
        if step_val != 0:
            estimated_size = abs((end_val - start_val) // step_val) + 1
            if estimated_size > MAX_COLLECTION_SIZE:
                msg = (
                    f"range() would produce ~{estimated_size:,} elements, "
                    f"exceeding limit of {MAX_COLLECTION_SIZE:,}. "
                    f"Adjust PYCYPHER_MAX_COLLECTION_SIZE to increase."
                )
                raise SecurityError(msg)
        # Cypher range() is inclusive on both ends
        result_list = list(range(start_val, end_val + 1, step_val))
        return _broadcast_series(result_list, len(start))

    registry.register_function(
        name="range",
        callable=_range,
        min_args=2,
        max_args=3,
        description="Generate a list of integers from start to end (inclusive)",
        example="range(1, 5) → [1, 2, 3, 4, 5]",
    )

    # ── Trigonometric functions ──────────────────────────────────────────

    def _make_trig1_np(fn_np: Any) -> Any:
        """Return a vectorised single-argument trig function using numpy.

        Null inputs produce None; domain errors (e.g. asin/acos outside
        [-1, 1]) produce None via NaN propagation — same semantics as the
        previous .apply() path but with O(1) Python overhead instead of
        O(n).
        """

        def _impl(s: pd.Series) -> pd.Series:
            arr = pd.to_numeric(s, errors="coerce").to_numpy(
                dtype=np.float64,
            )
            with np.errstate(invalid="ignore"):
                result = fn_np(arr)
            out = pd.Series(result, dtype=object)
            out[np.isnan(result)] = None
            return out

        return _impl

    for _name, _fn_np, _desc, _ex in [
        ("sin", np.sin, "Sine of an angle in radians", "sin(0) → 0.0"),
        ("cos", np.cos, "Cosine of an angle in radians", "cos(0) → 1.0"),
        ("tan", np.tan, "Tangent of an angle in radians", "tan(0) → 0.0"),
        ("asin", np.arcsin, "Arcsine, result in radians", "asin(1) → π/2"),
        (
            "acos",
            np.arccos,
            "Arccosine, result in radians",
            "acos(1) → 0.0",
        ),
        (
            "atan",
            np.arctan,
            "Arctangent, result in radians",
            "atan(1) → π/4",
        ),
    ]:
        registry.register_function(
            name=_name,
            callable=_make_trig1_np(_fn_np),
            min_args=1,
            max_args=1,
            description=_desc,
            example=_ex,
        )

    # atan2(y, x) — two-argument arctangent
    def _atan2(y: pd.Series, x: pd.Series) -> pd.Series:
        """Two-argument arctangent (null-safe, numpy-vectorised)."""
        if len(y) != len(x):
            msg = (
                f"atan2 requires both arguments to have the same length, "
                f"got {len(y)} and {len(x)}"
            )
            raise ValueError(
                msg,
            )
        y_arr = pd.to_numeric(y, errors="coerce").to_numpy(
            dtype=np.float64,
        )
        x_arr = pd.to_numeric(x, errors="coerce").to_numpy(
            dtype=np.float64,
        )
        with np.errstate(invalid="ignore"):
            result = np.arctan2(y_arr, x_arr)
        out = pd.Series(result, dtype=object)
        out[np.isnan(result)] = None
        return out

    registry.register_function(
        name="atan2",
        callable=_atan2,
        min_args=2,
        max_args=2,
        description="Two-argument arctangent atan2(y, x), result in radians",
        example="atan2(1, 1) → π/4",
    )

    # sinh / cosh / tanh — numpy-vectorised (no domain errors possible)
    for _hyp_name, _hyp_fn, _hyp_desc, _hyp_ex in [
        ("sinh", np.sinh, "Return the hyperbolic sine", "sinh(0) → 0.0"),
        ("cosh", np.cosh, "Return the hyperbolic cosine", "cosh(0) → 1.0"),
        (
            "tanh",
            np.tanh,
            "Return the hyperbolic tangent",
            "tanh(0) → 0.0",
        ),
    ]:
        registry.register_function(
            name=_hyp_name,
            callable=_make_trig1_np(_hyp_fn),
            min_args=1,
            max_args=1,
            description=_hyp_desc,
            example=_hyp_ex,
        )

    # degrees / radians — numpy-vectorised, no domain errors possible
    def _degrees(s: pd.Series) -> pd.Series:
        """Convert radians to degrees (null-safe, numpy-vectorised)."""
        arr = pd.to_numeric(s, errors="coerce").to_numpy(dtype=np.float64)
        result = np.degrees(arr)
        out = pd.Series(result, dtype=object)
        out[np.isnan(result)] = None
        return out

    registry.register_function(
        name="degrees",
        callable=_degrees,
        min_args=1,
        max_args=1,
        description="Convert radians to degrees",
        example="degrees(3.14159) → 180.0",
    )

    def _radians(s: pd.Series) -> pd.Series:
        """Convert degrees to radians (null-safe, numpy-vectorised)."""
        arr = pd.to_numeric(s, errors="coerce").to_numpy(dtype=np.float64)
        result = np.radians(arr)
        out = pd.Series(result, dtype=object)
        out[np.isnan(result)] = None
        return out

    registry.register_function(
        name="radians",
        callable=_radians,
        min_args=1,
        max_args=1,
        description="Convert degrees to radians",
        example="radians(180) → 3.14159",
    )

    # pi() and e() — mathematical constants returned as a uniform Series
    def _pi(s: pd.Series) -> pd.Series:
        """Return π for every row (dummy argument ignored)."""
        return _broadcast_series(math.pi, len(s))

    registry.register_function(
        name="pi",
        callable=_pi,
        min_args=0,
        max_args=1,
        description="Return the mathematical constant π (≈ 3.14159)",
        example="pi() → 3.14159265358979",
    )

    def _e(s: pd.Series) -> pd.Series:
        """Return Euler's number e for every row (dummy argument ignored)."""
        return _broadcast_series(math.e, len(s))

    registry.register_function(
        name="e",
        callable=_e,
        min_args=0,
        max_args=1,
        description="Return Euler's number e (≈ 2.71828)",
        example="e() → 2.71828182845905",
    )

    def _rand(s: pd.Series) -> pd.Series:
        """Return a random float in [0.0, 1.0) for every row."""
        import secrets

        secure_random = secrets.SystemRandom()
        return pd.Series(
            [secure_random.random() for _ in range(len(s))],
            dtype=float,
        )

    registry.register_function(
        name="rand",
        callable=_rand,
        min_args=0,
        max_args=0,
        description="Return a random float in [0.0, 1.0)",
        example="rand() → 0.6837071833499885",
    )

    # log10(x) — base-10 logarithm (numpy-vectorised; np.log10(0)=-inf → None)
    def _log10(s: pd.Series) -> pd.Series:
        """Base-10 logarithm — null-safe, numpy-vectorised; null for x ≤ 0."""
        arr = pd.to_numeric(s, errors="coerce").to_numpy(dtype=np.float64)
        with np.errstate(invalid="ignore", divide="ignore"):
            result = np.log10(arr)
        out = pd.Series(result, dtype=object)
        out[~np.isfinite(result)] = (
            None  # NaN (null input) and -inf (x≤0) → None
        )
        return out

    registry.register_function(
        name="log10",
        callable=_log10,
        min_args=1,
        max_args=1,
        description="Base-10 logarithm (null for x ≤ 0)",
        example="log10(100) → 2.0",
    )

    # pow(base, exponent) — power function (numpy-vectorised)
    # NaN → None (null input or complex result e.g. pow(-2, 0.5))
    # +inf preserved (overflow per Neo4j spec)
    def _pow(base: pd.Series, exponent: pd.Series) -> pd.Series:
        """Power function base^exponent — null-safe, numpy-vectorised."""
        if len(base) != len(exponent):
            msg = (
                f"pow requires both arguments to have the same length, "
                f"got {len(base)} and {len(exponent)}"
            )
            raise ValueError(
                msg,
            )
        b_arr = pd.to_numeric(base, errors="coerce").to_numpy(
            dtype=np.float64,
        )
        e_arr = pd.to_numeric(exponent, errors="coerce").to_numpy(
            dtype=np.float64,
        )
        with np.errstate(invalid="ignore", divide="ignore", over="ignore"):
            result = np.power(b_arr, e_arr)
        out = pd.Series(result, dtype=object)
        out[np.isnan(result)] = (
            None  # NaN (null input or complex) → None; +inf kept
        )
        return out

    registry.register_function(
        name="pow",
        callable=_pow,
        min_args=2,
        max_args=2,
        description="Raise base to the power of exponent (overflow → +inf)",
        example="pow(2, 10) → 1024.0",
    )

    # sort(list) -> sorted list (ascending, nulls last)
    def _sort(s: pd.Series) -> pd.Series:
        """Return a new list sorted in ascending order (null-safe).

        Null elements are placed at the end (Neo4j semantics).
        Non-list inputs return null.

        Args:
            s: Series of lists to sort.

        Returns:
            Series of sorted lists; null for null inputs.

        """
        # Vectorized implementation replacing .apply(_sort_one) anti-pattern
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        list_indices = []
        sorted_list_values = []

        for idx, val in nr.non_null_vals.items():
            if isinstance(val, list):
                non_null = [v for v in val if v is not None]
                nulls = [v for v in val if v is None]
                sorted_list = sorted(non_null) + nulls
                list_indices.append(idx)
                sorted_list_values.append(sorted_list)

        if list_indices:
            nr.result[list_indices] = sorted_list_values

        return nr.result

    registry.register_function(
        name="sort",
        callable=_sort,
        min_args=1,
        max_args=1,
        description="Return a sorted copy of a list (ascending, nulls last)",
        example="sort([3, 1, 2]) → [1, 2, 3]",
    )

    # flatten(list) -> flat list (one level deep)
    def _flatten(s: pd.Series) -> pd.Series:
        """Flatten one level of nesting in a list (null-safe).

        Non-list sub-elements are included as-is.  Only one level is
        flattened (matching openCypher semantics).

        Args:
            s: Series of lists to flatten.

        Returns:
            Series of flattened lists; null for null inputs.

        """
        # Vectorized implementation replacing .apply(_flatten_one) anti-pattern
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        list_indices = []
        flattened_list_values = []

        for idx, val in nr.non_null_vals.items():
            if isinstance(val, list):
                result_list: list[object] = []
                for item in val:
                    if isinstance(item, list):
                        result_list.extend(item)
                    else:
                        result_list.append(item)
                list_indices.append(idx)
                flattened_list_values.append(result_list)

        if list_indices:
            nr.result[list_indices] = flattened_list_values

        return nr.result

    registry.register_function(
        name="flatten",
        callable=_flatten,
        min_args=1,
        max_args=1,
        description="Flatten one level of nesting in a list",
        example="flatten([[1, 2], [3]]) → [1, 2, 3]",
    )

    # ── List conversion functions ─────────────────────────────────────────

    def _to_string_list(s: pd.Series) -> pd.Series:
        """Apply toString to each element of a list (null-safe)."""
        # Vectorized implementation eliminating .apply() anti-pattern
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        list_indices = []
        string_list_values = []

        for idx, val in nr.non_null_vals.items():
            if _is_null(val) or not isinstance(val, list):
                continue

            converted_list = []
            for v in val:
                if v is None:
                    converted_list.append(None)
                elif isinstance(v, bool):
                    converted_list.append("true" if v else "false")
                else:
                    converted_list.append(str(v))

            list_indices.append(idx)
            string_list_values.append(converted_list)

        if list_indices:
            nr.result[list_indices] = string_list_values

        return nr.result

    registry.register_function(
        name="toStringList",
        callable=_to_string_list,
        min_args=1,
        max_args=1,
        description="Convert each list element to a string (null for null elements)",
        example="toStringList([1, 2]) → ['1', '2']",
    )

    def _to_integer_list(s: pd.Series) -> pd.Series:
        """Apply toInteger to each element of a list (null-safe)."""
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        def _cvt(v: object) -> object:
            if v is None:
                return None
            try:
                import math as _math

                return _math.trunc(float(str(v)))
            except (ValueError, TypeError):
                return None

        list_indices = []
        integer_list_values = []

        for idx, val in nr.non_null_vals.items():
            if _is_null(val) or not isinstance(val, list):
                continue

            list_indices.append(idx)
            integer_list_values.append([_cvt(v) for v in val])

        if list_indices:
            nr.result[list_indices] = integer_list_values

        return nr.result

    registry.register_function(
        name="toIntegerList",
        callable=_to_integer_list,
        min_args=1,
        max_args=1,
        description="Convert each list element to an integer (null for unconvertible)",
        example="toIntegerList(['1', '2']) → [1, 2]",
    )

    def _to_float_list(s: pd.Series) -> pd.Series:
        """Apply toFloat to each element of a list (null-safe)."""
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        def _cvt(v: object) -> object:
            if v is None:
                return None
            try:
                fval = float(str(v))
                return (
                    None
                    if math.isnan(fval) and str(v) not in ("nan", "NaN")
                    else fval
                )
            except (ValueError, TypeError):
                return None

        list_indices = []
        float_list_values = []

        for idx, val in nr.non_null_vals.items():
            if _is_null(val) or not isinstance(val, list):
                continue

            list_indices.append(idx)
            float_list_values.append([_cvt(v) for v in val])

        if list_indices:
            nr.result[list_indices] = float_list_values

        return nr.result

    registry.register_function(
        name="toFloatList",
        callable=_to_float_list,
        min_args=1,
        max_args=1,
        description="Convert each list element to a float (null for unconvertible)",
        example="toFloatList(['1.1', '2.2']) → [1.1, 2.2]",
    )

    def _to_boolean_list(s: pd.Series) -> pd.Series:
        """Apply toBoolean to each element of a list (null-safe)."""
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        def _cvt(v: object) -> object:
            if v is None:
                return None
            if isinstance(v, bool):
                return v
            sv = str(v).lower()
            if sv == "true":
                return True
            if sv == "false":
                return False
            return None

        list_indices = []
        boolean_list_values = []

        for idx, val in nr.non_null_vals.items():
            if _is_null(val) or not isinstance(val, list):
                continue

            list_indices.append(idx)
            boolean_list_values.append([_cvt(v) for v in val])

        if list_indices:
            nr.result[list_indices] = boolean_list_values

        return nr.result

    registry.register_function(
        name="toBooleanList",
        callable=_to_boolean_list,
        min_args=1,
        max_args=1,
        description="Convert each list element to a boolean (null for unconvertible)",
        example="toBooleanList(['true', 'false']) → [true, false]",
    )

    # min(list) -> scalar minimum of list elements (null input or empty → null)
    def _list_min(s: pd.Series) -> pd.Series:
        """Return the minimum non-null element of each list.

        Args:
            s: Series whose elements are lists.

        Returns:
            Series of minimum values; null for null/empty inputs.

        """

        def _one(x: object) -> object:
            if _is_null(x):
                return None
            if not isinstance(x, list):
                return None
            non_null = [v for v in x if not _is_null(v)]
            if not non_null:
                return None
            return min(non_null)

        return cast(pd.Series, s.apply(_one))

    registry.register_function(
        name="min",
        callable=_list_min,
        min_args=1,
        max_args=1,
        description="Return the minimum element of a list (null elements ignored)",
        example="min([3, 1, 2]) → 1",
    )

    # max(list) -> scalar maximum of list elements (null input or empty → null)
    def _list_max(s: pd.Series) -> pd.Series:
        """Return the maximum non-null element of each list.

        Args:
            s: Series whose elements are lists.

        Returns:
            Series of maximum values; null for null/empty inputs.

        """

        def _one(x: object) -> object:
            if _is_null(x):
                return None
            if not isinstance(x, list):
                return None
            non_null = [v for v in x if not _is_null(v)]
            if not non_null:
                return None
            return max(non_null)

        return cast(pd.Series, s.apply(_one))

    registry.register_function(
        name="max",
        callable=_list_max,
        min_args=1,
        max_args=1,
        description="Return the maximum element of a list (null elements ignored)",
        example="max([3, 1, 2]) → 3",
    )
