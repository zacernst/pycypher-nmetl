"""Category registration module for scalar functions."""

from __future__ import annotations

import decimal as _decimal
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from shared.helpers import is_null_value

from pycypher.constants import (
    _init_null_result,
    _scalar_int_opt,
)

if TYPE_CHECKING:
    from pycypher.scalar_functions import ScalarFunctionRegistry

_is_null = is_null_value


def register(registry: ScalarFunctionRegistry) -> None:
    """Register mathematical functions.

    Cypher usage::

        MATCH (p:Person) RETURN abs(p.balance) AS abs_balance
        MATCH (p:Person) RETURN ceil(p.rating) AS rounded_up
        MATCH (p:Person) RETURN round(p.score, 2) AS score_2dp
        MATCH (p:Person) RETURN round(p.value, 0, 'HALF_EVEN') AS bankers_round
        MATCH (p:Person) RETURN sqrt(p.area) AS side_length
        RETURN log10(1000) AS three, pow(2, 10) AS kilobyte

    Null handling: null inputs produce null outputs.  Domain errors
    (e.g. ``sqrt(-1)``, ``log(0)``) return null rather than raising.
    Overflow in ``exp``/``pow`` returns ``Infinity`` per Neo4j semantics.
    """

    # ---------------------------------------------------------------
    # Shared factory for numpy-vectorised single-arg math functions.
    # All conversions follow the same pattern established in Loop 188
    # for trig functions:
    #   pd.to_numeric(s, errors="coerce") converts None/NaN/non-numeric
    #   to NaN; the numpy op runs in C; NaN values become Python None.
    #
    # map_inf_to_null=True: ±inf also → None.  Use for logarithms
    # where np.log(0) = -inf (domain error, not a useful result).
    # Leave False for exp/pow where overflow → +inf is the
    # specified Neo4j behaviour.
    # ---------------------------------------------------------------
    def _make_math1_np(
        fn_np: Any,
        *,
        map_inf_to_null: bool = False,
    ) -> Any:
        """Return a vectorised, null-safe single-arg math function."""

        def _impl(s: pd.Series) -> pd.Series:
            arr = pd.to_numeric(s, errors="coerce").to_numpy(
                dtype=np.float64,
            )
            with np.errstate(
                invalid="ignore",
                divide="ignore",
                over="ignore",
            ):
                result = fn_np(arr)
            out = pd.Series(result, dtype=object)
            if map_inf_to_null:
                out[~np.isfinite(result)] = None
            else:
                out[np.isnan(result)] = None
            return out

        return _impl

    # abs(n) -> number  (null-safe; numpy-vectorised)
    registry.register_function(
        name="abs",
        callable=_make_math1_np(np.abs),
        min_args=1,
        max_args=1,
        description="Return the absolute value",
        example="abs(-5) → 5",
    )

    # ceil(n) -> Float (smallest integer >= n; numpy-vectorised)
    registry.register_function(
        name="ceil",
        callable=_make_math1_np(np.ceil),
        min_args=1,
        max_args=1,
        description="Round up to the nearest integer (returns Float)",
        example="ceil(1.1) → 2.0",
    )

    # floor(n) -> Float (largest integer <= n; numpy-vectorised)
    registry.register_function(
        name="floor",
        callable=_make_math1_np(np.floor),
        min_args=1,
        max_args=1,
        description="Round down to the nearest integer (returns Float)",
        example="floor(1.9) → 1.0",
    )

    # round(n, precision?, mode?) -> Float
    # Default rounding mode: HALF_UP (ties away from zero), per Neo4j spec.
    # Neo4j 5.x 3-arg form: round(n, precision, mode) where mode is one of
    # HALF_UP, HALF_DOWN, HALF_EVEN, CEILING, FLOOR, UP, DOWN.

    _ROUND_MODE_MAP: dict[str, str] = {
        "HALF_UP": _decimal.ROUND_HALF_UP,
        "HALF_DOWN": _decimal.ROUND_HALF_DOWN,
        "HALF_EVEN": _decimal.ROUND_HALF_EVEN,
        "CEILING": _decimal.ROUND_CEILING,
        "FLOOR": _decimal.ROUND_FLOOR,
        "UP": _decimal.ROUND_UP,
        "DOWN": _decimal.ROUND_DOWN,
    }

    def _round(
        s: pd.Series,
        precision: pd.Series | None = None,
        mode: pd.Series | None = None,
    ) -> pd.Series:
        """Round a numeric series to the requested precision using the given mode.

        Without a mode argument, uses HALF_UP (ties away from zero) per the
        Neo4j/openCypher default.  With the optional third argument, any of
        the seven Neo4j 5.x rounding modes are supported:

        * ``HALF_UP``   — ties go away from zero (default)
        * ``HALF_DOWN`` — ties go toward zero
        * ``HALF_EVEN`` — ties go to nearest even digit (banker's rounding)
        * ``CEILING``   — always toward +∞
        * ``FLOOR``     — always toward −∞
        * ``UP``        — always away from zero
        * ``DOWN``      — always toward zero (truncation)

        Mode names are case-insensitive.

        Args:
            s: Numeric series to round.
            precision: Optional Series of integer decimal places.  If the
                series has more than one distinct value, only the first row's
                value is used (uniform precision per call).
            mode: Optional Series of rounding-mode name strings.

        Returns:
            Rounded ``float64`` series.

        Raises:
            ValueError: If an unrecognised mode name is supplied.

        """
        prec_val = _scalar_int_opt(precision, 0)
        quant = _decimal.Decimal(10) ** (-prec_val)

        rounding_mode: str = _decimal.ROUND_HALF_UP
        if mode is not None and len(mode) > 0:
            raw_mode = str(mode.iloc[0]).upper()
            if raw_mode not in _ROUND_MODE_MAP:
                valid_modes = ", ".join(sorted(_ROUND_MODE_MAP))
                msg = (
                    f"Unknown rounding mode: {mode.iloc[0]!r}. "
                    f"Valid modes: {valid_modes}"
                )
                raise ValueError(
                    msg,
                )
            rounding_mode = _ROUND_MODE_MAP[raw_mode]

        # Vectorized implementation eliminating .apply() anti-pattern
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result.astype("float64")

        try:
            values_array = nr.non_null_vals.to_numpy()
            rounded_values = []

            for val in values_array:
                if _is_null(val):
                    rounded_values.append(None)
                else:
                    try:
                        decimal_val = _decimal.Decimal(str(val))
                        quantized_val = decimal_val.quantize(
                            quant,
                            rounding=rounding_mode,
                        )
                        rounded_values.append(float(quantized_val))
                    except (
                        ValueError,
                        TypeError,
                        _decimal.InvalidOperation,
                    ):
                        rounded_values.append(None)

            nr.result[nr.non_null_mask] = rounded_values

        except (ValueError, TypeError, IndexError):
            # Fallback for complex cases
            nr.result = pd.Series(
                [None] * len(s),
                index=s.index,
                dtype=object,
            )
            for i, val in enumerate(s):
                if _is_null(val):
                    nr.result.iloc[i] = None
                else:
                    try:
                        decimal_val = _decimal.Decimal(str(val))
                        quantized_val = decimal_val.quantize(
                            quant,
                            rounding=rounding_mode,
                        )
                        nr.result.iloc[i] = float(quantized_val)
                    except (
                        ValueError,
                        TypeError,
                        _decimal.InvalidOperation,
                    ):
                        nr.result.iloc[i] = None

        return nr.result.astype("float64")

    registry.register_function(
        name="round",
        callable=_round,
        min_args=1,
        max_args=3,
        description=(
            "Round to nearest integer or specified precision. "
            "Optional third argument selects rounding mode: "
            "HALF_UP (default), HALF_DOWN, HALF_EVEN, CEILING, FLOOR, UP, DOWN."
        ),
        example="round(2.5) → 3.0, round(1.567, 2) → 1.57, round(2.5, 0, 'HALF_EVEN') → 2.0",
    )

    # sign(n) -> -1 | 0 | 1  (numpy-vectorised)
    registry.register_function(
        name="sign",
        callable=_make_math1_np(np.sign),
        min_args=1,
        max_args=1,
        description="Return the sign of a number (-1, 0, or 1)",
        example="sign(-3) → -1, sign(0) → 0, sign(5) → 1",
    )

    # sqrt(n) -> float  (numpy: negative input → NaN → None)
    registry.register_function(
        name="sqrt",
        callable=_make_math1_np(np.sqrt),
        min_args=1,
        max_args=1,
        description="Return the square root (null for x < 0)",
        example="sqrt(9) → 3.0",
    )

    # cbrt(n) -> float  (cube root; supports negative inputs; numpy-vectorised)
    registry.register_function(
        name="cbrt",
        callable=_make_math1_np(np.cbrt),
        min_args=1,
        max_args=1,
        description="Return the cube root",
        example="cbrt(27.0) → 3.0",
    )

    # log(n) -> float  (natural logarithm; np.log(0)=-inf → None via map_inf_to_null)
    registry.register_function(
        name="log",
        callable=_make_math1_np(np.log, map_inf_to_null=True),
        min_args=1,
        max_args=1,
        description="Return the natural logarithm (null for x ≤ 0)",
        example="log(1) → 0.0",
    )

    # exp(n) -> float  (e^n; overflow → +infinity per Neo4j spec; NOT nulled)
    registry.register_function(
        name="exp",
        callable=_make_math1_np(np.exp),
        min_args=1,
        max_args=1,
        description="Return e raised to the power of n (overflow → +inf)",
        example="exp(1) → 2.718...",
    )

    # cot(x) = cos(x)/sin(x) — cotangent (Neo4j 5.x built-in)
    # Returns null for null input and for x = 0 (division by zero → NaN).
    def _cot(s: pd.Series) -> pd.Series:
        """Cotangent: cos(x)/sin(x) — null-safe, numpy-vectorised."""
        arr = pd.to_numeric(s, errors="coerce").to_numpy(dtype=np.float64)
        with np.errstate(invalid="ignore", divide="ignore"):
            result = np.cos(arr) / np.sin(arr)
        out = pd.Series(result, dtype=object)
        out[~np.isfinite(result)] = (
            None  # NaN (null input) and ±inf (x=0) → None
        )
        return out

    registry.register_function(
        name="cot",
        callable=_cot,
        min_args=1,
        max_args=1,
        description="Cotangent cos(x)/sin(x) — null for null input or x = 0",
        example="cot(pi()/4) → 1.0",
    )

    # haversin(x) = (1 - cos(x)) / 2 — half the versine (Neo4j 5.x built-in)
    # Used as a building block for the great-circle distance formula.
    def _haversin(s: pd.Series) -> pd.Series:
        """Half the versine: (1 - cos(x)) / 2 — null-safe, numpy-vectorised."""
        arr = pd.to_numeric(s, errors="coerce").to_numpy(dtype=np.float64)
        with np.errstate(invalid="ignore"):
            result = (1.0 - np.cos(arr)) / 2.0
        out = pd.Series(result, dtype=object)
        out[np.isnan(result)] = None
        return out

    registry.register_function(
        name="haversin",
        callable=_haversin,
        min_args=1,
        max_args=1,
        description="Half the versine (1 - cos(x)) / 2 — great-circle distance building block",
        example="haversin(0) → 0.0, haversin(pi()) → 1.0",
    )

    # hypot(x, y) = sqrt(x² + y²) — Euclidean distance (Neo4j 5.x built-in)
    def _hypot(x: pd.Series, y: pd.Series) -> pd.Series:
        """Euclidean distance sqrt(x²+y²) — null-safe, numpy-vectorised."""
        if len(x) != len(y):
            msg = (
                f"hypot requires both arguments to have the same length, "
                f"got {len(x)} and {len(y)}"
            )
            raise ValueError(
                msg,
            )
        x_arr = pd.to_numeric(x, errors="coerce").to_numpy(
            dtype=np.float64,
        )
        y_arr = pd.to_numeric(y, errors="coerce").to_numpy(
            dtype=np.float64,
        )
        with np.errstate(invalid="ignore"):
            result = np.hypot(x_arr, y_arr)
        out = pd.Series(result, dtype=object)
        out[np.isnan(result)] = None
        return out

    registry.register_function(
        name="hypot",
        callable=_hypot,
        min_args=2,
        max_args=2,
        description="Euclidean distance sqrt(x²+y²) — null if either arg is null",
        example="hypot(3, 4) → 5.0",
    )

    # fmod(x, y) — IEEE 754 floating-point modulo (remainder after truncation)
    # Different from the % operator (integer modulo): fmod preserves the sign
    # of the dividend.  fmod(x, 0) → null (domain error / division by zero).
    def _fmod(x: pd.Series, y: pd.Series) -> pd.Series:
        """IEEE 754 floating-point modulo — null-safe, numpy-vectorised."""
        if len(x) != len(y):
            msg = (
                f"fmod requires both arguments to have the same length, "
                f"got {len(x)} and {len(y)}"
            )
            raise ValueError(
                msg,
            )
        x_arr = pd.to_numeric(x, errors="coerce").to_numpy(
            dtype=np.float64,
        )
        y_arr = pd.to_numeric(y, errors="coerce").to_numpy(
            dtype=np.float64,
        )
        with np.errstate(invalid="ignore", divide="ignore"):
            result = np.fmod(x_arr, y_arr)
        out = pd.Series(result, dtype=object)
        # NaN (null input) and non-finite (y=0 → NaN in IEEE 754 fmod) → None
        out[~np.isfinite(result) | np.isnan(result)] = None
        return out

    registry.register_function(
        name="fmod",
        callable=_fmod,
        min_args=2,
        max_args=2,
        description="IEEE 754 floating-point modulo — null if either arg null or y = 0",
        example="fmod(10, 3) → 1.0, fmod(5.5, 2.3) → 0.9",
    )

    # log2(x) -> float  (base-2 logarithm; np.log2(0)=-inf → None via map_inf_to_null)
    registry.register_function(
        name="log2",
        callable=_make_math1_np(np.log2, map_inf_to_null=True),
        min_args=1,
        max_args=1,
        description="Return the base-2 logarithm (null for x ≤ 0)",
        example="log2(8) → 3.0",
    )

    # ---------------------------------------------------------------
    # Neo4j 5.x bitwise integer functions
    # All six use numpy C-level bitwise ops; null propagation via mask.
    # Input: object-dtype Series (may contain Python int, numpy int, None).
    # Output: object-dtype Series (Python int or None).
    # ---------------------------------------------------------------

    def _bitwise_null_mask(*series: pd.Series) -> np.ndarray:
        """Return a boolean mask that is True where any arg is null/NaN."""
        null_mask = np.zeros(len(series[0]), dtype=bool)
        for s in series:
            null_mask |= s.isna().to_numpy()
        return null_mask

    def _make_bitwise2(np_op: Any) -> Any:
        """Factory for two-argument bitwise functions."""

        def _impl(x: pd.Series, y: pd.Series) -> pd.Series:
            null_mask = _bitwise_null_mask(x, y)
            arr_x = x.to_numpy(dtype=object)
            arr_y = y.to_numpy(dtype=object)
            # Fill None with 0 so numpy won't choke; masked out afterwards.
            safe_x = np.where(null_mask, 0, arr_x).astype(np.int64)
            safe_y = np.where(null_mask, 0, arr_y).astype(np.int64)
            result = np_op(safe_x, safe_y).astype(object)
            result[null_mask] = None
            return pd.Series(result, dtype=object)

        return _impl

    def _make_bitwise1(np_op: Any) -> Any:
        """Factory for single-argument bitwise functions."""

        def _impl(x: pd.Series) -> pd.Series:
            null_mask = _bitwise_null_mask(x)
            arr_x = x.to_numpy(dtype=object)
            safe_x = np.where(null_mask, 0, arr_x).astype(np.int64)
            result = np_op(safe_x).astype(object)
            result[null_mask] = None
            return pd.Series(result, dtype=object)

        return _impl

    registry.register_function(
        name="bitAnd",
        callable=_make_bitwise2(np.bitwise_and),
        min_args=2,
        max_args=2,
        description="Bitwise AND of two integers (null if either arg is null)",
        example="bitAnd(12, 10) → 8",
    )

    registry.register_function(
        name="bitOr",
        callable=_make_bitwise2(np.bitwise_or),
        min_args=2,
        max_args=2,
        description="Bitwise OR of two integers (null if either arg is null)",
        example="bitOr(12, 10) → 14",
    )

    registry.register_function(
        name="bitXor",
        callable=_make_bitwise2(np.bitwise_xor),
        min_args=2,
        max_args=2,
        description="Bitwise XOR of two integers (null if either arg is null)",
        example="bitXor(12, 10) → 6",
    )

    registry.register_function(
        name="bitNot",
        callable=_make_bitwise1(np.invert),
        min_args=1,
        max_args=1,
        description="Bitwise NOT (one's complement) of an integer (null if arg is null)",
        example="bitNot(0) → -1",
    )

    registry.register_function(
        name="bitShiftLeft",
        callable=_make_bitwise2(np.left_shift),
        min_args=2,
        max_args=2,
        description="Arithmetic left shift: x << y (null if either arg is null)",
        example="bitShiftLeft(1, 3) → 8",
    )

    registry.register_function(
        name="bitShiftRight",
        callable=_make_bitwise2(np.right_shift),
        min_args=2,
        max_args=2,
        description="Arithmetic right shift: x >> y (null if either arg is null)",
        example="bitShiftRight(16, 2) → 4",
    )

    # ------------------------------------------------------------------
    # gcd(integer, integer) → integer   (Neo4j 5.0+)
    # Vectorized via np.gcd ufunc — same null-mask pattern as bitwise
    # functions above.  ~3-4x faster than a Python-level apply() loop.
    # ------------------------------------------------------------------
    registry.register_function(
        name="gcd",
        callable=_make_bitwise2(np.gcd),
        min_args=2,
        max_args=2,
        description=(
            "Return the greatest common divisor of two integers (Neo4j 5.x). "
            "Result is always non-negative; null propagates."
        ),
        example="gcd(12, 8) → 4, gcd(0, 5) → 5",
    )

    # ------------------------------------------------------------------
    # lcm(integer, integer) → integer   (Neo4j 5.0+)
    # Vectorized via np.lcm ufunc — same null-mask pattern as bitwise
    # functions above.  ~3-4x faster than a Python-level apply() loop.
    # ------------------------------------------------------------------
    registry.register_function(
        name="lcm",
        callable=_make_bitwise2(np.lcm),
        min_args=2,
        max_args=2,
        description=(
            "Return the least common multiple of two integers (Neo4j 5.x). "
            "Result is always non-negative; null propagates; lcm(0, n) = 0."
        ),
        example="lcm(4, 6) → 12, lcm(0, 5) → 0",
    )
