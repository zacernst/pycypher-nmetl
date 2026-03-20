"""Tests covering uncovered lines in scalar function modules.

Targets gaps in:
- math_functions.py: round() null/invalid handling, hypot/fmod length checks
- extended_string_functions.py: left, right, join, isEmpty, lpad, rpad,
  char, charCodeAt, normalize, byteSize
- utility_functions.py: coalesce zero-args, nullIf null comparison,
  isNaN, isInfinite, isFinite edge cases

All tests exercise the functions through Star.execute_query() with Cypher
queries where possible, falling back to direct registry.execute() for
error paths that the query pipeline cannot trigger.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest
from pycypher.relational_models import (
    Context,
    EntityMapping,
)
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def empty_star() -> Star:
    """Star with no entities -- for standalone RETURN queries."""
    ctx = Context(entity_mapping=EntityMapping(mapping={}))
    return Star(context=ctx)


@pytest.fixture()
def scalar_registry() -> ScalarFunctionRegistry:
    """Shared scalar function registry."""
    return ScalarFunctionRegistry.get_instance()


# ---------------------------------------------------------------------------
# Math functions -- round()
# ---------------------------------------------------------------------------


class TestRoundNullAndInvalid:
    """Cover round() null propagation (line 190) and invalid type handling (199-204)."""

    def test_round_null_returns_nan(self, social_star: Star) -> None:
        """round(null) should return null (NaN in DataFrame terms)."""
        result = social_star.execute_query("RETURN round(null) AS r")
        assert len(result) == 1
        val = result["r"].iloc[0]
        assert val is None or (isinstance(val, float) and math.isnan(val))

    def test_round_with_precision(self, empty_star: Star) -> None:
        """round(1.567, 2) should return 1.57."""
        result = empty_star.execute_query("RETURN round(1.567, 2) AS r")
        assert abs(result["r"].iloc[0] - 1.57) < 1e-9

    def test_round_with_half_even_mode(self, empty_star: Star) -> None:
        """round(2.5, 0, 'HALF_EVEN') should return 2.0 (banker's rounding)."""
        result = empty_star.execute_query(
            "RETURN round(2.5, 0, 'HALF_EVEN') AS r"
        )
        assert result["r"].iloc[0] == 2.0

    def test_round_with_half_up_mode(self, empty_star: Star) -> None:
        """round(2.5, 0, 'HALF_UP') should return 3.0."""
        result = empty_star.execute_query(
            "RETURN round(2.5, 0, 'HALF_UP') AS r"
        )
        assert result["r"].iloc[0] == 3.0

    def test_round_invalid_mode_raises(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """An unrecognised rounding mode should raise ValueError."""
        s = pd.Series([1.5])
        prec = pd.Series([0])
        mode = pd.Series(["BOGUS"])
        with pytest.raises(ValueError, match="Unknown rounding mode"):
            scalar_registry.execute("round", [s, prec, mode])

    def test_round_all_nulls(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """round() on a series of all nulls should return all NaN."""
        s = pd.Series([None, None, None], dtype=object)
        result = scalar_registry.execute("round", [s])
        # All values should be NaN (the float64 representation of null)
        assert all(math.isnan(v) for v in result)

    def test_round_mixed_null_and_values(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """round() with a mix of nulls and valid values in the inner loop (line 190)."""
        # Use object dtype so that non-null mask passes but inner loop
        # encounters is_null_value items.
        s = pd.Series([1.5, None, 2.5], dtype=object)
        result = scalar_registry.execute("round", [s])
        assert result.iloc[0] == 2.0  # HALF_UP default
        assert math.isnan(result.iloc[1])
        assert result.iloc[2] == 3.0

    def test_round_invalid_type_in_series(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """round() with non-numeric string values should return None for those (lines 199-204)."""
        s = pd.Series(["not_a_number", 3.14], dtype=object)
        result = scalar_registry.execute("round", [s])
        # "not_a_number" should become NaN; 3.14 should round to 3.0
        assert math.isnan(result.iloc[0])
        assert result.iloc[1] == 3.0


# ---------------------------------------------------------------------------
# Math functions -- hypot() and fmod() basic usage
# ---------------------------------------------------------------------------


class TestHypotAndFmod:
    """Cover hypot and fmod basic operation and null handling."""

    def test_hypot_basic(self, empty_star: Star) -> None:
        """hypot(3, 4) should return 5.0."""
        result = empty_star.execute_query("RETURN hypot(3, 4) AS r")
        assert abs(result["r"].iloc[0] - 5.0) < 1e-9

    def test_fmod_basic(self, empty_star: Star) -> None:
        """fmod(10, 3) should return 1.0."""
        result = empty_star.execute_query("RETURN fmod(10, 3) AS r")
        assert abs(result["r"].iloc[0] - 1.0) < 1e-9

    def test_hypot_mismatched_lengths(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """hypot() with mismatched Series lengths should raise ValueError (lines 345-349)."""
        x = pd.Series([3.0, 4.0])
        y = pd.Series([4.0])
        with pytest.raises(ValueError, match="hypot requires both arguments"):
            scalar_registry.execute("hypot", [x, y])

    def test_fmod_mismatched_lengths(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """fmod() with mismatched Series lengths should raise ValueError (lines 379-383)."""
        x = pd.Series([10.0, 20.0])
        y = pd.Series([3.0])
        with pytest.raises(ValueError, match="fmod requires both arguments"):
            scalar_registry.execute("fmod", [x, y])


# ---------------------------------------------------------------------------
# Extended string functions -- left() and right()
# ---------------------------------------------------------------------------


class TestLeftRight:
    """Cover left() lines 48-49 and right() lines 72-73."""

    def test_left_basic(self, empty_star: Star) -> None:
        """left('hello', 3) should return 'hel'."""
        result = empty_star.execute_query("RETURN left('hello', 3) AS r")
        assert result["r"].iloc[0] == "hel"

    def test_left_zero(self, empty_star: Star) -> None:
        """left('hello', 0) should return empty string."""
        result = empty_star.execute_query("RETURN left('hello', 0) AS r")
        assert result["r"].iloc[0] == ""

    def test_right_basic(self, empty_star: Star) -> None:
        """right('hello', 3) should return 'llo'."""
        result = empty_star.execute_query("RETURN right('hello', 3) AS r")
        assert result["r"].iloc[0] == "llo"

    def test_right_zero(self, empty_star: Star) -> None:
        """right('hello', 0) should return empty string."""
        result = empty_star.execute_query("RETURN right('hello', 0) AS r")
        assert result["r"].iloc[0] == ""


# ---------------------------------------------------------------------------
# Extended string functions -- join() with null items
# ---------------------------------------------------------------------------


class TestJoinFunction:
    """Cover join() null-item filtering (line 173)."""

    def test_join_filters_null_items(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """join() should filter out null items from the list (line 188)."""
        lst = pd.Series([["a", None, "b"]])
        delimiter = pd.Series([","])
        result = scalar_registry.execute("join", [lst, delimiter])
        assert result.iloc[0] == "a,b"

    def test_join_null_delimiter(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """join() with null delimiter should return null (line 174-175)."""
        lst = pd.Series([["a", "b"]])
        delimiter = pd.Series([None], dtype=object)
        result = scalar_registry.execute("join", [lst, delimiter])
        assert result.iloc[0] is None

    def test_join_non_list_input(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """join() with a non-list input should return null (line 184-185)."""
        lst = pd.Series(["not_a_list"])
        delimiter = pd.Series([","])
        result = scalar_registry.execute("join", [lst, delimiter])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# Extended string functions -- isEmpty() with non-collection types
# ---------------------------------------------------------------------------


class TestIsEmpty:
    """Cover isEmpty() non-collection fallback (line 242)."""

    def test_is_empty_integer(self, empty_star: Star) -> None:
        """isEmpty(42) should return false (non-collection, non-null)."""
        result = empty_star.execute_query("RETURN isEmpty(42) AS r")
        assert result["r"].iloc[0] is False or result["r"].iloc[0] == False  # noqa: E712

    def test_is_empty_null(self, empty_star: Star) -> None:
        """isEmpty(null) should return true."""
        result = empty_star.execute_query("RETURN isEmpty(null) AS r")
        assert result["r"].iloc[0] is True or result["r"].iloc[0] == True  # noqa: E712

    def test_is_empty_empty_string(self, empty_star: Star) -> None:
        """isEmpty('') should return true."""
        result = empty_star.execute_query("RETURN isEmpty('') AS r")
        assert result["r"].iloc[0] is True or result["r"].iloc[0] == True  # noqa: E712


# ---------------------------------------------------------------------------
# Extended string functions -- lpad() and rpad() with empty series
# ---------------------------------------------------------------------------


class TestLpadRpadEmptySeries:
    """Cover lpad/rpad empty series early-return (lines 283, 362)."""

    def test_lpad_empty_series(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """lpad() on an empty Series should return an empty Series (line 283)."""
        s = pd.Series([], dtype=object)
        size = pd.Series([5])
        result = scalar_registry.execute("lpad", [s, size])
        assert len(result) == 0

    def test_rpad_empty_series(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """rpad() on an empty Series should return an empty Series (line 362)."""
        s = pd.Series([], dtype=object)
        size = pd.Series([5])
        result = scalar_registry.execute("rpad", [s, size])
        assert len(result) == 0

    def test_lpad_basic(self, empty_star: Star) -> None:
        """lpad('Bob', 6, '*') should return '***Bob'."""
        result = empty_star.execute_query("RETURN lpad('Bob', 6, '*') AS r")
        assert result["r"].iloc[0] == "***Bob"

    def test_rpad_basic(self, empty_star: Star) -> None:
        """rpad('Bob', 6, '*') should return 'Bob***'."""
        result = empty_star.execute_query("RETURN rpad('Bob', 6, '*') AS r")
        assert result["r"].iloc[0] == "Bob***"


# ---------------------------------------------------------------------------
# Extended string functions -- char() with invalid code points
# ---------------------------------------------------------------------------


class TestCharFunction:
    """Cover char() invalid code point handling (lines 630-632)."""

    def test_char_basic(self, empty_star: Star) -> None:
        """char(65) should return 'A'."""
        result = empty_star.execute_query("RETURN char(65) AS r")
        assert result["r"].iloc[0] == "A"

    def test_char_invalid_negative(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """char() with a negative code point should return null (lines 630-632)."""
        s = pd.Series([-1])
        result = scalar_registry.execute("char", [s])
        assert result.iloc[0] is None

    def test_char_invalid_large(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """char() with an excessively large code point should return null (lines 630-632)."""
        # 0x110000 is beyond the valid Unicode range
        s = pd.Series([0x110000])
        result = scalar_registry.execute("char", [s])
        assert result.iloc[0] is None

    def test_char_null(self, scalar_registry: ScalarFunctionRegistry) -> None:
        """char(null) should return null."""
        s = pd.Series([None], dtype=object)
        result = scalar_registry.execute("char", [s])
        assert result.iloc[0] is None


# ---------------------------------------------------------------------------
# Extended string functions -- charCodeAt() basic operation
# ---------------------------------------------------------------------------


class TestCharCodeAt:
    """Cover charCodeAt() basic operation (lines 661-667)."""

    def test_char_code_at_basic(self, empty_star: Star) -> None:
        """charCodeAt('A', 0) should return 65."""
        result = empty_star.execute_query("RETURN charCodeAt('A', 0) AS r")
        assert result["r"].iloc[0] == 65

    def test_char_code_at_second_char(self, empty_star: Star) -> None:
        """charCodeAt('hello', 1) should return ord('e') = 101."""
        result = empty_star.execute_query("RETURN charCodeAt('hello', 1) AS r")
        assert result["r"].iloc[0] == 101

    def test_char_code_at_out_of_range(self, empty_star: Star) -> None:
        """charCodeAt('hi', 5) should return null (out of range)."""
        result = empty_star.execute_query("RETURN charCodeAt('hi', 5) AS r")
        assert result["r"].iloc[0] is None


# ---------------------------------------------------------------------------
# Extended string functions -- normalize() element-wise form
# ---------------------------------------------------------------------------


class TestNormalizeElementWise:
    """Cover normalize() element-wise form path (lines 806-809)."""

    def test_normalize_default_nfc(self, empty_star: Star) -> None:
        """normalize('hello') with default NFC should return 'hello'."""
        result = empty_star.execute_query("RETURN normalize('hello') AS r")
        assert result["r"].iloc[0] == "hello"

    def test_normalize_explicit_nfc(self, empty_star: Star) -> None:
        """normalize('hello', 'NFC') should return 'hello'."""
        result = empty_star.execute_query(
            "RETURN normalize('hello', 'NFC') AS r"
        )
        assert result["r"].iloc[0] == "hello"

    def test_normalize_element_wise_form(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """normalize() with multi-element form Series hits the element-wise path (lines 806-809)."""
        s = pd.Series(["hello", "world"])
        form = pd.Series(["NFC", "NFD"])
        result = scalar_registry.execute("normalize", [s, form])
        assert result.iloc[0] == "hello"
        assert result.iloc[1] == "world"


# ---------------------------------------------------------------------------
# Extended string functions -- byteSize() string dtype
# ---------------------------------------------------------------------------


class TestByteSizeStringDtype:
    """Cover byteSize() string-dtype fast path (lines 945-951)."""

    def test_byte_size_basic(self, empty_star: Star) -> None:
        """byteSize('hello') should return 5."""
        result = empty_star.execute_query("RETURN byteSize('hello') AS r")
        assert result["r"].iloc[0] == 5

    def test_byte_size_string_dtype(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """byteSize() on a StringDtype Series should use the fast path (lines 944-951)."""
        s = pd.Series(["hello", None, "cafe"], dtype="string")
        result = scalar_registry.execute("byteSize", [s])
        assert result.iloc[0] == 5
        assert result.iloc[1] is None
        assert result.iloc[2] == 4

    def test_byte_size_multibyte(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """byteSize on a multibyte character string returns correct UTF-8 byte count."""
        # e-acute in UTF-8 is 2 bytes; test via direct registry call to avoid
        # Cypher parser unicode-escape ambiguity.
        s = pd.Series(["caf\u00e9"], dtype=object)
        result = scalar_registry.execute("byteSize", [s])
        assert result.iloc[0] == 5  # c(1) + a(1) + f(1) + e-acute(2) = 5


# ---------------------------------------------------------------------------
# Utility functions -- coalesce() zero arguments
# ---------------------------------------------------------------------------


class TestCoalesceZeroArgs:
    """Cover coalesce() zero-argument error (lines 50-52)."""

    def test_coalesce_zero_args_raises(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """coalesce() with no arguments should raise an error."""
        from pycypher.exceptions import FunctionArgumentError

        with pytest.raises(FunctionArgumentError):
            scalar_registry.execute("coalesce", [])

    def test_coalesce_basic(self, empty_star: Star) -> None:
        """coalesce(null, 'fallback') should return 'fallback'."""
        result = empty_star.execute_query(
            "RETURN coalesce(null, 'fallback') AS r"
        )
        assert result["r"].iloc[0] == "fallback"

    def test_coalesce_first_non_null(self, empty_star: Star) -> None:
        """coalesce('first', 'second') should return 'first'."""
        result = empty_star.execute_query(
            "RETURN coalesce('first', 'second') AS r"
        )
        assert result["r"].iloc[0] == "first"


# ---------------------------------------------------------------------------
# Utility functions -- nullIf() comparing to null
# ---------------------------------------------------------------------------


class TestNullIfNullComparison:
    """Cover nullIf() null comparison path (line 130)."""

    def test_nullif_comparing_to_null(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """nullIf(x, null) should return x unchanged (line 128-130)."""
        v1 = pd.Series([1, 2, None], dtype=object)
        v2 = pd.Series([None], dtype=object)
        result = scalar_registry.execute("nullIf", [v1, v2])
        assert result.iloc[0] == 1
        assert result.iloc[1] == 2
        # The third element is already null, stays null
        assert result.iloc[2] is None or pd.isna(result.iloc[2])

    def test_nullif_basic(self, empty_star: Star) -> None:
        """nullIf(0, 0) should return null."""
        result = empty_star.execute_query("RETURN nullIf(0, 0) AS r")
        val = result["r"].iloc[0]
        assert val is None or (isinstance(val, float) and math.isnan(val))

    def test_nullif_no_match(self, empty_star: Star) -> None:
        """nullIf(1, 0) should return 1."""
        result = empty_star.execute_query("RETURN nullIf(1, 0) AS r")
        assert result["r"].iloc[0] == 1


# ---------------------------------------------------------------------------
# Utility functions -- isNaN()
# ---------------------------------------------------------------------------


class TestIsNaN:
    """Cover isNaN() NaN detection (line 179)."""

    def test_isnan_with_nan_value(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """isNaN() on a float NaN value should return True (line 178-179)."""
        s = pd.Series([float("nan"), 1.0, None], dtype=object)
        result = scalar_registry.execute("isNaN", [s])
        assert result.iloc[0] is True
        assert result.iloc[1] is False
        assert result.iloc[2] is None

    def test_isnan_integer(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """isNaN() on integer dtype should return all False (line 172)."""
        s = pd.Series([1, 2, 3], dtype="int64")
        result = scalar_registry.execute("isNaN", [s])
        assert all(v is False or v == False for v in result)  # noqa: E712

    def test_isnan_float_dtype(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """isNaN() on float64 dtype should use numpy fast path (line 169-170)."""
        s = pd.Series([float("nan"), 1.0, 2.0], dtype="float64")
        result = scalar_registry.execute("isNaN", [s])
        assert result.iloc[0] is True or result.iloc[0] == True  # noqa: E712
        assert result.iloc[1] is False or result.iloc[1] == False  # noqa: E712


# ---------------------------------------------------------------------------
# Utility functions -- isInfinite() and isFinite() with integer input
# ---------------------------------------------------------------------------


class TestIsInfiniteIsFinite:
    """Cover isInfinite/isFinite integer fast paths (lines 227, 266) and non-numeric (274-277)."""

    def test_is_infinite_integer(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """isInfinite() on integer dtype should return all False (line 227)."""
        s = pd.Series([1, 2, 3], dtype="int64")
        result = scalar_registry.execute("isInfinite", [s])
        assert all(v is False or v == False for v in result)  # noqa: E712

    def test_is_finite_integer(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """isFinite() on integer dtype should return all True (line 266)."""
        s = pd.Series([1, 2, 3], dtype="int64")
        result = scalar_registry.execute("isFinite", [s])
        assert all(v is True or v == True for v in result)  # noqa: E712

    def test_is_finite_non_numeric(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """isFinite() on non-numeric types returns False (lines 274-277)."""
        s = pd.Series(["hello", True, None], dtype=object)
        result = scalar_registry.execute("isFinite", [s])
        # "hello" is not numeric -> False
        assert result.iloc[0] is False
        # True is a bool (isinstance check excludes bool from int) -> False
        assert result.iloc[1] is False
        # None -> None
        assert result.iloc[2] is None

    def test_is_infinite_with_infinity(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """isInfinite() on +inf should return True."""
        s = pd.Series([float("inf")], dtype="float64")
        result = scalar_registry.execute("isInfinite", [s])
        assert result.iloc[0] is True or result.iloc[0] == True  # noqa: E712

    def test_is_finite_with_infinity(
        self, scalar_registry: ScalarFunctionRegistry
    ) -> None:
        """isFinite() on +inf should return False."""
        s = pd.Series([float("inf")], dtype="float64")
        result = scalar_registry.execute("isFinite", [s])
        assert result.iloc[0] is False or result.iloc[0] == False  # noqa: E712

    def test_is_finite_with_integer_via_query(self, empty_star: Star) -> None:
        """isFinite(42) should return true."""
        result = empty_star.execute_query("RETURN isFinite(42) AS r")
        val = result["r"].iloc[0]
        assert val is True or val == True  # noqa: E712
