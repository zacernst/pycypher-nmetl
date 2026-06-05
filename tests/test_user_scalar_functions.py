"""Tests for user-friendly scalar function registration.

Covers:
- Arity inference from a Python signature.
- Row-wise wrapper applies a scalar function across a Series.
- Null propagation: any null input → null output without invoking the function.
- ``register_user_function`` end-to-end against the registry.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.scalar_functions.user_functions import (
    _infer_arity,
    _wrap_row_wise,
    register_user_function,
)


@pytest.fixture(autouse=True)
def _reset_registry_singleton() -> None:
    """Each test starts with a freshly-built registry to avoid name collisions."""
    ScalarFunctionRegistry._instance = None
    yield
    ScalarFunctionRegistry._instance = None


class TestInferArity:
    def test_single_required_arg(self) -> None:
        def f(x):
            return x

        assert _infer_arity(f) == (1, 1)

    def test_required_plus_optional(self) -> None:
        def f(x, y=10):
            return x + y

        assert _infer_arity(f) == (1, 2)

    def test_varargs_is_unbounded(self) -> None:
        def f(x, *args):
            return x

        assert _infer_arity(f) == (1, None)

    def test_keyword_only_ignored(self) -> None:
        def f(x, *, mode="up"):
            return x

        assert _infer_arity(f) == (1, 1)

    def test_kwargs_ignored(self) -> None:
        def f(x, **kw):
            return x

        assert _infer_arity(f) == (1, 1)


class TestWrapRowWise:
    def test_applies_function_per_row(self) -> None:
        def double(x: int) -> int:
            return x * 2

        wrapped = _wrap_row_wise(double)
        result = wrapped(pd.Series([1, 2, 3]))
        assert list(result) == [2, 4, 6]

    def test_two_arg_function(self) -> None:
        def add(a, b):
            return a + b

        wrapped = _wrap_row_wise(add)
        result = wrapped(pd.Series([1, 2, 3]), pd.Series([10, 20, 30]))
        assert list(result) == [11, 22, 33]

    def test_propagates_none(self) -> None:
        def double(x):
            return x * 2

        wrapped = _wrap_row_wise(double)
        result = wrapped(pd.Series([1, None, 3], dtype=object))
        assert result.iloc[0] == 2
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == 6

    def test_propagates_nan(self) -> None:
        def double(x):
            return x * 2

        wrapped = _wrap_row_wise(double)
        result = wrapped(pd.Series([1.0, math.nan, 3.0]))
        assert result.iloc[0] == 2.0
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == 6.0

    def test_null_in_any_argument_propagates(self) -> None:
        def add(a, b):
            return a + b

        wrapped = _wrap_row_wise(add)
        result = wrapped(
            pd.Series([1, 2, 3]),
            pd.Series([10, None, 30], dtype=object),
        )
        assert result.iloc[0] == 11
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == 33

    def test_function_never_called_for_null_rows(self) -> None:
        # Tracks whether the user function gets called on null inputs — it
        # must not, otherwise the null contract is broken.
        calls: list[object] = []

        def record(x):
            calls.append(x)
            return x

        wrapped = _wrap_row_wise(record)
        wrapped(pd.Series([1, None, 3], dtype=object))
        assert calls == [1, 3]

    def test_preserves_index(self) -> None:
        def double(x):
            return x * 2

        wrapped = _wrap_row_wise(double)
        s = pd.Series([1, 2, 3], index=["a", "b", "c"])
        result = wrapped(s)
        assert list(result.index) == ["a", "b", "c"]


class TestRegisterUserFunction:
    def test_registers_with_inferred_metadata(self) -> None:
        def my_double(x):
            """Double the value."""
            return x * 2

        register_user_function(my_double)
        registry = ScalarFunctionRegistry.get_instance()
        assert registry.has_function("my_double")
        # Case-insensitive lookup.
        assert registry.has_function("MY_DOUBLE")

    def test_executes_through_registry(self) -> None:
        def triple(x):
            return x * 3

        register_user_function(triple)
        registry = ScalarFunctionRegistry.get_instance()
        result = registry.execute("triple", [pd.Series([1, 2, 3])])
        assert list(result) == [3, 6, 9]

    def test_arity_validation_via_registry(self) -> None:
        def needs_two(a, b):
            return a + b

        register_user_function(needs_two)
        registry = ScalarFunctionRegistry.get_instance()
        from pycypher.exceptions import FunctionArgumentError

        with pytest.raises(FunctionArgumentError):
            registry.execute("needs_two", [pd.Series([1, 2])])

    def test_name_override(self) -> None:
        def fips_state(s):
            """Return the leading 2 chars."""
            return s[:2]

        register_user_function(fips_state, name="fipsState")
        registry = ScalarFunctionRegistry.get_instance()
        assert registry.has_function("fipsState")
        result = registry.execute("fipsState", [pd.Series(["13001", "13321"])])
        assert list(result) == ["13", "13"]

    def test_zero_arg_function_rejected(self) -> None:
        def takes_none():
            return 0

        with pytest.raises(TypeError, match="at least one positional argument"):
            register_user_function(takes_none)


class TestRowExceptionHandling:
    """Per-row exception logging + null-fallback behavior."""

    def test_exception_replaced_with_none_other_rows_unaffected(self) -> None:
        def parse_int(s):
            return int(s)  # Raises ValueError on non-numeric strings.

        wrapped = _wrap_row_wise(parse_int)
        result = wrapped(pd.Series(["10", "not_a_number", "20"]))
        assert result.iloc[0] == 10
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == 20

    def test_warning_logged_per_failing_row(self, caplog) -> None:
        import logging

        def boom(x):
            raise ValueError(f"bad value {x}")

        wrapped = _wrap_row_wise(boom)
        with caplog.at_level(logging.WARNING):
            wrapped(pd.Series([1, 2]))

        # Per-row warnings (2) + summary line (1).
        warnings_text = "\n".join(
            r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING
        )
        assert "boom" in warnings_text
        assert "ValueError" in warnings_text
        assert "bad value 1" in warnings_text
        assert "bad value 2" in warnings_text
        # Summary count appears.
        assert "2 / 2" in warnings_text

    def test_per_row_logs_capped_with_summary(self, caplog) -> None:
        import logging

        def always_fails(_x):
            raise RuntimeError("nope")

        wrapped = _wrap_row_wise(always_fails)
        with caplog.at_level(logging.WARNING):
            # 10 failing rows; per-row logs should be capped at 3.
            wrapped(pd.Series(list(range(10))))

        per_row = [
            r for r in caplog.records
            if "failed on row" in r.getMessage()
        ]
        assert len(per_row) == 3
        # A "further per-row failures will be suppressed" line.
        suppressed = [
            r for r in caplog.records
            if "suppressed" in r.getMessage()
        ]
        assert len(suppressed) == 1
        # And a summary line.
        summary = [
            r for r in caplog.records
            if "10 / 10" in r.getMessage()
        ]
        assert len(summary) == 1

    def test_no_warning_when_all_rows_succeed(self, caplog) -> None:
        import logging

        def double(x):
            return x * 2

        wrapped = _wrap_row_wise(double)
        with caplog.at_level(logging.WARNING):
            wrapped(pd.Series([1, 2, 3]))

        assert not any(
            "failed on row" in r.getMessage() or "row(s) raised" in r.getMessage()
            for r in caplog.records
        )
