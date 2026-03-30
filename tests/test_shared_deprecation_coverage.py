"""Tests for shared.deprecation module.

Covers _build_message, emit_deprecation, and deprecated decorator — targeting 51% → 95%+ coverage.
"""

from __future__ import annotations

import warnings

from shared.deprecation import _build_message, deprecated, emit_deprecation


class TestBuildMessage:
    """Tests for _build_message helper."""

    def test_minimal(self) -> None:
        msg = _build_message("old_func", since="1.0")
        assert "old_func is deprecated since v1.0" in msg

    def test_with_removed_in(self) -> None:
        msg = _build_message("old_func", since="1.0", removed_in="2.0")
        assert "will be removed in v2.0" in msg

    def test_with_alternative(self) -> None:
        msg = _build_message("old_func", since="1.0", alternative="new_func")
        assert "Use new_func instead" in msg

    def test_with_detail(self) -> None:
        msg = _build_message("old_func", since="1.0", detail="See docs.")
        assert "See docs." in msg

    def test_full_message(self) -> None:
        msg = _build_message(
            "OldClass",
            since="0.0.19",
            removed_in="0.1.0",
            alternative="NewClass",
            detail="Import via: from pkg import NewClass",
        )
        assert "OldClass is deprecated since v0.0.19" in msg
        assert "will be removed in v0.1.0" in msg
        assert "Use NewClass instead" in msg
        assert "Import via" in msg

    def test_no_optional_fields(self) -> None:
        msg = _build_message("func", since="1.0")
        assert msg == "func is deprecated since v1.0."


class TestEmitDeprecation:
    """Tests for emit_deprecation function."""

    def test_emits_deprecation_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            emit_deprecation("old_thing", since="1.0")
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "old_thing" in str(w[0].message)

    def test_includes_alternative(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            emit_deprecation("old", since="1.0", alternative="new")
            assert "Use new instead" in str(w[0].message)


class TestDeprecatedDecorator:
    """Tests for @deprecated decorator."""

    def test_function_deprecation(self) -> None:
        @deprecated(since="1.0", removed_in="2.0", alternative="new_func")
        def old_func(x: int) -> int:
            return x + 1

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = old_func(5)
            assert result == 6
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "old_func" in str(w[0].message)
            assert "v1.0" in str(w[0].message)

    def test_class_deprecation(self) -> None:
        @deprecated(since="0.5", removed_in="1.0", alternative="NewClass")
        class OldClass:
            def __init__(self, val: int) -> None:
                self.val = val

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            obj = OldClass(42)
            assert obj.val == 42
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "OldClass" in str(w[0].message)

    def test_preserves_function_metadata(self) -> None:
        @deprecated(since="1.0")
        def documented_func() -> None:
            """This is the docstring."""

        assert documented_func.__name__ == "documented_func"
        assert documented_func.__doc__ == "This is the docstring."

    def test_function_with_kwargs(self) -> None:
        @deprecated(since="1.0")
        def func_with_kwargs(a: int, b: str = "default") -> str:
            return f"{a}-{b}"

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            assert func_with_kwargs(1, b="test") == "1-test"
