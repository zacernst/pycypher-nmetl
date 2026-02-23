"""Tests for PyCypher custom exception classes."""

from __future__ import annotations

from pycypher.exceptions import InvalidCastError, WrongCypherTypeError


class TestWrongCypherTypeError:
    """Tests for WrongCypherTypeError."""

    def test_message_attribute(self) -> None:
        """Exception stores the message attribute."""
        exc = WrongCypherTypeError("expected int, got string")
        assert exc.message == "expected int, got string"

    def test_str_representation(self) -> None:
        """str() of the exception returns the message."""
        exc = WrongCypherTypeError("type mismatch")
        assert str(exc) == "type mismatch"

    def test_is_exception(self) -> None:
        """Can be raised and caught as Exception."""
        with __import__("pytest").raises(WrongCypherTypeError):
            raise WrongCypherTypeError("bad type")


class TestInvalidCastError:
    """Tests for InvalidCastError."""

    def test_message_attribute(self) -> None:
        """Exception stores the message attribute."""
        exc = InvalidCastError("cannot cast string to int")
        assert exc.message == "cannot cast string to int"

    def test_str_representation(self) -> None:
        """str() of the exception returns the message."""
        exc = InvalidCastError("cast failed")
        assert str(exc) == "cast failed"

    def test_is_exception(self) -> None:
        """Can be raised and caught as Exception."""
        with __import__("pytest").raises(InvalidCastError):
            raise InvalidCastError("bad cast")
