"""Tests for shared helper utilities (encode/decode, ensure_uri, ensure_bytes)."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import ParseResult, urlparse

import pytest

from shared.helpers import decode, encode, ensure_bytes, ensure_uri


# ---------------------------------------------------------------------------
# ensure_uri
# ---------------------------------------------------------------------------


class TestEnsureUri:
    """Tests for ensure_uri()."""

    def test_string_input(self) -> None:
        """String is parsed into a ParseResult."""
        result = ensure_uri("http://example.com/path")
        assert isinstance(result, ParseResult)
        assert result.scheme == "http"
        assert result.netloc == "example.com"
        assert result.path == "/path"

    def test_parse_result_passthrough(self) -> None:
        """ParseResult is returned unchanged."""
        original = urlparse("https://example.com")
        result = ensure_uri(original)
        assert result is original

    def test_path_input(self) -> None:
        """pathlib.Path is converted to a file URI."""
        result = ensure_uri(Path("/tmp/test.txt"))
        assert isinstance(result, ParseResult)
        assert result.scheme == "file"
        assert "/tmp/test.txt" in result.path

    def test_unsupported_type_raises(self) -> None:
        """Non-string/ParseResult/Path raises ValueError."""
        with pytest.raises(ValueError, match="URI must be a string"):
            ensure_uri(42)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# encode / decode round-trip
# ---------------------------------------------------------------------------


class TestEncodeDecode:
    """Tests for encode() and decode()."""

    def test_roundtrip_dict(self) -> None:
        """A dict survives encode → decode."""
        obj = {"key": "value", "n": 42}
        encoded = encode(obj)
        assert isinstance(encoded, str)
        decoded = decode(encoded)
        assert decoded == obj

    def test_roundtrip_list(self) -> None:
        """A list survives encode → decode."""
        obj = [1, 2, "three"]
        assert decode(encode(obj)) == obj

    def test_encode_to_bytes(self) -> None:
        """encode(to_bytes=True) returns bytes."""
        result = encode("hello", to_bytes=True)
        assert isinstance(result, bytes)

    def test_encode_to_string_default(self) -> None:
        """encode() returns str by default."""
        result = encode("hello")
        assert isinstance(result, str)

    def test_decode_invalid_data_raises(self) -> None:
        """decode() raises ValueError on garbage input."""
        with pytest.raises(ValueError, match="Error decoding"):
            decode("not-valid-base64!!!")

    def test_encode_unpickleable_raises(self) -> None:
        """encode() raises ValueError for unpickleable objects."""
        with pytest.raises(ValueError, match="Error encoding"):
            encode(lambda x: x)  # lambdas can't be pickled


# ---------------------------------------------------------------------------
# ensure_bytes
# ---------------------------------------------------------------------------


class TestEnsureBytes:
    """Tests for ensure_bytes()."""

    def test_none_returns_none(self) -> None:
        """None input returns None."""
        assert ensure_bytes(None) is None

    def test_bytes_passthrough(self) -> None:
        """bytes input is returned unchanged."""
        b = b"hello"
        assert ensure_bytes(b) is b

    def test_string_encoded(self) -> None:
        """String input is encoded to bytes."""
        result = ensure_bytes("hello")
        assert result == b"hello"

    def test_string_with_encoding(self) -> None:
        """Custom encoding kwarg is forwarded."""
        result = ensure_bytes("café", encoding="utf-8")
        assert isinstance(result, bytes)
        assert result == "café".encode("utf-8")
