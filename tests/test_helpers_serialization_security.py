"""TDD security tests for shared.helpers encode/decode.

Loop 215 — Security: shared/helpers.py exposes encode() and decode() that are
implemented with pickle.dumps() / pickle.loads().  pickle.loads() can execute
arbitrary Python code during deserialization — a well-known RCE vector.

The functions are currently unused in the pycypher package, but they are part of
the public shared API and could be called with untrusted input by downstream code.

The fix is to replace pickle with JSON-based serialization.  JSON cannot execute
code during parsing and is the correct tool for serializing simple Python values
(dicts, lists, strings, numbers, booleans, None).

Red-phase tests that fail with the current pickle implementation:
  - test_encode_produces_json_decodable_bytes: pickle bytes are NOT valid JSON
  - test_decode_rejects_pickle_payload: current decode() happily loads pickle
  - test_encode_non_serializable_raises: current impl silently encodes complex types

Green-phase behaviour (after JSON replacement):
  - Roundtrip works for all JSON-compatible types
  - decode() of a pickle payload raises ValueError (not executes)
  - encode() of a non-JSON type raises ValueError with descriptive message
"""

from __future__ import annotations

import base64
import json
import pickle

import pytest
from shared.helpers import decode, encode

# ---------------------------------------------------------------------------
# Category 1 — Output format: encode() must produce JSON-decodable bytes
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.slow


class TestEncodeProducesJson:
    def test_encode_simple_dict_produces_json(self) -> None:
        """base64-decoding encode(dict) must yield valid JSON, not pickle bytes."""
        encoded = encode({"key": "value", "num": 42})
        raw_bytes = base64.b64decode(encoded)
        # This fails with the pickle implementation: pickle bytes are not JSON
        parsed = json.loads(raw_bytes)
        assert parsed == {"key": "value", "num": 42}

    def test_encode_list_produces_json(self) -> None:
        """base64-decoding encode(list) must yield valid JSON."""
        encoded = encode([1, 2, 3])
        raw_bytes = base64.b64decode(encoded)
        assert json.loads(raw_bytes) == [1, 2, 3]

    def test_encode_string_produces_json(self) -> None:
        """base64-decoding encode(str) must yield valid JSON."""
        encoded = encode("hello world")
        raw_bytes = base64.b64decode(encoded)
        assert json.loads(raw_bytes) == "hello world"

    def test_encode_none_produces_json(self) -> None:
        """base64-decoding encode(None) must yield valid JSON 'null'."""
        encoded = encode(None)
        raw_bytes = base64.b64decode(encoded)
        assert json.loads(raw_bytes) is None

    def test_encode_integer_produces_json(self) -> None:
        """base64-decoding encode(int) must yield valid JSON."""
        encoded = encode(99)
        raw_bytes = base64.b64decode(encoded)
        assert json.loads(raw_bytes) == 99


# ---------------------------------------------------------------------------
# Category 2 — decode() must reject pickle payloads
# ---------------------------------------------------------------------------


class TestDecodeRejectsPickle:
    def test_decode_rejects_pickle_payload(self) -> None:
        """decode() must raise ValueError when given a base64-encoded pickle payload.

        With the current pickle implementation, this test FAILS because decode()
        happily deserializes the pickle payload.  After the JSON fix, it raises
        ValueError because the pickle bytes are not valid JSON.
        """
        # Encode a harmless object as pickle to create a pickle payload
        pickle_payload = base64.b64encode(
            pickle.dumps({"key": "value"})
        ).decode()
        with pytest.raises(ValueError):
            decode(pickle_payload)

    def test_decode_rejects_arbitrary_bytes(self) -> None:
        """decode() must raise ValueError for any non-JSON base64 input."""
        garbage = base64.b64encode(b"\x80\x04\x95malicious").decode()
        with pytest.raises(ValueError):
            decode(garbage)


# ---------------------------------------------------------------------------
# Category 3 — encode()/decode() roundtrip correctness
# ---------------------------------------------------------------------------


class TestRoundtrip:
    def test_roundtrip_dict(self) -> None:
        """Encoding then decoding a dict must reproduce the original value."""
        original = {"name": "Alice", "age": 30, "active": True}
        assert decode(encode(original)) == original

    def test_roundtrip_list(self) -> None:
        """Encoding then decoding a list must reproduce the original value."""
        original = [1, "two", 3.0, None, False]
        assert decode(encode(original)) == original

    def test_roundtrip_nested(self) -> None:
        """Encoding then decoding a nested structure must reproduce it."""
        original = {"data": [{"id": 1}, {"id": 2}], "meta": None}
        assert decode(encode(original)) == original

    def test_roundtrip_empty_dict(self) -> None:
        """Empty dict roundtrip."""
        assert decode(encode({})) == {}

    def test_roundtrip_empty_list(self) -> None:
        """Empty list roundtrip."""
        assert decode(encode([])) == []


# ---------------------------------------------------------------------------
# Category 4 — encode() must refuse non-JSON-serializable types
# ---------------------------------------------------------------------------


class TestEncodeRefusesNonJsonTypes:
    def test_encode_set_raises_value_error(self) -> None:
        """encode({1, 2, 3}) must raise ValueError: sets are not JSON-serializable."""
        with pytest.raises(ValueError):
            encode({1, 2, 3})

    def test_encode_custom_object_raises_value_error(self) -> None:
        """encode() of an arbitrary Python object must raise ValueError."""

        class CustomObj:
            pass

        with pytest.raises(ValueError):
            encode(CustomObj())

    def test_error_message_is_descriptive(self) -> None:
        """ValueError message from a failed encode should describe the failure."""
        with pytest.raises(ValueError, match="encoding"):
            encode({1, 2, 3})


# ---------------------------------------------------------------------------
# Category 5 — to_bytes parameter
# ---------------------------------------------------------------------------


class TestToBytes:
    def test_encode_returns_string_by_default(self) -> None:
        """encode() returns a str by default."""
        result = encode({"x": 1})
        assert isinstance(result, str)

    def test_encode_returns_bytes_when_requested(self) -> None:
        """encode(to_bytes=True) returns bytes."""
        result = encode({"x": 1}, to_bytes=True)
        assert isinstance(result, bytes)

    def test_to_bytes_and_string_roundtrip_both_work(self) -> None:
        """Both bytes and string encodings must decode correctly."""
        original = {"x": 1}
        str_encoded = encode(original, to_bytes=False)
        bytes_encoded = encode(original, to_bytes=True)
        assert decode(str_encoded) == original
        assert decode(bytes_encoded.decode()) == original
