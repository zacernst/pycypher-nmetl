"""TDD tests for hash and Base64 encoding functions (Loop 179).

Neo4j 4.4+ standard functions absent from ScalarFunctionRegistry:

    md5(string)         → 32-char lowercase hex MD5 digest
    sha1(string)        → 40-char lowercase hex SHA-1 digest
    sha256(string)      → 64-char lowercase hex SHA-256 digest
    encodeBase64(string)→ standard Base64-encoded string (no line breaks)
    decodeBase64(string)→ decoded UTF-8 string from Base64 input

Use cases that are currently blocked without these functions:
  - Deduplication: JOIN two datasets on md5(id_col) when IDs differ by case
  - Privacy: RETURN sha256(p.ssn) AS hashed_ssn (hash PII before export)
  - Encoding: Store binary blobs as base64; decode on retrieval
  - Data integrity: compare md5 checksums across systems

All tests are written before the implementation (TDD red phase).
"""

from __future__ import annotations

import base64
import hashlib

import pandas as pd
import pytest
from pycypher.scalar_functions import ScalarFunctionRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reg() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


def _s(*values: object) -> pd.Series:
    return pd.Series(list(values), dtype=object)


def _exec(name: str, *values: object) -> pd.Series:
    """Execute a scalar function with scalar/None inputs (wraps in list for API)."""
    return _reg().execute(name, [_s(*values)])


# ---------------------------------------------------------------------------
# Category 1 — Registration
# ---------------------------------------------------------------------------


class TestHashEncodingRegistration:
    """All five functions must be registered in the singleton registry."""

    def test_md5_is_registered(self) -> None:
        assert _reg().has_function("md5")

    def test_sha1_is_registered(self) -> None:
        assert _reg().has_function("sha1")

    def test_sha256_is_registered(self) -> None:
        assert _reg().has_function("sha256")

    def test_encodebase64_is_registered(self) -> None:
        assert _reg().has_function("encodeBase64")

    def test_decodebase64_is_registered(self) -> None:
        assert _reg().has_function("decodeBase64")


# ---------------------------------------------------------------------------
# Category 2 — md5: correctness
# ---------------------------------------------------------------------------


class TestMd5Correctness:
    """md5(string) must return a 32-char lowercase hex digest."""

    def test_md5_known_value(self) -> None:
        expected = hashlib.md5(b"hello").hexdigest()
        result = _exec("md5", "hello")
        assert result.iloc[0] == expected

    def test_md5_empty_string(self) -> None:
        expected = hashlib.md5(b"").hexdigest()
        result = _exec("md5", "")
        assert result.iloc[0] == expected

    def test_md5_returns_32_chars(self) -> None:
        result = _exec("md5", "abc")
        assert len(result.iloc[0]) == 32

    def test_md5_is_lowercase_hex(self) -> None:
        result = _exec("md5", "test")
        val = result.iloc[0]
        assert val == val.lower()
        assert all(c in "0123456789abcdef" for c in val)

    def test_md5_null_propagation(self) -> None:
        result = _exec("md5", None)
        assert result.iloc[0] is None or pd.isna(result.iloc[0])

    def test_md5_vectorized_series(self) -> None:
        inputs = _s("hello", "world", None, "")
        result = _reg().execute("md5", [inputs])
        assert result.iloc[0] == hashlib.md5(b"hello").hexdigest()
        assert result.iloc[1] == hashlib.md5(b"world").hexdigest()
        assert pd.isna(result.iloc[2])
        assert result.iloc[3] == hashlib.md5(b"").hexdigest()


# ---------------------------------------------------------------------------
# Category 3 — sha1: correctness
# ---------------------------------------------------------------------------


class TestSha1Correctness:
    """sha1(string) must return a 40-char lowercase hex digest."""

    def test_sha1_known_value(self) -> None:
        expected = hashlib.sha1(b"hello").hexdigest()
        result = _exec("sha1", "hello")
        assert result.iloc[0] == expected

    def test_sha1_returns_40_chars(self) -> None:
        result = _exec("sha1", "abc")
        assert len(result.iloc[0]) == 40

    def test_sha1_null_propagation(self) -> None:
        result = _exec("sha1", None)
        assert pd.isna(result.iloc[0])

    def test_sha1_different_from_md5(self) -> None:
        """Different algorithms must produce different digests."""
        md5_result = _exec("md5", "test")
        sha1_result = _exec("sha1", "test")
        assert md5_result.iloc[0] != sha1_result.iloc[0]

    def test_sha1_vectorized_series(self) -> None:
        inputs = _s("a", "b", None)
        result = _reg().execute("sha1", [inputs])
        assert result.iloc[0] == hashlib.sha1(b"a").hexdigest()
        assert result.iloc[1] == hashlib.sha1(b"b").hexdigest()
        assert pd.isna(result.iloc[2])


# ---------------------------------------------------------------------------
# Category 4 — sha256: correctness
# ---------------------------------------------------------------------------


class TestSha256Correctness:
    """sha256(string) must return a 64-char lowercase hex digest."""

    def test_sha256_known_value(self) -> None:
        expected = hashlib.sha256(b"hello").hexdigest()
        result = _exec("sha256", "hello")
        assert result.iloc[0] == expected

    def test_sha256_returns_64_chars(self) -> None:
        result = _exec("sha256", "abc")
        assert len(result.iloc[0]) == 64

    def test_sha256_null_propagation(self) -> None:
        result = _exec("sha256", None)
        assert pd.isna(result.iloc[0])

    def test_sha256_differs_from_sha1(self) -> None:
        sha1_result = _exec("sha1", "data")
        sha256_result = _exec("sha256", "data")
        assert sha1_result.iloc[0] != sha256_result.iloc[0]

    def test_sha256_vectorized_series(self) -> None:
        inputs = _s("x", None, "y")
        result = _reg().execute("sha256", [inputs])
        assert result.iloc[0] == hashlib.sha256(b"x").hexdigest()
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == hashlib.sha256(b"y").hexdigest()


# ---------------------------------------------------------------------------
# Category 5 — encodeBase64: correctness
# ---------------------------------------------------------------------------


class TestEncodeBase64Correctness:
    """encodeBase64(string) must return standard Base64 without newlines."""

    def test_encodebase64_known_value(self) -> None:
        expected = base64.b64encode(b"hello").decode("utf-8")
        result = _exec("encodeBase64", "hello")
        assert result.iloc[0] == expected

    def test_encodebase64_empty_string(self) -> None:
        expected = base64.b64encode(b"").decode("utf-8")
        result = _exec("encodeBase64", "")
        assert result.iloc[0] == expected

    def test_encodebase64_no_newlines(self) -> None:
        long_input = "a" * 100
        result = _exec("encodeBase64", long_input)
        assert "\n" not in result.iloc[0]
        assert "\r" not in result.iloc[0]

    def test_encodebase64_null_propagation(self) -> None:
        result = _exec("encodeBase64", None)
        assert pd.isna(result.iloc[0])

    def test_encodebase64_roundtrip_with_decode(self) -> None:
        """encodeBase64 followed by decodeBase64 must recover the original."""
        original = "Hello, World!"
        encoded = _exec("encodeBase64", original)
        decoded = _reg().execute("decodeBase64", [encoded])
        assert decoded.iloc[0] == original

    def test_encodebase64_vectorized_series(self) -> None:
        inputs = _s("a", "b", None)
        result = _reg().execute("encodeBase64", [inputs])
        assert result.iloc[0] == base64.b64encode(b"a").decode("utf-8")
        assert result.iloc[1] == base64.b64encode(b"b").decode("utf-8")
        assert pd.isna(result.iloc[2])


# ---------------------------------------------------------------------------
# Category 6 — decodeBase64: correctness
# ---------------------------------------------------------------------------


class TestDecodeBase64Correctness:
    """decodeBase64(string) must return the UTF-8-decoded plaintext."""

    def test_decodebase64_known_value(self) -> None:
        encoded = base64.b64encode(b"hello").decode("utf-8")
        result = _exec("decodeBase64", encoded)
        assert result.iloc[0] == "hello"

    def test_decodebase64_empty(self) -> None:
        encoded = base64.b64encode(b"").decode("utf-8")
        result = _exec("decodeBase64", encoded)
        assert result.iloc[0] == ""

    def test_decodebase64_null_propagation(self) -> None:
        result = _exec("decodeBase64", None)
        assert pd.isna(result.iloc[0])

    def test_decodebase64_vectorized_series(self) -> None:
        encoded_a = base64.b64encode(b"alpha").decode("utf-8")
        encoded_b = base64.b64encode(b"beta").decode("utf-8")
        inputs = _s(encoded_a, None, encoded_b)
        result = _reg().execute("decodeBase64", [inputs])
        assert result.iloc[0] == "alpha"
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == "beta"


# ---------------------------------------------------------------------------
# Category 7 — Cypher integration via Star.execute_query
# ---------------------------------------------------------------------------


class TestHashEncodingIntegration:
    """End-to-end Cypher query execution using the hash/encoding functions."""

    @pytest.fixture
    def star(self):
        import pandas as pd
        from pycypher.relational_models import (
            ID_COLUMN,
            Context,
            EntityMapping,
            EntityTable,
            RelationshipMapping,
        )
        from pycypher.star import Star

        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "ssn": ["123-45-6789", "987-65-4321", "111-22-3333"],
            },
        )
        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name", "ssn"],
            source_obj_attribute_map={"name": "name", "ssn": "ssn"},
            attribute_map={"name": "name", "ssn": "ssn"},
            source_obj=df,
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
        return Star(context=ctx)

    def test_md5_in_return_clause(self, star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN md5(p.ssn) AS hashed",
        )
        expected = hashlib.md5(b"123-45-6789").hexdigest()
        assert result["hashed"].iloc[0] == expected

    def test_sha256_in_where_clause(self, star) -> None:
        """Filter by SHA-256 hash of a property."""
        target_hash = hashlib.sha256(b"Bob").hexdigest()
        result = star.execute_query(
            f"MATCH (p:Person) WHERE sha256(p.name) = '{target_hash}' "
            "RETURN p.name AS name",
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "Bob"

    def test_encodebase64_in_return_clause(self, star) -> None:
        expected = base64.b64encode(b"Alice").decode("utf-8")
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN encodeBase64(p.name) AS encoded",
        )
        assert result["encoded"].iloc[0] == expected

    def test_encodebase64_decodebase64_roundtrip_in_query(self, star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Carol' "
            "RETURN decodeBase64(encodeBase64(p.name)) AS roundtrip",
        )
        assert result["roundtrip"].iloc[0] == "Carol"

    def test_sha256_all_rows_produces_unique_hashes(self, star) -> None:
        """Different names must hash to different SHA-256 digests."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN sha256(p.name) AS h ORDER BY p.name",
        )
        hashes = list(result["h"])
        assert len(set(hashes)) == 3  # all distinct
