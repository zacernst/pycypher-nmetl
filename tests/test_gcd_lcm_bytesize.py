"""TDD tests for gcd(), lcm(), and byteSize() — Neo4j 5.x scalar functions.

Problem: Neo4j 5.0 added three scalar functions that are completely absent
from pycypher's ScalarFunctionRegistry:

  gcd(integer, integer) → integer
    Returns the greatest common divisor of two integers.
    gcd(12, 8) → 4
    gcd(0, 5) → 5
    gcd(null, 5) → null
    gcd(-12, 8) → 4  (result is always non-negative)

  lcm(integer, integer) → integer
    Returns the least common multiple of two integers.
    lcm(4, 6) → 12
    lcm(0, 5) → 0
    lcm(null, 5) → null
    lcm(-4, 6) → 12  (result is always non-negative)

  byteSize(string) → integer
    Returns the number of bytes the string occupies in UTF-8 encoding.
    byteSize('hello') → 5   (pure ASCII: 1 byte per char)
    byteSize('café') → 5    (é is 2 bytes in UTF-8)
    byteSize('') → 0
    byteSize(null) → null

All tests are written before the implementation (TDD red phase).
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def ctx() -> ContextBuilder:
    return ContextBuilder().from_dict(
        {
            "Pair": pd.DataFrame(
                {
                    "__ID__": ["p1", "p2", "p3", "p4"],
                    "a": [12, 0, 15, 100],
                    "b": [8, 5, 25, 75],
                },
            ),
            "Word": pd.DataFrame(
                {
                    "__ID__": ["w1", "w2", "w3", "w4"],
                    "text": ["hello", "café", "", "日本語"],
                },
            ),
        },
    )


@pytest.fixture(scope="module")
def star(ctx: ContextBuilder) -> Star:
    return Star(context=ctx)


@pytest.fixture(scope="module")
def registry() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


# ---------------------------------------------------------------------------
# Category 1 — Registration
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_gcd_is_registered(self, registry: ScalarFunctionRegistry) -> None:
        """Gcd must be in the scalar function registry."""
        assert registry.has_function("gcd"), (
            "gcd() is not registered. "
            "It is a standard Neo4j 5.x function (available since Neo4j 5.0)."
        )

    def test_lcm_is_registered(self, registry: ScalarFunctionRegistry) -> None:
        """Lcm must be in the scalar function registry."""
        assert registry.has_function("lcm"), (
            "lcm() is not registered. "
            "It is a standard Neo4j 5.x function (available since Neo4j 5.0)."
        )

    def test_bytesize_is_registered(
        self,
        registry: ScalarFunctionRegistry,
    ) -> None:
        """ByteSize must be in the scalar function registry."""
        assert registry.has_function("byteSize"), (
            "byteSize() is not registered. "
            "It is a standard Neo4j 5.x function (available since Neo4j 5.0)."
        )


# ---------------------------------------------------------------------------
# Category 2 — gcd() correctness
# ---------------------------------------------------------------------------


class TestGcd:
    def test_gcd_basic(self, star: Star) -> None:
        """gcd(12, 8) = 4."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 12 AND p.b = 8 RETURN gcd(p.a, p.b) AS g",
        )
        assert r["g"].iloc[0] == 4

    def test_gcd_zero_first_arg(self, star: Star) -> None:
        """gcd(0, 5) = 5."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 0 AND p.b = 5 RETURN gcd(p.a, p.b) AS g",
        )
        assert r["g"].iloc[0] == 5

    def test_gcd_coprime(self, star: Star) -> None:
        """Gcd of two coprime integers = 1."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 100 AND p.b = 75 "
            "RETURN gcd(p.a + 1, p.b + 1) AS g",
        )
        # 101 and 76 share no common factors; gcd = 1
        assert r["g"].iloc[0] == 1

    def test_gcd_identical(self, star: Star) -> None:
        """gcd(n, n) = n."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 12 RETURN gcd(p.a, p.a) AS g",
        )
        assert r["g"].iloc[0] == 12

    def test_gcd_with_integer_literals(self, star: Star) -> None:
        """Gcd works on literal integer arguments."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 12 RETURN gcd(36, 48) AS g",
        )
        assert r["g"].iloc[0] == 12

    def test_gcd_null_propagation(self, star: Star) -> None:
        """gcd(null, 5) returns null (null propagation)."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 12 RETURN gcd(null, 5) AS g",
        )
        assert r["g"].iloc[0] is None or pd.isna(r["g"].iloc[0])

    def test_gcd_result_non_negative(self, star: Star) -> None:
        """Gcd result is always >= 0 (matches Neo4j behaviour for negatives)."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 15 AND p.b = 25 RETURN gcd(p.a, p.b) AS g",
        )
        assert r["g"].iloc[0] == 5
        assert r["g"].iloc[0] >= 0

    def test_gcd_vectorised(self, star: Star) -> None:
        """Gcd returns one result per row — must not truncate to 1 row."""
        r = star.execute_query("MATCH (p:Pair) RETURN gcd(p.a, p.b) AS g")
        assert len(r) == 4, f"Expected 4 rows, got {len(r)}"


# ---------------------------------------------------------------------------
# Category 3 — lcm() correctness
# ---------------------------------------------------------------------------


class TestLcm:
    def test_lcm_basic(self, star: Star) -> None:
        """lcm(4, 6) = 12."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 12 RETURN lcm(4, 6) AS l",
        )
        assert r["l"].iloc[0] == 12

    def test_lcm_zero(self, star: Star) -> None:
        """lcm(0, 5) = 0 (any number × 0 = 0)."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 0 AND p.b = 5 RETURN lcm(p.a, p.b) AS l",
        )
        assert r["l"].iloc[0] == 0

    def test_lcm_same_number(self, star: Star) -> None:
        """lcm(n, n) = n."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 12 RETURN lcm(p.a, p.a) AS l",
        )
        assert r["l"].iloc[0] == 12

    def test_lcm_coprime(self, star: Star) -> None:
        """Lcm of coprime numbers = their product."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 12 RETURN lcm(7, 11) AS l",
        )
        assert r["l"].iloc[0] == 77  # 7 and 11 are coprime, lcm = 7 * 11

    def test_lcm_with_column_args(self, star: Star) -> None:
        """Lcm works on column values from MATCH."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 12 AND p.b = 8 RETURN lcm(p.a, p.b) AS l",
        )
        assert r["l"].iloc[0] == 24  # lcm(12, 8) = 24

    def test_lcm_null_propagation(self, star: Star) -> None:
        """lcm(null, 5) returns null."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 12 RETURN lcm(null, 5) AS l",
        )
        assert r["l"].iloc[0] is None or pd.isna(r["l"].iloc[0])

    def test_lcm_result_non_negative(self, star: Star) -> None:
        """Lcm result is always >= 0."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 15 AND p.b = 25 RETURN lcm(p.a, p.b) AS l",
        )
        assert r["l"].iloc[0] == 75  # lcm(15, 25) = 75
        assert r["l"].iloc[0] >= 0

    def test_lcm_vectorised(self, star: Star) -> None:
        """Lcm returns one result per row."""
        r = star.execute_query("MATCH (p:Pair) RETURN lcm(p.a, p.b) AS l")
        assert len(r) == 4, f"Expected 4 rows, got {len(r)}"


# ---------------------------------------------------------------------------
# Category 4 — byteSize() correctness
# ---------------------------------------------------------------------------


class TestByteSize:
    def test_ascii_string(self, star: Star) -> None:
        """byteSize('hello') = 5 (pure ASCII, 1 byte/char)."""
        r = star.execute_query(
            "MATCH (w:Word) WHERE w.text = 'hello' RETURN byteSize(w.text) AS sz",
        )
        assert r["sz"].iloc[0] == 5

    def test_multibyte_char(self, star: Star) -> None:
        """byteSize('café') = 5 (é is 2 bytes in UTF-8)."""
        r = star.execute_query(
            "MATCH (w:Word) WHERE w.text = 'café' RETURN byteSize(w.text) AS sz",
        )
        assert r["sz"].iloc[0] == 5

    def test_empty_string(self, star: Star) -> None:
        """byteSize('') = 0."""
        r = star.execute_query(
            "MATCH (w:Word) WHERE w.text = '' RETURN byteSize(w.text) AS sz",
        )
        assert r["sz"].iloc[0] == 0

    def test_cjk_characters(self, star: Star) -> None:
        """byteSize('日本語') = 9 (each CJK char is 3 bytes in UTF-8)."""
        r = star.execute_query(
            "MATCH (w:Word) WHERE w.text = '日本語' RETURN byteSize(w.text) AS sz",
        )
        assert r["sz"].iloc[0] == 9

    def test_null_propagation(self, star: Star) -> None:
        """byteSize(null) returns null."""
        r = star.execute_query(
            "MATCH (w:Word) WHERE w.text = 'hello' RETURN byteSize(null) AS sz",
        )
        assert r["sz"].iloc[0] is None or pd.isna(r["sz"].iloc[0])

    def test_literal_string(self, star: Star) -> None:
        """ByteSize works on a literal string argument."""
        r = star.execute_query(
            "MATCH (w:Word) WHERE w.text = 'hello' RETURN byteSize('abc') AS sz",
        )
        assert r["sz"].iloc[0] == 3

    def test_vectorised(self, star: Star) -> None:
        """ByteSize returns one result per row."""
        r = star.execute_query("MATCH (w:Word) RETURN byteSize(w.text) AS sz")
        assert len(r) == 4, f"Expected 4 rows, got {len(r)}"

    def test_integer_input_null(self, star: Star) -> None:
        """ByteSize on a non-string input returns null (not a crash)."""
        r = star.execute_query(
            "MATCH (p:Pair) WHERE p.a = 12 RETURN byteSize(p.a) AS sz LIMIT 1",
        )
        # Non-string inputs should return null per Neo4j semantics
        assert r["sz"].iloc[0] is None or pd.isna(r["sz"].iloc[0])
