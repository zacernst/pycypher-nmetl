"""TDD tests for Neo4j 5.x bitwise scalar functions.

Neo4j 5.x provides six bitwise functions for integer manipulation:
  bitAnd(x, y)         → x & y
  bitOr(x, y)          → x | y
  bitXor(x, y)         → x ^ y
  bitNot(x)            → ~x (inverts all bits; same as -(x+1) for two's complement)
  bitShiftLeft(x, y)   → x << y  (arithmetic left shift)
  bitShiftRight(x, y)  → x >> y  (arithmetic right shift, sign-extending)

All functions:
  - Accept integer arguments (int or numpy integer).
  - Return null if any argument is null.
  - Are vectorised using numpy bitwise operations (no per-row Python loops).
  - Are registered in ScalarFunctionRegistry and callable from Cypher.

Reference: https://neo4j.com/docs/cypher-manual/current/functions/mathematical-numeric/
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star


@pytest.fixture(scope="module")
def registry() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


@pytest.fixture(scope="module")
def bits_ctx() -> ContextBuilder:
    """Small context with integer flag columns for integration tests."""
    return ContextBuilder().from_dict(
        {
            "Item": pd.DataFrame(
                {
                    "__ID__": ["i1", "i2", "i3", "i4"],
                    "flags": [0b1010, 0b1100, 0b0110, 0b1111],
                    "shift": [1, 2, 3, 4],
                    "val": [12, 7, 0, -1],
                }
            )
        }
    )


# ---------------------------------------------------------------------------
# Registration tests
# ---------------------------------------------------------------------------


class TestBitwiseFunctionsRegistered:
    def test_bitand_registered(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("bitAnd")

    def test_bitor_registered(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("bitOr")

    def test_bitxor_registered(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("bitXor")

    def test_bitnot_registered(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("bitNot")

    def test_bitshiftleft_registered(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        assert registry.has_function("bitShiftLeft")

    def test_bitshiftright_registered(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        assert registry.has_function("bitShiftRight")


# ---------------------------------------------------------------------------
# bitAnd
# ---------------------------------------------------------------------------


class TestBitAnd:
    def test_basic_and(self) -> None:
        s = pd.Series([0b1010, 0b1100], dtype=object)
        mask = pd.Series([0b1100, 0b1100], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitAnd", [s, mask])
        assert r.tolist() == [0b1000, 0b1100]

    def test_and_with_zero(self) -> None:
        s = pd.Series([255], dtype=object)
        z = pd.Series([0], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitAnd", [s, z])
        assert r.iloc[0] == 0

    def test_and_with_all_ones(self) -> None:
        s = pd.Series([0b1010], dtype=object)
        ones = pd.Series([0b1111], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitAnd", [s, ones])
        assert r.iloc[0] == 0b1010

    def test_null_first_arg_returns_null(self) -> None:
        s = pd.Series([None], dtype=object)
        y = pd.Series([5], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitAnd", [s, y])
        assert r.iloc[0] is None

    def test_null_second_arg_returns_null(self) -> None:
        s = pd.Series([5], dtype=object)
        y = pd.Series([None], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitAnd", [s, y])
        assert r.iloc[0] is None

    def test_cypher_integration(self, bits_ctx: ContextBuilder) -> None:
        s = Star(context=bits_ctx)
        # 0b1010 & 0b1100 = 0b1000 = 8; 0b1100 & 0b1100 = 0b1100 = 12;
        # 0b0110 & 0b1100 = 0b0100 = 4; 0b1111 & 0b1100 = 0b1100 = 12
        result = s.execute_query(
            "MATCH (n:Item) RETURN bitAnd(n.flags, 12) AS v ORDER BY n.flags"
        )
        assert result["v"].tolist() == [4, 8, 12, 12]


# ---------------------------------------------------------------------------
# bitOr
# ---------------------------------------------------------------------------


class TestBitOr:
    def test_basic_or(self) -> None:
        s = pd.Series([0b1010, 0b0101], dtype=object)
        y = pd.Series([0b0101, 0b1010], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitOr", [s, y])
        assert r.tolist() == [0b1111, 0b1111]

    def test_or_with_zero(self) -> None:
        s = pd.Series([42], dtype=object)
        z = pd.Series([0], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitOr", [s, z])
        assert r.iloc[0] == 42

    def test_null_propagation(self) -> None:
        s = pd.Series([None, 5], dtype=object)
        y = pd.Series([3, None], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitOr", [s, y])
        assert r.iloc[0] is None
        assert r.iloc[1] is None

    def test_cypher_integration(self, bits_ctx: ContextBuilder) -> None:
        s = Star(context=bits_ctx)
        # OR with 1 (0b0001): all values get bit 0 set
        result = s.execute_query(
            "MATCH (n:Item) RETURN bitOr(n.flags, 1) AS v ORDER BY n.flags"
        )
        expected = [0b0111, 0b1011, 0b1101, 0b1111]
        assert result["v"].tolist() == expected


# ---------------------------------------------------------------------------
# bitXor
# ---------------------------------------------------------------------------


class TestBitXor:
    def test_basic_xor(self) -> None:
        s = pd.Series([0b1010, 0b1111], dtype=object)
        y = pd.Series([0b1100, 0b1111], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitXor", [s, y])
        assert r.tolist() == [0b0110, 0]

    def test_xor_self_is_zero(self) -> None:
        s = pd.Series([255, 42], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitXor", [s, s])
        assert r.tolist() == [0, 0]

    def test_null_propagation(self) -> None:
        s = pd.Series([None], dtype=object)
        y = pd.Series([7], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitXor", [s, y])
        assert r.iloc[0] is None

    def test_cypher_integration(self, bits_ctx: ContextBuilder) -> None:
        s = Star(context=bits_ctx)
        # XOR each flag with itself → 0 for all
        result = s.execute_query(
            "MATCH (n:Item) RETURN bitXor(n.flags, n.flags) AS v"
        )
        assert all(v == 0 for v in result["v"].tolist())


# ---------------------------------------------------------------------------
# bitNot
# ---------------------------------------------------------------------------


class TestBitNot:
    def test_basic_not(self) -> None:
        s = pd.Series([0], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitNot", [s])
        assert r.iloc[0] == -1  # ~0 == -1 in two's complement

    def test_not_minus_one(self) -> None:
        s = pd.Series([-1], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitNot", [s])
        assert r.iloc[0] == 0  # ~(-1) == 0

    def test_not_of_positive(self) -> None:
        # ~x == -(x+1) for signed integers
        s = pd.Series([5, 42, 100], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitNot", [s])
        assert r.tolist() == [-6, -43, -101]

    def test_null_propagation(self) -> None:
        s = pd.Series([None, 5], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute("bitNot", [s])
        assert r.iloc[0] is None
        assert r.iloc[1] == -6

    def test_cypher_integration(self, bits_ctx: ContextBuilder) -> None:
        s = Star(context=bits_ctx)
        # ~(n.val): 12→-13, 7→-8, 0→-1, -1→0
        result = s.execute_query(
            "MATCH (n:Item) RETURN bitNot(n.val) AS v ORDER BY n.val"
        )
        vals = sorted(result["v"].tolist())
        assert vals == sorted([-13, -8, -1, 0])


# ---------------------------------------------------------------------------
# bitShiftLeft
# ---------------------------------------------------------------------------


class TestBitShiftLeft:
    def test_basic_left_shift(self) -> None:
        s = pd.Series([1, 2, 3], dtype=object)
        y = pd.Series([3, 3, 3], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute(
            "bitShiftLeft", [s, y]
        )
        assert r.tolist() == [8, 16, 24]

    def test_shift_by_zero(self) -> None:
        s = pd.Series([42], dtype=object)
        z = pd.Series([0], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute(
            "bitShiftLeft", [s, z]
        )
        assert r.iloc[0] == 42

    def test_null_propagation(self) -> None:
        s = pd.Series([None, 4], dtype=object)
        y = pd.Series([2, None], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute(
            "bitShiftLeft", [s, y]
        )
        assert r.iloc[0] is None
        assert r.iloc[1] is None

    def test_cypher_integration(self, bits_ctx: ContextBuilder) -> None:
        s = Star(context=bits_ctx)
        # 1 << shift: 1<<1=2, 1<<2=4, 1<<3=8, 1<<4=16
        result = s.execute_query(
            "MATCH (n:Item) RETURN bitShiftLeft(1, n.shift) AS v ORDER BY n.shift"
        )
        assert result["v"].tolist() == [2, 4, 8, 16]


# ---------------------------------------------------------------------------
# bitShiftRight
# ---------------------------------------------------------------------------


class TestBitShiftRight:
    def test_basic_right_shift(self) -> None:
        s = pd.Series([16, 32, 64], dtype=object)
        y = pd.Series([2, 2, 2], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute(
            "bitShiftRight", [s, y]
        )
        assert r.tolist() == [4, 8, 16]

    def test_shift_by_zero(self) -> None:
        s = pd.Series([42], dtype=object)
        z = pd.Series([0], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute(
            "bitShiftRight", [s, z]
        )
        assert r.iloc[0] == 42

    def test_right_shift_loses_bits(self) -> None:
        s = pd.Series([7], dtype=object)  # 0b0111
        y = pd.Series([1], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute(
            "bitShiftRight", [s, y]
        )
        assert r.iloc[0] == 3  # 0b0011

    def test_null_propagation(self) -> None:
        s = pd.Series([None], dtype=object)
        y = pd.Series([2], dtype=object)
        r = ScalarFunctionRegistry.get_instance().execute(
            "bitShiftRight", [s, y]
        )
        assert r.iloc[0] is None

    def test_cypher_integration(self, bits_ctx: ContextBuilder) -> None:
        s = Star(context=bits_ctx)
        # flags >> 1: 0b1010>>1=5, 0b1100>>1=6, 0b0110>>1=3, 0b1111>>1=7
        result = s.execute_query(
            "MATCH (n:Item) RETURN bitShiftRight(n.flags, 1) AS v ORDER BY n.flags"
        )
        assert result["v"].tolist() == [3, 5, 6, 7]


# ---------------------------------------------------------------------------
# Vectorisation sanity check
# ---------------------------------------------------------------------------


class TestBitwiseVectorised:
    def test_bitand_large_series(self) -> None:
        """bitAnd over 10 000 rows completes in < 100ms (not per-row Python)."""
        import time

        n = 10_000
        s = pd.Series(list(range(n)), dtype=object)
        mask = pd.Series([0xFF] * n, dtype=object)
        t0 = time.perf_counter()
        for _ in range(100):
            ScalarFunctionRegistry.get_instance().execute("bitAnd", [s, mask])
        elapsed = time.perf_counter() - t0
        assert elapsed < 5.0, (
            f"100 × 10k bitAnd took {elapsed:.2f}s (too slow)"
        )

    def test_all_six_return_series(self) -> None:
        """All six functions return a pd.Series (not a scalar or list)."""
        x = pd.Series([10, 20], dtype=object)
        y = pd.Series([3, 5], dtype=object)
        reg = ScalarFunctionRegistry.get_instance()
        for fname, args in [
            ("bitAnd", [x, y]),
            ("bitOr", [x, y]),
            ("bitXor", [x, y]),
            ("bitNot", [x]),
            ("bitShiftLeft", [x, y]),
            ("bitShiftRight", [x, y]),
        ]:
            result = reg.execute(fname, args)
            assert isinstance(result, pd.Series), (
                f"{fname} did not return Series"
            )
            assert len(result) == 2, f"{fname} wrong length"
