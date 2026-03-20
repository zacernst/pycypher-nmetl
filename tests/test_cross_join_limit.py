"""Tests for the cross-join row limit safety mechanism.

Validates that ``BindingFrame.cross_join()`` refuses to produce result sets
exceeding ``MAX_CROSS_JOIN_ROWS``, preventing accidental Cartesian explosions
from consuming all available memory.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest
from pycypher import ContextBuilder, Star
from pycypher.binding_frame import MAX_CROSS_JOIN_ROWS, BindingFrame
from pycypher.exceptions import QueryMemoryBudgetError


@pytest.fixture
def context() -> object:
    """Build a context with a small Person entity for basic tests."""
    df = pd.DataFrame({"__ID__": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]})
    return ContextBuilder().add_entity("Person", df).build()


class TestCrossJoinLimit:
    """Verify the hard ceiling on cross-join result size."""

    def test_small_cross_join_succeeds(self, context: object) -> None:
        """A small cross-join completes normally."""
        bf_left = BindingFrame(
            bindings=pd.DataFrame({"a": [1, 2]}),
            type_registry={"a": "Person"},
            context=context,
        )
        bf_right = BindingFrame(
            bindings=pd.DataFrame({"b": [3, 4]}),
            type_registry={"b": "Person"},
            context=context,
        )
        result = bf_left.cross_join(bf_right)
        assert len(result) == 4

    def test_exceeding_limit_raises_query_memory_budget_error(
        self, context: object
    ) -> None:
        """Cross-join exceeding MAX_CROSS_JOIN_ROWS raises QueryMemoryBudgetError."""
        # Create frames whose product exceeds the limit
        with patch("pycypher.binding_frame.MAX_CROSS_JOIN_ROWS", 100):
            bf_left = BindingFrame(
                bindings=pd.DataFrame({"a": list(range(20))}),
                type_registry={"a": "Person"},
                context=context,
            )
            bf_right = BindingFrame(
                bindings=pd.DataFrame({"b": list(range(10))}),
                type_registry={"b": "Person"},
                context=context,
            )
            # 20 x 10 = 200 > 100 limit
            with pytest.raises(
                QueryMemoryBudgetError, match="200"
            ) as exc_info:
                bf_left.cross_join(bf_right)
            # Verify structured attributes are set
            assert exc_info.value.estimated_bytes > exc_info.value.budget_bytes

    def test_exactly_at_limit_succeeds(self, context: object) -> None:
        """Cross-join producing exactly MAX_CROSS_JOIN_ROWS rows is allowed."""
        with patch("pycypher.binding_frame.MAX_CROSS_JOIN_ROWS", 100):
            bf_left = BindingFrame(
                bindings=pd.DataFrame({"a": list(range(10))}),
                type_registry={"a": "Person"},
                context=context,
            )
            bf_right = BindingFrame(
                bindings=pd.DataFrame({"b": list(range(10))}),
                type_registry={"b": "Person"},
                context=context,
            )
            # 10 x 10 = 100, exactly at limit — should succeed
            result = bf_left.cross_join(bf_right)
            assert len(result) == 100

    def test_error_message_includes_dimensions(self, context: object) -> None:
        """Error message reports both input sizes and the limit."""
        with patch("pycypher.binding_frame.MAX_CROSS_JOIN_ROWS", 50):
            bf_left = BindingFrame(
                bindings=pd.DataFrame({"a": list(range(10))}),
                type_registry={"a": "Person"},
                context=context,
            )
            bf_right = BindingFrame(
                bindings=pd.DataFrame({"b": list(range(10))}),
                type_registry={"b": "Person"},
                context=context,
            )
            with pytest.raises(
                QueryMemoryBudgetError, match=r"10.*×.*10"
            ) as exc_info:
                bf_left.cross_join(bf_right)
            msg = str(exc_info.value)
            assert "100" in msg  # result size
            assert "50" in msg  # limit

    def test_zero_rows_always_succeeds(self, context: object) -> None:
        """Cross-join with an empty frame produces 0 rows."""
        bf_left = BindingFrame(
            bindings=pd.DataFrame({"a": [1, 2]}),
            type_registry={"a": "Person"},
            context=context,
        )
        bf_right = BindingFrame(
            bindings=pd.DataFrame({"b": pd.Series([], dtype=int)}),
            type_registry={"b": "Person"},
            context=context,
        )
        result = bf_left.cross_join(bf_right)
        assert len(result) == 0


class TestCrossJoinLimitDefault:
    """Verify the default limit value."""

    def test_default_limit_is_10_million(self) -> None:
        """Default MAX_CROSS_JOIN_ROWS is 10 million."""
        assert MAX_CROSS_JOIN_ROWS == 10_000_000


class TestCrossJoinLimitIntegration:
    """Integration tests via Star.execute_query."""

    def test_normal_match_succeeds(self) -> None:
        """Normal queries with small datasets work fine."""
        df = pd.DataFrame({"__ID__": [1, 2], "name": ["A", "B"]})
        context = ContextBuilder().add_entity("Person", df).build()
        star = Star(context=context)
        result = star.execute_query("MATCH (p:Person) RETURN p.name")
        assert len(result) == 2

    def test_multi_match_small_succeeds(self) -> None:
        """Multiple MATCH clauses with small dataset produce valid cross-join."""
        df = pd.DataFrame({"__ID__": [1, 2], "name": ["A", "B"]})
        context = ContextBuilder().add_entity("Person", df).build()
        star = Star(context=context)
        result = star.execute_query(
            "MATCH (a:Person) MATCH (b:Person) RETURN a.name, b.name",
        )
        assert len(result) == 4  # 2 x 2
