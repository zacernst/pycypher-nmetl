"""TDD tests for clear TypeError messages when string predicates receive
non-string operands.

Before this fix:
    WHERE p.age STARTS WITH 'A'
      → AttributeError: Can only use .str accessor with string values,
                        not integer. Did you mean: 'std'?
        (deep 12-frame pandas traceback with zero Cypher-level context)

After this fix:
    → TypeError: Operator 'STARTS WITH' requires a string left-hand operand,
                 but got 'int64'. Use toString() to convert if needed.

Covered operators: STARTS WITH, ENDS WITH, CONTAINS, =~
Null-only and all-string columns must continue to work without error.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def ctx() -> ContextBuilder:
    return ContextBuilder().from_dict(
        {
            "Person": pd.DataFrame(
                {
                    "__ID__": ["p1", "p2", "p3"],
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [30, 25, 40],
                    "score": [1.5, 2.0, 3.5],
                    "active": [True, False, True],
                },
            ),
        },
    )


@pytest.fixture(scope="module")
def star(ctx: ContextBuilder) -> Star:
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# STARTS WITH on non-string operands
# ---------------------------------------------------------------------------


class TestStartsWithTypeError:
    def test_integer_operand(self, star: Star) -> None:
        """STARTS WITH on integer column raises TypeError, not AttributeError."""
        with pytest.raises(TypeError, match="STARTS WITH"):
            star.execute_query(
                "MATCH (p:Person) WHERE p.age STARTS WITH '3' RETURN p.name",
            )

    def test_float_operand(self, star: Star) -> None:
        """STARTS WITH on float column raises TypeError."""
        with pytest.raises(TypeError, match="STARTS WITH"):
            star.execute_query(
                "MATCH (p:Person) WHERE p.score STARTS WITH '1' RETURN p.name",
            )

    def test_boolean_operand(self, star: Star) -> None:
        """STARTS WITH on boolean column raises TypeError."""
        with pytest.raises(TypeError, match="STARTS WITH"):
            star.execute_query(
                "MATCH (p:Person) WHERE p.active STARTS WITH 'T' RETURN p.name",
            )

    def test_error_message_mentions_operator(self, star: Star) -> None:
        """TypeError message must name the operator."""
        with pytest.raises(TypeError) as exc_info:
            star.execute_query(
                "MATCH (p:Person) WHERE p.age STARTS WITH '3' RETURN p.name",
            )
        assert "STARTS WITH" in str(exc_info.value)

    def test_error_message_mentions_toString(self, star: Star) -> None:
        """TypeError message should hint at toString() as a fix."""
        with pytest.raises(TypeError) as exc_info:
            star.execute_query(
                "MATCH (p:Person) WHERE p.age STARTS WITH '3' RETURN p.name",
            )
        assert "toString" in str(exc_info.value)

    def test_string_operand_still_works(self, star: Star) -> None:
        """STARTS WITH on a real string column must NOT raise."""
        r = star.execute_query(
            "MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p.name",
        )
        assert len(r) == 1
        assert r["name"].iloc[0] == "Alice"


# ---------------------------------------------------------------------------
# ENDS WITH on non-string operands
# ---------------------------------------------------------------------------


class TestEndsWithTypeError:
    def test_integer_operand(self, star: Star) -> None:
        """ENDS WITH on integer column raises TypeError."""
        with pytest.raises(TypeError, match="ENDS WITH"):
            star.execute_query(
                "MATCH (p:Person) WHERE p.age ENDS WITH '5' RETURN p.name",
            )

    def test_string_operand_still_works(self, star: Star) -> None:
        """ENDS WITH on string column must NOT raise."""
        r = star.execute_query(
            "MATCH (p:Person) WHERE p.name ENDS WITH 'e' RETURN p.name",
        )
        assert len(r) == 2  # Alice, Charlie


# ---------------------------------------------------------------------------
# CONTAINS on non-string operands
# ---------------------------------------------------------------------------


class TestContainsTypeError:
    def test_integer_operand(self, star: Star) -> None:
        """CONTAINS on integer column raises TypeError."""
        with pytest.raises(TypeError, match="CONTAINS"):
            star.execute_query(
                "MATCH (p:Person) WHERE p.age CONTAINS '2' RETURN p.name",
            )

    def test_string_operand_still_works(self, star: Star) -> None:
        """CONTAINS on string column must NOT raise."""
        r = star.execute_query(
            "MATCH (p:Person) WHERE p.name CONTAINS 'ob' RETURN p.name",
        )
        assert len(r) == 1
        assert r["name"].iloc[0] == "Bob"


# ---------------------------------------------------------------------------
# =~ (regex) on non-string operands
# ---------------------------------------------------------------------------


class TestRegexTypeError:
    def test_integer_operand(self, star: Star) -> None:
        """=~ on integer column raises TypeError."""
        with pytest.raises(TypeError, match="=~"):
            star.execute_query(
                "MATCH (p:Person) WHERE p.age =~ '\\\\d+' RETURN p.name",
            )

    def test_string_operand_still_works(self, star: Star) -> None:
        """=~ on string column must NOT raise."""
        r = star.execute_query(
            "MATCH (p:Person) WHERE p.name =~ 'A.*' RETURN p.name",
        )
        assert len(r) == 1
        assert r["name"].iloc[0] == "Alice"


# ---------------------------------------------------------------------------
# Null-only column must not trigger the type guard
# ---------------------------------------------------------------------------


class TestNullColumnNotRejected:
    def test_null_column_does_not_raise(self) -> None:
        """A column that is entirely NULL should not trigger the type guard."""
        ctx = ContextBuilder().from_dict(
            {
                "Thing": pd.DataFrame(
                    {
                        "__ID__": ["t1", "t2"],
                        "label": [None, None],
                    },
                ),
            },
        )
        s = Star(context=ctx)
        # All-null column: STARTS WITH should return null (not raise)
        r = s.execute_query(
            "MATCH (t:Thing) WHERE t.label STARTS WITH 'A' RETURN t.label",
        )
        # No rows match (null IS NOT 'A...'), result is empty or all-false
        assert len(r) == 0 or r["label"].isna().all()
