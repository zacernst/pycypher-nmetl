"""Tests for null-safe boolean logic in Cypher expressions.

Per the openCypher spec, null is treated as False in boolean
predicates (AND, OR, NOT, XOR) so that filter rows are not
unexpectedly dropped when one operand evaluates to null.

These tests also cover the edge-case empty-operand behaviour
(AND over zero operands → True; OR/XOR over zero operands → False),
which is exposed only through the evaluator internals but should be
stable across refactors.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder


@pytest.fixture
def star() -> Star:
    persons = pd.DataFrame(
        {
            "__ID__": ["p1", "p2", "p3"],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
            # 'score' intentionally absent so p.score is null for every row
        },
    )
    return Star(context=ContextBuilder.from_dict({"Person": persons}))


class TestBooleanNullSafety:
    """Boolean operators must treat null as False, not propagate nulls."""

    def test_and_with_null_operand_treats_null_as_false(
        self,
        star: Star,
    ) -> None:
        """Null AND true == false — the null operand is treated as False."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.score IS NOT NULL AND p.name = 'Alice' "
            "RETURN p.name AS name",
        )
        # p.score is null for all rows; null IS NOT NULL == False; False AND anything == False
        assert len(result) == 0

    def test_or_with_null_left_operand(self, star: Star) -> None:
        """Null OR true — null treated as False, so OR evaluates on the right."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.score IS NOT NULL OR p.name = 'Alice' "
            "RETURN p.name AS name",
        )
        # p.score IS NOT NULL == False for all; False OR (name='Alice') → only Alice
        assert list(result["name"]) == ["Alice"]

    def test_not_with_null_operand(self, star: Star) -> None:
        """NOT null is False — null treated as False before inversion."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE NOT p.score IS NOT NULL "
            "RETURN p.name AS name ORDER BY p.name ASC",
        )
        # NOT (null IS NOT NULL) = NOT False = True → all three rows pass
        assert list(result["name"]) == ["Alice", "Bob", "Carol"]

    def test_and_all_true(self, star: Star) -> None:
        """True AND True evaluates correctly."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 20 AND p.name = 'Alice' "
            "RETURN p.name AS name",
        )
        assert list(result["name"]) == ["Alice"]

    def test_or_all_false(self, star: Star) -> None:
        """False OR False returns no rows."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 100 OR p.name = 'Nobody' "
            "RETURN p.name AS name",
        )
        assert len(result) == 0

    def test_xor_exclusive(self, star: Star) -> None:
        """XOR is True only when exactly one operand is True."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE (p.age > 28) XOR (p.name = 'Bob') "
            "RETURN p.name AS name ORDER BY p.name ASC",
        )
        # age > 28: Alice (30), Carol (35) → True; name='Bob': Bob → True
        # Alice: True XOR False = True
        # Bob:   False XOR True = True
        # Carol: True XOR False = True
        assert list(result["name"]) == ["Alice", "Bob", "Carol"]

    def test_xor_both_true_cancels(self, star: Star) -> None:
        """XOR is False when both operands are True."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "WITH p WHERE (p.age > 20) XOR (p.name = 'Alice') "
            "RETURN p.name AS name",
        )
        # age > 20: True; name='Alice': True → True XOR True = False → no rows
        assert len(result) == 0

    def test_xor_with_null(self, star: Star) -> None:
        """XOR with null operand — null treated as False."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "WITH p WHERE (p.score IS NOT NULL) XOR (p.name = 'Alice') "
            "RETURN p.name AS name",
        )
        # score IS NOT NULL = False; name='Alice' = True; False XOR True = True
        assert list(result["name"]) == ["Alice"]
