"""TDD tests for auto-generated column names from predicate / boolean expressions.

Loop 205 extended ``_infer_alias`` to cover literals, function calls, and
arithmetic.  This file covers the remaining expression shapes that produce
``None`` column names without an explicit ``AS`` alias:

* ``Comparison``    — ``RETURN p.age > 25``   → ``"age > 25"``
* ``StringPredicate`` — ``RETURN p.name STARTS WITH 'A'`` → ``"name STARTS WITH A"``
* ``NullCheck``     — ``RETURN p.name IS NULL``  → ``"name IS NULL"``
* ``Not``           — ``RETURN NOT p.active``    → ``"NOT active"``
* ``And``           — ``RETURN p.age > 25 AND p.active`` → ``"age > 25 AND active"``
* ``Or``            — ``RETURN p.age < 30 OR p.active``  → ``"age < 30 OR active"``
* ``Xor``           — ``RETURN p.active XOR false``      → ``"active XOR false"``
* ``LabelPredicate`` — ``RETURN p:Person``              → ``"p:Person"``

Existing column names (Variable, PropertyLookup, explicit alias) must not regress.
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
                    "active": [True, False, True],
                },
            ),
        },
    )


@pytest.fixture(scope="module")
def star(ctx: ContextBuilder) -> Star:
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# Comparison expressions
# ---------------------------------------------------------------------------


class TestComparisonColumnNames:
    def test_gt(self, star: Star) -> None:
        """RETURN p.age > 25 → column 'age > 25'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.age > 25")
        assert "age > 25" in r.columns
        assert None not in r.columns

    def test_lt(self, star: Star) -> None:
        """RETURN p.age < 30 → column 'age < 30'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.age < 30")
        assert "age < 30" in r.columns

    def test_eq(self, star: Star) -> None:
        """RETURN p.name = 'Alice' → column 'name = Alice'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.name = 'Alice'")
        assert "name = Alice" in r.columns

    def test_neq(self, star: Star) -> None:
        """RETURN p.age <> 30 → column 'age <> 30'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.age <> 30")
        assert "age <> 30" in r.columns

    def test_values_are_correct(self, star: Star) -> None:
        """Values produced for a comparison column must be correct booleans."""
        r = star.execute_query(
            "MATCH (p:Person) RETURN p.age > 25 ORDER BY p.age",
        )
        col = "age > 25"
        assert col in r.columns
        vals = r[col].tolist()
        # ages 25, 30, 40 → False, True, True
        assert vals == [False, True, True]


# ---------------------------------------------------------------------------
# String predicate expressions
# ---------------------------------------------------------------------------


class TestStringPredicateColumnNames:
    def test_starts_with(self, star: Star) -> None:
        """RETURN p.name STARTS WITH 'A' → column 'name STARTS WITH A'."""
        r = star.execute_query(
            "MATCH (p:Person) RETURN p.name STARTS WITH 'A'",
        )
        assert "name STARTS WITH A" in r.columns
        assert None not in r.columns

    def test_ends_with(self, star: Star) -> None:
        """RETURN p.name ENDS WITH 'e' → column 'name ENDS WITH e'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.name ENDS WITH 'e'")
        assert "name ENDS WITH e" in r.columns

    def test_contains(self, star: Star) -> None:
        """RETURN p.name CONTAINS 'ob' → column 'name CONTAINS ob'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.name CONTAINS 'ob'")
        assert "name CONTAINS ob" in r.columns


# ---------------------------------------------------------------------------
# NullCheck expressions
# ---------------------------------------------------------------------------


class TestNullCheckColumnNames:
    def test_is_null(self, star: Star) -> None:
        """RETURN p.name IS NULL → column 'name IS NULL'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.name IS NULL")
        assert "name IS NULL" in r.columns
        assert None not in r.columns

    def test_is_not_null(self, star: Star) -> None:
        """RETURN p.name IS NOT NULL → column 'name IS NOT NULL'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.name IS NOT NULL")
        assert "name IS NOT NULL" in r.columns


# ---------------------------------------------------------------------------
# Not expression
# ---------------------------------------------------------------------------


class TestNotColumnNames:
    def test_not_property(self, star: Star) -> None:
        """RETURN NOT p.active → column 'NOT active'."""
        r = star.execute_query("MATCH (p:Person) RETURN NOT p.active")
        assert "NOT active" in r.columns
        assert None not in r.columns

    def test_not_comparison(self, star: Star) -> None:
        """RETURN NOT (p.age > 30) → column starts with 'NOT'."""
        r = star.execute_query("MATCH (p:Person) RETURN NOT p.age > 30")
        col = r.columns[0]
        assert col is not None
        assert col.startswith("NOT")


# ---------------------------------------------------------------------------
# Boolean connectives (And / Or / Xor)
# ---------------------------------------------------------------------------


class TestBooleanConnectiveColumnNames:
    def test_and(self, star: Star) -> None:
        """RETURN p.age > 25 AND p.active → column 'age > 25 AND active'."""
        r = star.execute_query(
            "MATCH (p:Person) RETURN p.age > 25 AND p.active",
        )
        assert "age > 25 AND active" in r.columns
        assert None not in r.columns

    def test_or(self, star: Star) -> None:
        """RETURN p.age < 30 OR p.active → column 'age < 30 OR active'."""
        r = star.execute_query(
            "MATCH (p:Person) RETURN p.age < 30 OR p.active",
        )
        assert "age < 30 OR active" in r.columns

    def test_xor(self, star: Star) -> None:
        """RETURN p.active XOR false → column 'active XOR false'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.active XOR false")
        assert "active XOR false" in r.columns


# ---------------------------------------------------------------------------
# No-None guard — multiple predicates, no aliases
# ---------------------------------------------------------------------------


class TestNoNonePredicateColumns:
    def test_two_predicates_distinct_columns(self, star: Star) -> None:
        """Two aliasless predicates must yield two distinct columns."""
        r = star.execute_query(
            "MATCH (p:Person) RETURN p.age > 25, p.name CONTAINS 'li'",
        )
        assert len(r.columns) == 2
        assert None not in r.columns

    def test_predicate_and_arithmetic_distinct(self, star: Star) -> None:
        """Predicate + arithmetic must yield two distinct non-None columns."""
        r = star.execute_query("MATCH (p:Person) RETURN p.age > 25, p.age + 1")
        assert len(r.columns) == 2
        assert None not in r.columns
        assert "age > 25" in r.columns
        assert "age + 1" in r.columns
