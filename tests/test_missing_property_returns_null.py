"""Tests verifying that accessing a nonexistent node property returns null.

Per Cypher semantics, `n.nonexistent` must return null rather than raising an
exception.  This enables idioms like:

    MATCH (n) RETURN coalesce(n.optionalProp, "default")
    MATCH (n) WHERE n.optionalFlag IS NULL RETURN n
    MATCH (n) RETURN n.missing IS NOT NULL AS hasIt

All execute through Star.execute_query() for full integration coverage.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def people_df() -> pd.DataFrame:
    """Persons with only 'name' — no 'age', 'email', or 'score' columns."""
    return pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
        },
    )


@pytest.fixture
def mixed_df() -> pd.DataFrame:
    """Persons with sparse 'age' column (some rows have it, some don't)."""
    return pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, pd.NA, 25],
        },
    )


@pytest.fixture
def star(people_df: pd.DataFrame) -> Star:
    context = ContextBuilder.from_dict({"Person": people_df})
    return Star(context=context)


@pytest.fixture
def star_mixed(mixed_df: pd.DataFrame) -> Star:
    context = ContextBuilder.from_dict({"Person": mixed_df})
    return Star(context=context)


# ===========================================================================
# Accessing a completely absent property column
# ===========================================================================


class TestMissingPropertyReturnsNull:
    """n.prop where 'prop' is not a column in the entity table → null."""

    def test_return_missing_property_gives_null(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age AS age",
        )
        val = result["age"].iloc[0]
        # Must be null/NaN, not raise
        assert (
            val is None or val is pd.NA or (isinstance(val, float) and math.isnan(val))
        )

    def test_all_rows_missing_property_all_null(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.age AS age ORDER BY p.name ASC",
        )
        assert len(result) == 3
        for val in result["age"]:
            assert (
                val is None
                or val is pd.NA
                or (isinstance(val, float) and math.isnan(val))
            )

    def test_missing_property_is_null_check(self, star: Star) -> None:
        """IS NULL on a missing property must be True."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.age IS NULL AS missing ORDER BY p.name ASC",
        )
        assert all(bool(v) for v in result["missing"])

    def test_missing_property_is_not_null_check(self, star: Star) -> None:
        """IS NOT NULL on a missing property must be False."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.age IS NOT NULL AS has_age ORDER BY p.name ASC",
        )
        assert all(not bool(v) for v in result["has_age"])

    def test_coalesce_missing_property(self, star: Star) -> None:
        """coalesce(p.missing, 'default') must return the fallback."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN coalesce(p.age, 0) AS age",
        )
        assert result["age"].iloc[0] == 0

    def test_coalesce_missing_property_string_fallback(
        self,
        star: Star,
    ) -> None:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN coalesce(p.email, 'unknown') AS email",
        )
        assert result["email"].iloc[0] == "unknown"

    def test_where_filter_on_missing_property(self, star: Star) -> None:
        """WHERE p.missing > 0 on a missing property must return no rows."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 0 RETURN p.name AS name",
        )
        # null > 0 is null (falsy) — no rows survive
        assert len(result) == 0

    def test_count_with_missing_property(self, star: Star) -> None:
        """count(p.age) should count non-null values, which is 0 when col absent."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN count(p.age) AS n",
        )
        # All nulls → count of non-null values is 0
        assert result["n"].iloc[0] == 0

    def test_missing_property_in_arithmetic_gives_null(
        self,
        star: Star,
    ) -> None:
        """p.age + 1 where age is missing must be null, not raise."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age + 1 AS incremented",
        )
        val = result["incremented"].iloc[0]
        assert (
            val is None or val is pd.NA or (isinstance(val, float) and math.isnan(val))
        )


# ===========================================================================
# Sparse property column (some rows have it, some are NA)
# ===========================================================================


class TestSparsePropertyReturnsNull:
    """n.prop where 'prop' exists but is NA for some rows — existing behaviour."""

    def test_sparse_property_returns_value_or_null(
        self,
        star_mixed: Star,
    ) -> None:
        result = star_mixed.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY p.name ASC",
        )
        alice_age = result[result["name"] == "Alice"]["age"].iloc[0]
        bob_age = result[result["name"] == "Bob"]["age"].iloc[0]
        assert alice_age == 30
        # Bob's age is NA
        assert (
            bob_age is None
            or bob_age is pd.NA
            or (isinstance(bob_age, float) and math.isnan(bob_age))
        )

    def test_coalesce_sparse_property(self, star_mixed: Star) -> None:
        result = star_mixed.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, coalesce(p.age, -1) AS age "
            "ORDER BY p.name ASC",
        )
        alice = result[result["name"] == "Alice"]["age"].iloc[0]
        bob = result[result["name"] == "Bob"]["age"].iloc[0]
        assert alice == 30
        assert bob == -1
