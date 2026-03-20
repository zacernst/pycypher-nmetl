"""TDD tests for auto-generated column names in RETURN / WITH clauses.

When a RETURN item has no explicit ``AS`` alias, Neo4j generates a display
name from the expression.  Previously pycypher set alias=None for any
expression other than a bare Variable or PropertyLookup.  With two aliasless
expressions, the second silently overwrote the first in the result dict
(both got ``None`` as key), so the caller saw a single column ``None``
instead of two distinct columns.

Expected behaviour after the fix:

  * ``RETURN n.name``            → column ``name`` (unchanged — existing convention)
  * ``RETURN n``                 → column ``n``   (unchanged)
  * ``RETURN toUpper(n.name)``   → column ``toUpper(name)``
  * ``RETURN size(n.name)``      → column ``size(name)``
  * ``RETURN abs(n.age)``        → column ``abs(age)``
  * ``RETURN n.age + 1``         → column ``age + 1``
  * ``RETURN n.age * 2``         → column ``age * 2``
  * ``RETURN 42``                → column ``42``
  * ``RETURN 3.14``              → column ``3.14``
  * ``RETURN 'hello'``           → column ``hello``
  * ``RETURN true``              → column ``true``
  * ``RETURN null``              → column ``null``
  * ``RETURN toUpper(n.name), n.age + 1`` → two distinct columns (not both None)

The auto-name uses the same unqualified property convention as the existing
PropertyLookup path: ``n.name`` → ``name``, so ``toUpper(n.name)``
→ ``toUpper(name)`` (not ``toUpper(n.name)``).  This keeps column names short
and consistent with the rest of the API.
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
                    "__ID__": ["p1", "p2"],
                    "name": ["Alice", "Bob"],
                    "age": [30, 25],
                    "active": [True, False],
                }
            )
        }
    )


@pytest.fixture(scope="module")
def star(ctx: ContextBuilder) -> Star:
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# Existing behaviour (must not regress)
# ---------------------------------------------------------------------------


class TestExistingNamingUnchanged:
    def test_property_lookup_alias_is_property_name(self, star: Star) -> None:
        """RETURN p.name → column 'name'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.name")
        assert "name" in r.columns

    def test_variable_alias_is_var_name(self, star: Star) -> None:
        """RETURN p → column 'p'."""
        r = star.execute_query("MATCH (p:Person) RETURN p")
        assert "p" in r.columns

    def test_explicit_alias_is_preserved(self, star: Star) -> None:
        """RETURN p.name AS n → column 'n'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.name AS n")
        assert "n" in r.columns
        assert "name" not in r.columns


# ---------------------------------------------------------------------------
# Function calls
# ---------------------------------------------------------------------------


class TestFunctionCallColumnNames:
    def test_single_arg_function(self, star: Star) -> None:
        """RETURN toUpper(p.name) → column 'toUpper(name)'."""
        r = star.execute_query("MATCH (p:Person) RETURN toUpper(p.name)")
        assert "toUpper(name)" in r.columns
        assert r["toUpper(name)"].tolist() == ["ALICE", "BOB"]

    def test_size_function(self, star: Star) -> None:
        """RETURN size(p.name) → column 'size(name)'."""
        r = star.execute_query("MATCH (p:Person) RETURN size(p.name)")
        assert "size(name)" in r.columns

    def test_abs_function(self, star: Star) -> None:
        """RETURN abs(p.age) → column 'abs(age)'."""
        r = star.execute_query("MATCH (p:Person) RETURN abs(p.age)")
        assert "abs(age)" in r.columns

    def test_nested_function(self, star: Star) -> None:
        """RETURN size(toUpper(p.name)) → column contains 'size'."""
        r = star.execute_query("MATCH (p:Person) RETURN size(toUpper(p.name))")
        # Exact name: size(toUpper(name))
        assert "size(toUpper(name))" in r.columns

    def test_two_arg_function(self, star: Star) -> None:
        """RETURN substring(p.name, 0) → column 'substring(name, 0)'."""
        r = star.execute_query("MATCH (p:Person) RETURN substring(p.name, 0)")
        assert "substring(name, 0)" in r.columns


# ---------------------------------------------------------------------------
# Arithmetic expressions
# ---------------------------------------------------------------------------


class TestArithmeticColumnNames:
    def test_addition(self, star: Star) -> None:
        """RETURN p.age + 1 → column 'age + 1'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.age + 1")
        assert "age + 1" in r.columns

    def test_multiplication(self, star: Star) -> None:
        """RETURN p.age * 2 → column 'age * 2'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.age * 2")
        assert "age * 2" in r.columns

    def test_subtraction(self, star: Star) -> None:
        """RETURN p.age - 5 → column 'age - 5'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.age - 5")
        assert "age - 5" in r.columns

    def test_division(self, star: Star) -> None:
        """RETURN p.age / 2 → column 'age / 2'."""
        r = star.execute_query("MATCH (p:Person) RETURN p.age / 2")
        assert "age / 2" in r.columns


# ---------------------------------------------------------------------------
# Literals
# ---------------------------------------------------------------------------


class TestLiteralColumnNames:
    def test_integer_literal(self, star: Star) -> None:
        """RETURN 42 → column '42'."""
        r = star.execute_query("MATCH (p:Person) RETURN 42")
        assert "42" in r.columns

    def test_float_literal(self, star: Star) -> None:
        """RETURN 3.14 → column '3.14'."""
        r = star.execute_query("MATCH (p:Person) RETURN 3.14")
        assert "3.14" in r.columns

    def test_string_literal(self, star: Star) -> None:
        """RETURN 'hello' → column 'hello'."""
        r = star.execute_query("MATCH (p:Person) RETURN 'hello'")
        assert "hello" in r.columns

    def test_boolean_literal_true(self, star: Star) -> None:
        """RETURN true → column 'true'."""
        r = star.execute_query("MATCH (p:Person) RETURN true")
        assert "true" in r.columns

    def test_boolean_literal_false(self, star: Star) -> None:
        """RETURN false → column 'false'."""
        r = star.execute_query("MATCH (p:Person) RETURN false")
        assert "false" in r.columns

    def test_null_literal(self, star: Star) -> None:
        """RETURN null → column 'null'."""
        r = star.execute_query("MATCH (p:Person) RETURN null")
        assert "null" in r.columns


# ---------------------------------------------------------------------------
# Multiple unarrowed expressions — the original silent-clobber bug
# ---------------------------------------------------------------------------


class TestMultipleUnarrowedExpressions:
    def test_two_functions_distinct_columns(self, star: Star) -> None:
        """RETURN toUpper(p.name), toLower(p.name) → two distinct columns."""
        r = star.execute_query(
            "MATCH (p:Person) RETURN toUpper(p.name), toLower(p.name)"
        )
        assert len(r.columns) == 2
        assert "toUpper(name)" in r.columns
        assert "toLower(name)" in r.columns

    def test_function_and_arithmetic_distinct(self, star: Star) -> None:
        """RETURN toUpper(p.name), p.age + 1 → two distinct columns."""
        r = star.execute_query(
            "MATCH (p:Person) RETURN toUpper(p.name), p.age + 1"
        )
        assert len(r.columns) == 2
        assert "toUpper(name)" in r.columns
        assert "age + 1" in r.columns

    def test_no_none_column_names(self, star: Star) -> None:
        """After the fix, no column should be named None."""
        r = star.execute_query(
            "MATCH (p:Person) RETURN toUpper(p.name), abs(p.age)"
        )
        assert None not in r.columns

    def test_values_are_correct(self, star: Star) -> None:
        """Values in auto-named columns must be correct."""
        r = star.execute_query(
            "MATCH (p:Person) RETURN toUpper(p.name), p.age + 1 ORDER BY p.age"
        )
        assert r["toUpper(name)"].tolist() == ["BOB", "ALICE"]
        assert r["age + 1"].tolist() == [26, 31]

    def test_mixed_aliased_and_unaliased(self, star: Star) -> None:
        """Explicit alias and auto-name co-exist without collision."""
        r = star.execute_query(
            "MATCH (p:Person) RETURN toUpper(p.name) AS up, p.age + 1"
        )
        assert "up" in r.columns
        assert "age + 1" in r.columns
        assert None not in r.columns
