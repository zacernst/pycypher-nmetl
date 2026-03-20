"""TDD tests for startsWith(), endsWith(), contains() as scalar functions.

openCypher supports these as WHERE predicates (``STARTS WITH``, ``ENDS WITH``,
``CONTAINS``) but also as scalar functions callable in RETURN / WITH:

    RETURN startsWith(n.name, 'Al')      -- boolean
    RETURN endsWith(n.name, 'ce')        -- boolean
    RETURN contains(n.name, 'li')        -- boolean

All three functions:
- Return a boolean Series (True/False/null)
- Are case-sensitive (openCypher spec)
- Return null when either argument is null
- Are registered in the scalar function registry

All tests written before the fix (TDD step 1).
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star


@pytest.fixture()
def registry() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


@pytest.fixture()
def people_star() -> Star:
    """Alice, Bob, Carol, Dave — for integration tests."""
    df = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3, 4], "name": ["Alice", "Bob", "Carol", "Dave"]}
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


# ---------------------------------------------------------------------------
# Unit tests — registry-level (no Star, no graph)
# ---------------------------------------------------------------------------


class TestStartsWithRegistry:
    """startsWith() in the scalar function registry."""

    def test_registered(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("startsWith")

    def test_basic_true(self, registry: ScalarFunctionRegistry) -> None:
        s = pd.Series(["Alice"])
        prefix = pd.Series(["Al"])
        result = registry.execute("startsWith", [s, prefix])
        assert result.iloc[0] is True or result.iloc[0] == True  # noqa: E712

    def test_basic_false(self, registry: ScalarFunctionRegistry) -> None:
        s = pd.Series(["Alice"])
        prefix = pd.Series(["al"])  # case-sensitive: lowercase 'al' ≠ 'Al'
        result = registry.execute("startsWith", [s, prefix])
        assert result.iloc[0] is False or result.iloc[0] == False  # noqa: E712

    def test_null_string_returns_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        s = pd.Series([None])
        prefix = pd.Series(["Al"])
        result = registry.execute("startsWith", [s, prefix])
        assert pd.isna(result.iloc[0])

    def test_null_prefix_returns_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        s = pd.Series(["Alice"])
        prefix = pd.Series([None])
        result = registry.execute("startsWith", [s, prefix])
        assert pd.isna(result.iloc[0])

    def test_empty_prefix_true(self, registry: ScalarFunctionRegistry) -> None:
        """Every string starts with the empty string."""
        s = pd.Series(["Alice"])
        prefix = pd.Series([""])
        result = registry.execute("startsWith", [s, prefix])
        assert result.iloc[0] is True or result.iloc[0] == True  # noqa: E712

    def test_column(self, registry: ScalarFunctionRegistry) -> None:
        names = pd.Series(["Alice", "Bob", "Albert"])
        prefix = pd.Series(["Al"])
        result = registry.execute("startsWith", [names, prefix])
        assert list(result) == [True, False, True]


class TestEndsWithRegistry:
    """endsWith() in the scalar function registry."""

    def test_registered(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("endsWith")

    def test_basic_true(self, registry: ScalarFunctionRegistry) -> None:
        s = pd.Series(["Alice"])
        suffix = pd.Series(["ice"])
        result = registry.execute("endsWith", [s, suffix])
        assert result.iloc[0] is True or result.iloc[0] == True  # noqa: E712

    def test_basic_false(self, registry: ScalarFunctionRegistry) -> None:
        s = pd.Series(["Alice"])
        suffix = pd.Series(["Ice"])  # case-sensitive
        result = registry.execute("endsWith", [s, suffix])
        assert result.iloc[0] is False or result.iloc[0] == False  # noqa: E712

    def test_null_string_returns_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        s = pd.Series([None])
        suffix = pd.Series(["ice"])
        result = registry.execute("endsWith", [s, suffix])
        assert pd.isna(result.iloc[0])

    def test_null_suffix_returns_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        s = pd.Series(["Alice"])
        suffix = pd.Series([None])
        result = registry.execute("endsWith", [s, suffix])
        assert pd.isna(result.iloc[0])

    def test_empty_suffix_true(self, registry: ScalarFunctionRegistry) -> None:
        """Every string ends with the empty string."""
        s = pd.Series(["Alice"])
        suffix = pd.Series([""])
        result = registry.execute("endsWith", [s, suffix])
        assert result.iloc[0] is True or result.iloc[0] == True  # noqa: E712

    def test_column(self, registry: ScalarFunctionRegistry) -> None:
        names = pd.Series(["Alice", "Bob", "Dave"])
        suffix = pd.Series(["e"])
        result = registry.execute("endsWith", [names, suffix])
        assert list(result) == [True, False, True]


class TestContainsRegistry:
    """contains() in the scalar function registry."""

    def test_registered(self, registry: ScalarFunctionRegistry) -> None:
        assert registry.has_function("contains")

    def test_basic_true(self, registry: ScalarFunctionRegistry) -> None:
        s = pd.Series(["Alice"])
        sub = pd.Series(["lic"])
        result = registry.execute("contains", [s, sub])
        assert result.iloc[0] is True or result.iloc[0] == True  # noqa: E712

    def test_basic_false(self, registry: ScalarFunctionRegistry) -> None:
        s = pd.Series(["Alice"])
        sub = pd.Series(["LIC"])  # case-sensitive
        result = registry.execute("contains", [s, sub])
        assert result.iloc[0] is False or result.iloc[0] == False  # noqa: E712

    def test_null_string_returns_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        s = pd.Series([None])
        sub = pd.Series(["lic"])
        result = registry.execute("contains", [s, sub])
        assert pd.isna(result.iloc[0])

    def test_null_substring_returns_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        s = pd.Series(["Alice"])
        sub = pd.Series([None])
        result = registry.execute("contains", [s, sub])
        assert pd.isna(result.iloc[0])

    def test_empty_substring_true(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """Every string contains the empty string."""
        s = pd.Series(["Alice"])
        sub = pd.Series([""])
        result = registry.execute("contains", [s, sub])
        assert result.iloc[0] is True or result.iloc[0] == True  # noqa: E712

    def test_column(self, registry: ScalarFunctionRegistry) -> None:
        names = pd.Series(["Alice", "Bob", "Charlie"])
        sub = pd.Series(["li"])
        result = registry.execute("contains", [names, sub])
        assert list(result) == [True, False, True]


# ---------------------------------------------------------------------------
# Integration tests — via Star.execute_query
# ---------------------------------------------------------------------------


class TestStringPredicateFunctionsIntegration:
    """startsWith / endsWith / contains in real Cypher queries."""

    def test_starts_with_in_return(self, people_star: Star) -> None:
        r = people_star.execute_query(
            "MATCH (p:Person) RETURN startsWith(p.name, 'Al') AS sw ORDER BY p.name"
        )
        assert list(r["sw"]) == [True, False, False, False]  # Alice only

    def test_ends_with_in_return(self, people_star: Star) -> None:
        r = people_star.execute_query(
            "MATCH (p:Person) RETURN endsWith(p.name, 'e') AS ew ORDER BY p.name"
        )
        # Alice → True, Bob → False, Carol → False, Dave → True
        assert list(r["ew"]) == [True, False, False, True]

    def test_contains_in_return(self, people_star: Star) -> None:
        r = people_star.execute_query(
            "MATCH (p:Person) RETURN contains(p.name, 'o') AS c ORDER BY p.name"
        )
        # Alice → False, Bob → True, Carol → True, Dave → False
        assert list(r["c"]) == [False, True, True, False]

    def test_starts_with_in_where(self, people_star: Star) -> None:
        r = people_star.execute_query(
            "MATCH (p:Person) WHERE startsWith(p.name, 'Al') RETURN p.name"
        )
        assert list(r["name"]) == ["Alice"]

    def test_ends_with_in_where(self, people_star: Star) -> None:
        r = people_star.execute_query(
            "MATCH (p:Person) WHERE endsWith(p.name, 'b') RETURN p.name"
        )
        assert list(r["name"]) == ["Bob"]

    def test_contains_in_where(self, people_star: Star) -> None:
        r = people_star.execute_query(
            "MATCH (p:Person) WHERE contains(p.name, 'ar') RETURN p.name"
        )
        assert list(r["name"]) == ["Carol"]

    def test_starts_with_case_sensitive(self, people_star: Star) -> None:
        """startsWith is case-sensitive — lowercase prefix returns no match."""
        r = people_star.execute_query(
            "MATCH (p:Person) WHERE startsWith(p.name, 'al') RETURN p.name"
        )
        assert len(r) == 0

    def test_combined_with_predicate_operator(self, people_star: Star) -> None:
        """startsWith() function AND STARTS WITH predicate must agree."""
        r_func = people_star.execute_query(
            "MATCH (p:Person) WHERE startsWith(p.name, 'A') RETURN p.name ORDER BY p.name"
        )
        r_pred = people_star.execute_query(
            "MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p.name ORDER BY p.name"
        )
        assert list(r_func["name"]) == list(r_pred["name"])
