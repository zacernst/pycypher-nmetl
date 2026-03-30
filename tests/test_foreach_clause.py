"""Tests for the FOREACH clause: iterative mutation over a list.

FOREACH (variable IN list | update_clause1 update_clause2 ...)

Neo4j semantics:
  - Iterates over each element of *list* (evaluated once per outer row).
  - Binds *variable* to the current element for the inner update clauses.
  - Does not add variables to the outer frame — mutation-only side effect.
  - An empty list is a no-op.
  - Inner clauses may be: SET, CREATE, MERGE, DELETE, REMOVE.

TDD: all tests written before implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def empty_ctx() -> Context:
    """No entities at all."""
    return Context(entity_mapping=EntityMapping(mapping={}))


@pytest.fixture
def person_ctx() -> Context:
    """Single Person entity table with 2 rows."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
            "visited": [False, False],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "visited"],
        source_obj_attribute_map={"name": "name", "visited": "visited"},
        attribute_map={"name": "name", "visited": "visited"},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


# ---------------------------------------------------------------------------
# FOREACH + CREATE — create new entities from list elements
# ---------------------------------------------------------------------------


class TestForeachCreate:
    """FOREACH (x IN list | CREATE (n:Tag {val: x})) creates new nodes."""

    def test_foreach_creates_nodes_from_literal_list(
        self,
        empty_ctx: Context,
    ) -> None:
        """FOREACH with a literal list creates one node per element."""
        star = Star(context=empty_ctx)
        star.execute_query(
            "FOREACH (x IN [1, 2, 3] | CREATE (n:Num {val: x}))",
        )
        result = star.execute_query("MATCH (n:Num) RETURN n.val AS v")
        assert len(result) == 3

    def test_foreach_creates_correct_property_values(
        self,
        empty_ctx: Context,
    ) -> None:
        """Property values on created nodes equal the list elements."""
        star = Star(context=empty_ctx)
        star.execute_query("FOREACH (x IN [10, 20] | CREATE (n:Num {val: x}))")
        result = star.execute_query("MATCH (n:Num) RETURN n.val AS v")
        vals = set(result["v"].tolist())
        assert vals == {10, 20}

    def test_foreach_empty_list_is_noop(self, empty_ctx: Context) -> None:
        """An empty list produces no mutations and does not raise."""
        star = Star(context=empty_ctx)
        # No CREATE occurs; entity type 'Num' is never registered.
        result = star.execute_query(
            "FOREACH (x IN [] | CREATE (n:Num {val: x}))",
        )
        # Empty mutation returns empty DataFrame
        assert result is not None

    def test_foreach_does_not_raise(self, empty_ctx: Context) -> None:
        """Regression: FOREACH must not raise NotImplementedError."""
        star = Star(context=empty_ctx)
        result = star.execute_query(
            "FOREACH (x IN [1] | CREATE (n:Tag {name: 'test'}))",
        )
        assert result is not None


# ---------------------------------------------------------------------------
# FOREACH + SET — mutate existing nodes via list iteration
# ---------------------------------------------------------------------------


class TestForeachSet:
    """FOREACH can execute SET on outer-frame entity variables."""

    def test_foreach_set_via_outer_variable(self, person_ctx: Context) -> None:
        """FOREACH iterates; SET applies to outer-frame typed variable."""
        star = Star(context=person_ctx)
        # For each person, iterate once with a dummy list and SET p.visited.
        # 'p' is typed as Person in the outer frame and carried into the loop.
        star.execute_query(
            "MATCH (p:Person) FOREACH (ignored IN [1] | SET p.visited = true)",
        )
        result = star.execute_query("MATCH (p:Person) RETURN p.visited AS v")
        assert all(result["v"].tolist())

    def test_foreach_does_not_crash_on_set(self, person_ctx: Context) -> None:
        """Regression: FOREACH + SET must not raise."""
        star = Star(context=person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) FOREACH (x IN [1] | SET p.name = p.name)",
        )
        assert result is not None


# ---------------------------------------------------------------------------
# FOREACH in query context — outer MATCH feeds inner list
# ---------------------------------------------------------------------------


class TestForeachWithOuterMatch:
    """FOREACH can reference variables from the outer MATCH."""

    def test_foreach_creates_one_node_per_outer_row(
        self,
        person_ctx: Context,
    ) -> None:
        """Outer MATCH has N rows; FOREACH creates N nodes (one per outer row)."""
        star = Star(context=person_ctx)
        # For each Person, create a Badge node named after them
        star.execute_query(
            "MATCH (p:Person) "
            "FOREACH (ignored IN [1] | CREATE (b:Badge {owner: p.name}))",
        )
        result = star.execute_query("MATCH (b:Badge) RETURN b.owner AS owner")
        # 2 persons → 2 Badge nodes
        assert len(result) == 2
        assert set(result["owner"].tolist()) == {"Alice", "Bob"}

    def test_foreach_return_clause_after_is_valid(
        self,
        person_ctx: Context,
    ) -> None:
        """A RETURN clause following FOREACH produces the outer frame."""
        star = Star(context=person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) "
            "FOREACH (x IN [1] | CREATE (b:Badge {owner: p.name})) "
            "RETURN p.name AS name",
        )
        # The outer frame is unchanged; RETURN delivers person names.
        assert set(result["name"].tolist()) == {"Alice", "Bob"}
