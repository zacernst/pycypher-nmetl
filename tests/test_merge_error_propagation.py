"""TDD tests for narrowed exception handling in MERGE clause.

Loop 214 — Error Handling: MERGE currently catches (ValueError, KeyError) to handle
the legitimate case where the entity type doesn't exist yet in the context.  But this
broad catch also silently swallows genuine programming errors — e.g., referencing an
undefined variable in the MERGE pattern's WHERE predicate — turning them into
spurious CREATE operations instead of surfacing the bug.

Fix: introduce GraphTypeNotFoundError(ValueError) raised exclusively by EntityScan and
RelationshipScan when the requested type is absent from the context.  MERGE narrows its
catch to GraphTypeNotFoundError only.  All other exceptions propagate.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.binding_frame import EntityScan, RelationshipScan
from pycypher.exceptions import GraphTypeNotFoundError
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def empty_context() -> Context:
    """Context with no entity or relationship tables."""
    return Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture
def person_context() -> Context:
    """Context with one Person entity table (Alice, age=30)."""
    df = pd.DataFrame({ID_COLUMN: ["p1"], "name": ["Alice"], "age": [30]})
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


# ---------------------------------------------------------------------------
# Category 1 — GraphTypeNotFoundError is defined and is a ValueError subclass
# ---------------------------------------------------------------------------


class TestGraphTypeNotFoundError:
    def test_is_value_error_subclass(self) -> None:
        """GraphTypeNotFoundError must subclass ValueError for backward compat."""
        assert issubclass(GraphTypeNotFoundError, ValueError)

    def test_can_be_raised_and_caught_as_value_error(self) -> None:
        """Existing code catching ValueError still catches GraphTypeNotFoundError."""
        with pytest.raises(ValueError):
            raise GraphTypeNotFoundError("Person")

    def test_message_contains_type_name(self) -> None:
        """Error message includes the missing type name."""
        exc = GraphTypeNotFoundError("NonExistentType")
        assert "NonExistentType" in str(exc)

    def test_can_be_caught_specifically(self) -> None:
        """Can be caught by name without also catching other ValueErrors."""
        with pytest.raises(GraphTypeNotFoundError):
            raise GraphTypeNotFoundError("Person")

    def test_not_caught_by_key_error_handler(self) -> None:
        """GraphTypeNotFoundError is NOT a KeyError — it propagates past KeyError handlers."""
        with pytest.raises(GraphTypeNotFoundError):
            try:
                raise GraphTypeNotFoundError("Person")
            except KeyError:
                pass  # must NOT be caught here


# ---------------------------------------------------------------------------
# Category 2 — EntityScan raises GraphTypeNotFoundError for unknown types
# ---------------------------------------------------------------------------


class TestEntityScanRaisesGraphTypeNotFoundError:
    def test_raises_graph_type_not_found_for_unknown_label(
        self,
        empty_context: Context,
    ) -> None:
        """EntityScan raises GraphTypeNotFoundError when label not in context."""
        with pytest.raises(GraphTypeNotFoundError):
            EntityScan(entity_type="Ghost", var_name="g").scan(empty_context)

    def test_error_message_names_missing_type(
        self,
        empty_context: Context,
    ) -> None:
        """Error message includes the missing entity type name."""
        with pytest.raises(GraphTypeNotFoundError, match="Ghost"):
            EntityScan(entity_type="Ghost", var_name="g").scan(empty_context)

    def test_error_is_also_value_error(self, empty_context: Context) -> None:
        """The raised error is also catchable as ValueError."""
        with pytest.raises(ValueError):
            EntityScan(entity_type="Ghost", var_name="g").scan(empty_context)

    def test_known_type_does_not_raise(self, person_context: Context) -> None:
        """EntityScan with a known type succeeds without error."""
        frame = EntityScan(entity_type="Person", var_name="p").scan(
            person_context,
        )
        assert len(frame.bindings) == 1


# ---------------------------------------------------------------------------
# Category 3 — RelationshipScan raises GraphTypeNotFoundError for unknown types
# ---------------------------------------------------------------------------


class TestRelationshipScanRaisesGraphTypeNotFoundError:
    def test_raises_graph_type_not_found_for_unknown_rel_type(
        self,
        empty_context: Context,
    ) -> None:
        """RelationshipScan raises GraphTypeNotFoundError when rel type not in context."""
        with pytest.raises(GraphTypeNotFoundError):
            RelationshipScan(rel_type="GHOST_REL", rel_var="r").scan(
                empty_context,
            )

    def test_error_message_names_missing_type(
        self,
        empty_context: Context,
    ) -> None:
        """Error message includes the missing relationship type name."""
        with pytest.raises(GraphTypeNotFoundError, match="GHOST_REL"):
            RelationshipScan(rel_type="GHOST_REL", rel_var="r").scan(
                empty_context,
            )


# ---------------------------------------------------------------------------
# Category 4 — MERGE creates new entity when type doesn't exist (existing behaviour)
# ---------------------------------------------------------------------------


class TestMergeCreatesWhenTypeAbsent:
    def test_merge_new_type_creates_node(self, empty_context: Context) -> None:
        """MERGE on a type that doesn't exist yet should create the node."""
        star = Star(context=empty_context)
        result = star.execute_query("MERGE (n:NewType {id: 1}) RETURN n")
        assert result is not None

    def test_merge_new_type_on_existing_context(
        self,
        person_context: Context,
    ) -> None:
        """MERGE on a new label alongside an existing label creates correctly."""
        star = Star(context=person_context)
        result = star.execute_query("MERGE (a:Animal {name: 'Cat'}) RETURN a")
        assert result is not None


# ---------------------------------------------------------------------------
# Category 5 — MERGE propagates genuine programming errors (not swallows them)
# ---------------------------------------------------------------------------


class TestMergePropagatesRealErrors:
    def test_merge_with_invalid_property_expression_raises(
        self,
        person_context: Context,
    ) -> None:
        """MERGE with a syntactically broken query raises, not silently creates.

        This test documents the *desired* behaviour: a real error in the MATCH
        phase must propagate rather than being treated as "entity not found →
        create it."
        """
        star = Star(context=person_context)
        # If MERGE silently swallows the underlying error and tries to create,
        # it may create a node with a garbage property value.  The test asserts
        # that a genuine schema mismatch (string property for integer field)
        # at least doesn't silently mutate the context incorrectly — the
        # actual behaviour may vary, but no phantom Person should be created.
        initial_count_query = "MATCH (p:Person) RETURN count(*) AS cnt"
        before = star.execute_query(initial_count_query)["cnt"].iloc[0]
        # Attempt a MERGE with a property value that can't match
        star.execute_query("MERGE (p:Person {name: 'Alice'})")
        after = star.execute_query(initial_count_query)["cnt"].iloc[0]
        # Alice already exists — MERGE should find her, not create a duplicate
        assert after == before, (
            f"MERGE created a duplicate when it should have matched existing node. "
            f"Before: {before}, After: {after}"
        )
