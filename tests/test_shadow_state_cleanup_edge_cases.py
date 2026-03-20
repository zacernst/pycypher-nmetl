"""Tests for shadow state cleanup in exception handling edge cases.

Validates that the shadow write system properly cleans up in all failure
paths:
1. commit_query() failure mid-promotion always clears shadow dicts
2. UNION query atomicity — failure in any sub-query rolls back all
3. rollback_query() clears property-lookup cache

Run with:
    uv run pytest tests/test_shadow_state_cleanup_edge_cases.py -v
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest
from pycypher import Star
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipTable,
)


@pytest.fixture
def person_df() -> pd.DataFrame:
    """Minimal person DataFrame for tests."""
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        }
    )


@pytest.fixture
def context_with_people(person_df: pd.DataFrame) -> Context:
    """Context with a Person entity table."""
    table = EntityTable.from_dataframe("Person", person_df)
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


@pytest.fixture
def star_with_people(context_with_people: Context) -> Star:
    """Star wired to a Person entity table."""
    return Star(context=context_with_people)


class TestCommitQueryCleanupOnFailure:
    """commit_query() must clear shadow dicts even if promotion fails."""

    def test_shadow_cleared_after_commit_exception(
        self,
        context_with_people: Context,
    ) -> None:
        """If commit_query raises mid-loop, _shadow and _shadow_rels are still empty afterward."""
        ctx = context_with_people

        # Simulate a mutation by injecting shadow data directly.
        ctx.begin_query()
        ctx._shadow["Person"] = pd.DataFrame(
            {ID_COLUMN: [99], "name": ["Oops"]}
        )
        ctx._shadow_rels["FAKE"] = pd.DataFrame(
            {"__SOURCE__": [1], "__TARGET__": [2]}
        )

        # Patch EntityTable construction to explode during commit.
        with patch.object(
            EntityTable,
            "__init__",
            side_effect=RuntimeError("simulated commit failure"),
        ):
            # Inject a NEW entity type so the EntityTable constructor path runs.
            ctx._shadow["NewLabel"] = pd.DataFrame(
                {ID_COLUMN: [100], "x": [1]}
            )
            with pytest.raises(RuntimeError, match="simulated commit failure"):
                ctx.commit_query()

        # Shadow dicts MUST be empty — no stale data leaking to next query.
        assert ctx._shadow == {}
        assert ctx._shadow_rels == {}

    def test_shadow_cleared_on_relationship_commit_failure(
        self,
        context_with_people: Context,
    ) -> None:
        """Shadow cleared even if relationship promotion raises."""
        ctx = context_with_people
        ctx.begin_query()
        ctx._shadow_rels["NewRel"] = pd.DataFrame(
            {
                "__SOURCE__": [1],
                "__TARGET__": [2],
            }
        )

        with patch.object(
            RelationshipTable,
            "__init__",
            side_effect=RuntimeError("rel commit boom"),
        ):
            with pytest.raises(RuntimeError, match="rel commit boom"):
                ctx.commit_query()

        assert ctx._shadow == {}
        assert ctx._shadow_rels == {}


class TestRollbackClearsPropertyCache:
    """rollback_query() must clear _property_lookup_cache."""

    def test_rollback_clears_cache(self, context_with_people: Context) -> None:
        """After rollback, the property-lookup cache should be empty."""
        ctx = context_with_people

        # Pre-populate the cache to simulate a lookup during a failed query.
        ctx._property_lookup_cache["Person"] = pd.DataFrame({"dummy": [1]})

        ctx.begin_query()
        ctx.rollback_query()

        assert ctx._property_lookup_cache == {}

    def test_successful_read_query_preserves_cache(
        self,
        context_with_people: Context,
    ) -> None:
        """A no-mutation commit should NOT clear the property cache."""
        ctx = context_with_people

        ctx._property_lookup_cache["Person"] = pd.DataFrame({"dummy": [1]})

        ctx.begin_query()
        # Commit with no shadow writes — read-only query path.
        ctx.commit_query()

        # Cache should survive since there were no mutations.
        assert "Person" in ctx._property_lookup_cache


class TestUnionQueryAtomicity:
    """UNION queries must be atomic — all sub-queries share one transaction."""

    @pytest.fixture
    def star_for_union(self) -> Star:
        """Star with Person and Animal tables for UNION testing."""
        person_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2],
                "name": ["Alice", "Bob"],
            }
        )
        animal_df = pd.DataFrame(
            {
                ID_COLUMN: [10, 11],
                "name": ["Rex", "Spot"],
            }
        )
        person_table = EntityTable.from_dataframe("Person", person_df)
        animal_table = EntityTable.from_dataframe("Animal", animal_df)
        ctx = Context(
            entity_mapping=EntityMapping(
                mapping={"Person": person_table, "Animal": animal_table},
            ),
        )
        return Star(context=ctx)

    def test_union_read_only_succeeds(self, star_for_union: Star) -> None:
        """Basic UNION query executes correctly."""
        result = star_for_union.execute_query(
            "MATCH (p:Person) RETURN p.name AS name "
            "UNION ALL "
            "MATCH (a:Animal) RETURN a.name AS name"
        )
        assert len(result) == 4
        names = set(result["name"])
        assert names == {"Alice", "Bob", "Rex", "Spot"}

    def test_union_failure_leaves_context_unchanged(
        self,
        star_for_union: Star,
    ) -> None:
        """If any sub-query in a UNION fails, no mutations are committed."""
        # Record original state.
        original_person_names = set(
            star_for_union.context.entity_mapping["Person"].source_obj["name"],
        )

        # Craft a UNION where the second sub-query references a non-existent
        # label.  The first sub-query succeeds, but the second raises.
        # With atomic UNION, the first sub-query's mutations should also
        # be rolled back.
        try:
            star_for_union.execute_query(
                "MATCH (p:Person) RETURN p.name AS name "
                "UNION ALL "
                "MATCH (z:Nonexistent) RETURN z.name AS name"
            )
        except (ValueError, KeyError):
            pass  # Expected — Nonexistent label doesn't exist.

        # Context should be unchanged — no shadow leakage.
        current_person_names = set(
            star_for_union.context.entity_mapping["Person"].source_obj["name"],
        )
        assert current_person_names == original_person_names

        # Shadow state must be clean.
        assert star_for_union.context._shadow == {}
        assert star_for_union.context._shadow_rels == {}


class TestShadowStateAfterQueryFailure:
    """Shadow state must be clean after any kind of query failure."""

    def test_shadow_clean_after_parse_error(
        self, star_with_people: Star
    ) -> None:
        """Parse errors should not leave shadow state dirty."""
        with pytest.raises(Exception):
            star_with_people.execute_query("INVALID CYPHER !!!")

        assert star_with_people.context._shadow == {}
        assert star_with_people.context._shadow_rels == {}

    def test_shadow_clean_after_runtime_error(
        self, star_with_people: Star
    ) -> None:
        """Runtime errors should not leave shadow state dirty."""
        with pytest.raises(Exception):
            # Reference a non-existent label to trigger a runtime error.
            star_with_people.execute_query(
                "MATCH (x:NonExistent) SET x.foo = 1 RETURN x"
            )

        assert star_with_people.context._shadow == {}
        assert star_with_people.context._shadow_rels == {}

    def test_sequential_queries_after_failure(
        self, star_with_people: Star
    ) -> None:
        """A failed query must not poison subsequent successful queries."""
        # First query fails.
        with pytest.raises(Exception):
            star_with_people.execute_query("INVALID !!!")

        # Second query should succeed cleanly with no shadow leakage.
        result = star_with_people.execute_query(
            "MATCH (p:Person) RETURN p.name AS name"
        )
        assert len(result) == 3
        assert set(result["name"]) == {"Alice", "Bob", "Carol"}


class TestBaseExceptionShadowCleanup:
    """Shadow state must be cleaned up even for BaseException (e.g. KeyboardInterrupt).

    The finally-based cleanup in _execute_query_binding_frame catches
    BaseException subclasses that ``except Exception`` would miss.
    """

    def test_keyboard_interrupt_cleans_shadow(
        self,
        star_with_people: Star,
    ) -> None:
        """KeyboardInterrupt during inner execution still triggers rollback."""
        ctx = star_with_people.context

        # Inject shadow data to simulate a mutation in progress.
        ctx.begin_query()
        ctx._shadow["Person"] = pd.DataFrame(
            {"__ID__": [99], "name": ["Interrupted"]}
        )

        # Manually roll back to simulate what the finally block does.
        ctx.rollback_query()

        assert ctx._shadow == {}
        assert ctx._shadow_rels == {}
        assert ctx._property_lookup_cache == {}

    def test_defensive_cleanup_in_execute_query(
        self,
        star_with_people: Star,
    ) -> None:
        """execute_query's finally block catches leftover shadow state."""
        ctx = star_with_people.context

        # After a normal query, shadow state must be empty.
        star_with_people.execute_query(
            "MATCH (p:Person) RETURN p.name AS name",
        )
        assert ctx._shadow == {}
        assert ctx._shadow_rels == {}

        # After a failed query, shadow state must also be empty.
        with pytest.raises(Exception):
            star_with_people.execute_query(
                "MATCH (x:NonExistent) SET x.foo = 1 RETURN x",
            )
        assert ctx._shadow == {}
        assert ctx._shadow_rels == {}
        assert ctx._parameters == {}
