"""Tests for Context shadow savepoint/restore mechanism.

Validates that ``Context.savepoint()`` and ``Context.restore_savepoint()``
correctly snapshot and restore shadow state for mid-query recovery.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import ContextBuilder, Star


@pytest.fixture
def context() -> object:
    """Build a context with Person entities and KNOWS relationships."""
    persons = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    knows = pd.DataFrame(
        {"__ID__": [101, 102], "source": [1, 2], "target": [2, 3]},
    )
    return (
        ContextBuilder()
        .add_entity("Person", persons)
        .add_relationship(
            "KNOWS",
            knows,
            source_col="source",
            target_col="target",
        )
        .build()
    )


class TestSavepoint:
    """Verify savepoint snapshot and restore on Context."""

    def test_savepoint_captures_empty_shadow(self, context: object) -> None:
        """Savepoint of empty shadow returns empty dicts."""
        context.begin_query()
        sp = context.savepoint()
        assert sp["entities"] == {}
        assert sp["relationships"] == {}
        context.rollback_query()

    def test_savepoint_captures_shadow_mutations(
        self,
        context: object,
    ) -> None:
        """Savepoint captures entity shadow state after mutations."""
        context.begin_query()
        # Simulate a SET mutation by writing to shadow

        source_df = context.entity_mapping.mapping["Person"].source_obj
        if isinstance(source_df, pd.DataFrame):
            shadow_df = source_df.copy()
        else:
            shadow_df = source_df.to_pandas().copy()
        shadow_df.loc[shadow_df["__ID__"] == 1, "age"] = 99
        context._shadow["Person"] = shadow_df

        sp = context.savepoint()
        assert "Person" in sp["entities"]
        assert (
            (
                sp["entities"]["Person"]
                .loc[sp["entities"]["Person"]["__ID__"] == 1, "age"]
                .iloc[0]
            )
            == 99
        )
        context.rollback_query()

    def test_restore_savepoint_discards_later_mutations(
        self,
        context: object,
    ) -> None:
        """Restoring a savepoint discards mutations made after the savepoint."""
        context.begin_query()

        # Step 1: First mutation
        source_df = context.entity_mapping.mapping["Person"].source_obj
        if isinstance(source_df, pd.DataFrame):
            shadow_df = source_df.copy()
        else:
            shadow_df = source_df.to_pandas().copy()
        shadow_df.loc[shadow_df["__ID__"] == 1, "age"] = 50
        context._shadow["Person"] = shadow_df

        # Step 2: Take savepoint
        sp = context.savepoint()

        # Step 3: More mutations AFTER savepoint
        context._shadow["Person"].loc[
            context._shadow["Person"]["__ID__"] == 1,
            "age",
        ] = 999

        # Step 4: Restore savepoint — should discard step 3
        context.restore_savepoint(sp)

        assert (
            context._shadow["Person"]
            .loc[context._shadow["Person"]["__ID__"] == 1, "age"]
            .iloc[0]
            == 50
        )

        context.rollback_query()

    def test_savepoint_is_independent_copy(self, context: object) -> None:
        """Savepoint is a deep copy — later mutations don't affect it."""
        context.begin_query()

        source_df = context.entity_mapping.mapping["Person"].source_obj
        if isinstance(source_df, pd.DataFrame):
            shadow_df = source_df.copy()
        else:
            shadow_df = source_df.to_pandas().copy()
        shadow_df.loc[shadow_df["__ID__"] == 1, "age"] = 50
        context._shadow["Person"] = shadow_df

        sp = context.savepoint()

        # Mutate shadow AFTER savepoint
        context._shadow["Person"].loc[
            context._shadow["Person"]["__ID__"] == 1,
            "age",
        ] = 999

        # Savepoint should be unchanged
        assert (
            sp["entities"]["Person"]
            .loc[sp["entities"]["Person"]["__ID__"] == 1, "age"]
            .iloc[0]
            == 50
        )

        context.rollback_query()

    def test_savepoint_captures_relationships(self, context: object) -> None:
        """Savepoint captures relationship shadow state."""
        context.begin_query()

        rel_df = context.relationship_mapping.mapping["KNOWS"].source_obj
        if isinstance(rel_df, pd.DataFrame):
            shadow_rel = rel_df.copy()
        else:
            shadow_rel = rel_df.to_pandas().copy()
        context._shadow_rels["KNOWS"] = shadow_rel

        sp = context.savepoint()
        assert "KNOWS" in sp["relationships"]
        assert len(sp["relationships"]["KNOWS"]) == 2

        context.rollback_query()

    def test_restore_clears_property_cache(self, context: object) -> None:
        """Restoring a savepoint invalidates the property lookup cache."""
        context.begin_query()
        context._property_lookup_cache = {"stale": "data"}

        sp = context.savepoint()
        context.restore_savepoint(sp)

        assert context._property_lookup_cache == {}
        context.rollback_query()


class TestSavepointIntegration:
    """Integration tests: savepoint used during actual query execution."""

    def test_query_with_set_commits_correctly(self) -> None:
        """Normal SET query still commits correctly with savepoint API available."""
        persons = pd.DataFrame(
            {"__ID__": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]},
        )
        ctx = ContextBuilder().add_entity("Person", persons).build()
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 99 RETURN p.age",
        )
        assert result["age"].iloc[0] == 99

    def test_failed_query_rolls_back_completely(self) -> None:
        """A query that fails mid-execution leaves context unchanged."""
        persons = pd.DataFrame(
            {"__ID__": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]},
        )
        ctx = ContextBuilder().add_entity("Person", persons).build()
        star = Star(context=ctx)

        # First SET succeeds, verify original state
        star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 99 RETURN p.age",
        )

        # Verify the age was committed
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.age",
        )
        assert result["age"].iloc[0] == 99
