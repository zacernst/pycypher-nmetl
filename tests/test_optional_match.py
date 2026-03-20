"""Tests for OPTIONAL MATCH clause support.

OPTIONAL MATCH implements left-join semantics: rows from the preceding MATCH
are preserved even when the optional pattern has no matches, binding unmatched
variables to NULL.

TDD red phase → green phase.
"""

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star


@pytest.fixture
def knows_context() -> Context:
    """Three people; Alice knows Bob, Bob knows Carol, Carol knows nobody."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
        }
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )

    # KNOWS relationships: Alice(1)->Bob(2), Bob(2)->Carol(3)
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [101, 102],
            "__SOURCE__": [1, 2],
            "__TARGET__": [2, 3],
        }
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
    )

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": people_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )


@pytest.fixture
def simple_context() -> Context:
    """Two people, no relationships — for basic OPTIONAL MATCH tests."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        }
    )
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


# ─────────────────────────────────────────────────────────────────────────────
# Basic OPTIONAL MATCH (standalone, no preceding MATCH)
# ─────────────────────────────────────────────────────────────────────────────


class TestOptionalMatchBasic:
    def test_optional_match_as_first_clause_finds_results(
        self, simple_context: Context
    ) -> None:
        """OPTIONAL MATCH as first clause works like regular MATCH when rows exist."""
        star = Star(context=simple_context)
        result = star.execute_query("OPTIONAL MATCH (p:Person) RETURN p.name")
        assert len(result) == 2
        assert set(result["name"].tolist()) == {"Alice", "Bob"}

    def test_optional_match_as_first_clause_nonexistent_type(
        self, simple_context: Context
    ) -> None:
        """OPTIONAL MATCH on a nonexistent entity type returns 0 rows as first clause."""
        star = Star(context=simple_context)
        result = star.execute_query("OPTIONAL MATCH (p:Robot) RETURN p.name")
        assert len(result) == 0


# ─────────────────────────────────────────────────────────────────────────────
# OPTIONAL MATCH after MATCH — left-join semantics
# ─────────────────────────────────────────────────────────────────────────────


class TestOptionalMatchLeftJoin:
    def test_optional_match_preserves_all_left_rows(
        self, knows_context: Context
    ) -> None:
        """MATCH then OPTIONAL MATCH — all 3 persons returned even if no KNOWS match."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) RETURN p.name"
        )
        # All 3 persons should appear (Carol too, even with no outgoing KNOWS)
        assert len(result) == 3
        assert set(result["name"].tolist()) == {"Alice", "Bob", "Carol"}

    def test_optional_match_row_count_with_relationship(
        self, knows_context: Context
    ) -> None:
        """Rows where optional match found something have correct count."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
            "WITH p.name AS pname, f.name AS fname "
            "WHERE fname IS NOT NULL "
            "RETURN pname, fname"
        )
        # Only Alice->Bob and Bob->Carol have KNOWS edges
        assert len(result) == 2
        pairs = set(zip(result["pname"].tolist(), result["fname"].tolist()))
        assert ("Alice", "Bob") in pairs
        assert ("Bob", "Carol") in pairs

    def test_optional_match_null_for_unmatched_rows(
        self, knows_context: Context
    ) -> None:
        """Carol has no outgoing KNOWS — her f variable is null/None."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
            "WITH p.name AS pname, f.name AS fname "
            "WHERE pname = 'Carol' "
            "RETURN pname, fname"
        )
        assert len(result) == 1
        fname_val = result["fname"].iloc[0]
        # Should be None / NaN (null in pandas)
        assert fname_val is None or (
            isinstance(fname_val, float) and pd.isna(fname_val)
        )


# ─────────────────────────────────────────────────────────────────────────────
# NULL handling in OPTIONAL MATCH results
# ─────────────────────────────────────────────────────────────────────────────


class TestOptionalMatchNullHandling:
    def test_is_null_filters_unmatched_rows(
        self, knows_context: Context
    ) -> None:
        """WHERE fname IS NULL returns only Carol (no outgoing KNOWS)."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
            "WITH p.name AS pname, f.name AS fname "
            "WHERE fname IS NULL "
            "RETURN pname"
        )
        assert len(result) == 1
        assert result["pname"].iloc[0] == "Carol"

    def test_is_not_null_filters_matched_rows(
        self, knows_context: Context
    ) -> None:
        """WHERE fname IS NOT NULL returns Alice and Bob (have outgoing KNOWS)."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
            "WITH p.name AS pname, f.name AS fname "
            "WHERE fname IS NOT NULL "
            "RETURN pname"
        )
        assert len(result) == 2
        assert set(result["pname"].tolist()) == {"Alice", "Bob"}
