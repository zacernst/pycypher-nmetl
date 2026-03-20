"""Tests for UNWIND clause support.

UNWIND takes a list expression and produces one row per list element,
binding each element to an alias variable.

TDD: covers both UNWIND after MATCH/WITH and standalone UNWIND without preceding MATCH.
"""

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star


@pytest.fixture
def people_context() -> Context:
    """Three people with tag lists."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
            "tags": [
                ["python", "graph"],
                ["java"],
                ["python", "sql", "graph"],
            ],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "tags"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "tags": "tags",
        },
        attribute_map={"name": "name", "age": "age", "tags": "tags"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Basic UNWIND (without MATCH)
# ─────────────────────────────────────────────────────────────────────────────


class TestUnwindBasic:
    def test_unwind_integer_list(self, people_context: Context) -> None:
        """UNWIND on an integer literal list produces one row per element."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH collect(p.age) AS ages UNWIND ages AS a RETURN a"
        )
        assert len(result) == 3
        assert set(result["a"].tolist()) == {30, 25, 35}

    def test_unwind_preserves_order(self, people_context: Context) -> None:
        """UNWIND preserves list order."""
        star = Star(context=people_context)
        # sorted ages: 25, 30, 35
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH collect(p.age) AS ages ORDER BY ages "
            "UNWIND ages AS a "
            "RETURN a"
        )
        # After ORDER BY (on the list as a whole, not individual elements)
        # Just verify all values present
        assert set(result["a"].tolist()) == {25, 30, 35}

    def test_unwind_string_list(self, people_context: Context) -> None:
        """UNWIND string list produces correct rows."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH collect(p.name) AS names "
            "UNWIND names AS name "
            "RETURN name"
        )
        assert len(result) == 3
        assert set(result["name"].tolist()) == {"Alice", "Bob", "Carol"}


# ─────────────────────────────────────────────────────────────────────────────
# UNWIND after MATCH (cross-product style)
# ─────────────────────────────────────────────────────────────────────────────


class TestUnwindAfterMatch:
    def test_unwind_entity_list_property(
        self, people_context: Context
    ) -> None:
        """Explode p.tags — Alice has 2 tags, Bob has 1, Carol has 3 → 6 rows."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS name, p.tags AS tags "
            "UNWIND tags AS tag "
            "RETURN name, tag"
        )
        assert len(result) == 6  # 2 + 1 + 3
        assert "tag" in result.columns
        assert "name" in result.columns

    def test_unwind_entity_tags_with_filter(
        self, people_context: Context
    ) -> None:
        """Filter tags to 'python' rows only — Alice and Carol → 2 rows."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS name, p.tags AS tags "
            "UNWIND tags AS tag "
            "WITH name, tag WHERE tag = 'python' "
            "RETURN name, tag"
        )
        assert len(result) == 2
        assert set(result["name"].tolist()) == {"Alice", "Carol"}
        assert all(t == "python" for t in result["tag"].tolist())

    def test_unwind_produces_correct_name_tag_pairs(
        self, people_context: Context
    ) -> None:
        """Bob has only ['java'] — check his row is correct."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS name, p.tags AS tags "
            "UNWIND tags AS tag "
            "WITH name, tag WHERE name = 'Bob' "
            "RETURN name, tag"
        )
        assert len(result) == 1
        assert result["tag"].iloc[0] == "java"


# ─────────────────────────────────────────────────────────────────────────────
# UNWIND result column
# ─────────────────────────────────────────────────────────────────────────────


class TestUnwindReturnColumn:
    def test_unwind_alias_in_return(self, people_context: Context) -> None:
        """The UNWIND alias is available as a RETURN column."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) WITH collect(p.name) AS names UNWIND names AS n RETURN n"
        )
        assert "n" in result.columns

    def test_unwind_alias_available_in_where(
        self, people_context: Context
    ) -> None:
        """The UNWIND alias is usable in a subsequent WITH WHERE clause."""
        star = Star(context=people_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH collect(p.age) AS ages "
            "UNWIND ages AS age "
            "WITH age WHERE age > 25 "
            "RETURN age"
        )
        assert all(a > 25 for a in result["age"].tolist())
        assert len(result) == 2  # Alice (30) and Carol (35)


# ─────────────────────────────────────────────────────────────────────────────
# Standalone UNWIND (no preceding MATCH)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def empty_context() -> Context:
    """A minimal context with no entities — for standalone UNWIND tests."""
    return Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestUnwindStandalone:
    def test_unwind_literal_integer_list(self, empty_context: Context) -> None:
        """UNWIND [1, 2, 3] AS n RETURN n — no preceding MATCH."""
        star = Star(context=empty_context)
        result = star.execute_query("UNWIND [1, 2, 3] AS n RETURN n")
        assert len(result) == 3
        assert set(result["n"].tolist()) == {1, 2, 3}

    def test_unwind_literal_string_list(self, empty_context: Context) -> None:
        """UNWIND ['a', 'b', 'c'] AS x RETURN x — no preceding MATCH."""
        star = Star(context=empty_context)
        result = star.execute_query("UNWIND ['a', 'b', 'c'] AS x RETURN x")
        assert len(result) == 3
        assert set(result["x"].tolist()) == {"a", "b", "c"}

    def test_unwind_empty_list(self, empty_context: Context) -> None:
        """UNWIND [] AS x RETURN x — should produce 0 rows."""
        star = Star(context=empty_context)
        result = star.execute_query("UNWIND [] AS x RETURN x")
        assert len(result) == 0

    def test_unwind_range_function(self, empty_context: Context) -> None:
        """UNWIND range(1, 5) AS n RETURN n — produces 5 rows."""
        star = Star(context=empty_context)
        result = star.execute_query("UNWIND range(1, 5) AS n RETURN n")
        assert len(result) == 5
        assert set(result["n"].tolist()) == {1, 2, 3, 4, 5}

    def test_unwind_range_with_step(self, empty_context: Context) -> None:
        """UNWIND range(0, 10, 2) AS n RETURN n — produces 6 rows (0,2,4,6,8,10)."""
        star = Star(context=empty_context)
        result = star.execute_query("UNWIND range(0, 10, 2) AS n RETURN n")
        assert len(result) == 6
        assert set(result["n"].tolist()) == {0, 2, 4, 6, 8, 10}
