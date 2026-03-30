"""TDD tests for SET n = expr and SET n += expr where expr is not a literal map.

The current implementation handles:
    SET n = {key: val}   (MapLiteral)  ✓ already works
    SET n += {key: val}  (MapLiteral)  ✓ already works

But silently does nothing for:
    SET n = $props       (Parameter)   ✗ broken
    SET n += $props      (Parameter)   ✗ broken
    SET n = m            (Variable)    ✗ broken

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
from pycypher.star import Star


@pytest.fixture
def person_star() -> Star:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        },
    )
    t = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": t}),
            relationship_mapping=RelationshipMapping(),
        ),
    )


# ---------------------------------------------------------------------------
# SET n = $props  (parameter dict)
# ---------------------------------------------------------------------------


class TestSetAllPropertiesFromParameter:
    """SET n = $props where $props is a dict parameter must merge properties."""

    def test_set_all_from_param_new_property(self, person_star: Star) -> None:
        """SET p = $props adds properties from the parameter dict."""
        r = person_star.execute_query(
            "MATCH (p:Person) SET p = $props RETURN p.status AS status",
            parameters={"props": {"status": "active"}},
        )
        assert list(r["status"]) == ["active", "active"], (
            f"Expected ['active', 'active'], got {list(r['status'])}"
        )

    def test_set_all_from_param_multiple_props(
        self,
        person_star: Star,
    ) -> None:
        """SET p = $props with multiple keys sets all of them."""
        r = person_star.execute_query(
            "MATCH (p:Person) SET p = $props RETURN p.role AS role, p.level AS level",
            parameters={"props": {"role": "engineer", "level": 3}},
        )
        assert list(r["role"]) == ["engineer", "engineer"]
        assert [int(x) for x in r["level"]] == [3, 3]

    def test_set_all_from_param_does_not_raise(
        self,
        person_star: Star,
    ) -> None:
        """SET p = $props must not raise for a valid dict parameter."""
        person_star.execute_query(
            "MATCH (p:Person) SET p = $props RETURN p.status",
            parameters={"props": {"status": "ok"}},
        )


# ---------------------------------------------------------------------------
# SET n += $props  (parameter dict, merge)
# ---------------------------------------------------------------------------


class TestAddAllPropertiesFromParameter:
    """SET n += $props preserves existing properties and adds new ones."""

    def test_add_from_param_new_property(self, person_star: Star) -> None:
        """SET p += $props adds the new property without removing existing ones."""
        r = person_star.execute_query(
            "MATCH (p:Person) SET p += $props "
            "RETURN p.name AS name, p.score AS score "
            "ORDER BY p.name",
            parameters={"props": {"score": 100}},
        )
        assert list(r["name"]) == ["Alice", "Bob"]
        assert list(r["score"]) == [100, 100]

    def test_add_from_param_preserves_existing(
        self,
        person_star: Star,
    ) -> None:
        """SET p += $props keeps name and age intact."""
        r = person_star.execute_query(
            "MATCH (p:Person) SET p += $props "
            "RETURN p.name AS name, p.age AS age ORDER BY p.name",
            parameters={"props": {"score": 50}},
        )
        assert list(r["name"]) == ["Alice", "Bob"]
        assert [int(x) for x in r["age"]] == [30, 25]

    def test_add_from_param_does_not_raise(self, person_star: Star) -> None:
        """SET p += $props must not raise."""
        person_star.execute_query(
            "MATCH (p:Person) SET p += $props RETURN p.name",
            parameters={"props": {"flag": True}},
        )


# ---------------------------------------------------------------------------
# Regression: literal map still works after the fix
# ---------------------------------------------------------------------------


class TestSetMapLiteralRegression:
    """Literal map assignment must not regress after the parameter fix."""

    def test_literal_map_still_works(self, person_star: Star) -> None:
        """SET p = {status: 'ok'} (literal) must still set the property."""
        r = person_star.execute_query(
            "MATCH (p:Person) SET p = {status: 'ok'} RETURN p.status AS status",
        )
        assert list(r["status"]) == ["ok", "ok"]

    def test_literal_merge_still_works(self, person_star: Star) -> None:
        """SET p += {status: 'ok'} (literal) must still set the property."""
        r = person_star.execute_query(
            "MATCH (p:Person) SET p += {score: 99} "
            "RETURN p.name AS name, p.score AS score ORDER BY p.name",
        )
        assert list(r["name"]) == ["Alice", "Bob"]
        assert list(r["score"]) == [99, 99]
