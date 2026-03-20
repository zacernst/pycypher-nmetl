"""TDD tests for toString() boolean serialization.

In openCypher/Neo4j:
  toString(true)  → 'true'   (lowercase)
  toString(false) → 'false'  (lowercase)

Python's str() gives 'True'/'False' (capitalized), which is wrong.

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


@pytest.fixture()
def star() -> Star:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "flag": [True, False, None],
            "name": ["Alice", "Bob", "Carol"],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "flag", "name"],
        source_obj_attribute_map={"flag": "flag", "name": "name"},
        attribute_map={"flag": "flag", "name": "name"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


class TestToStringBoolean:
    def test_true_literal_is_lowercase(self, star: Star) -> None:
        r = star.execute_query("RETURN toString(true) AS r")
        assert r["r"].iloc[0] == "true"

    def test_false_literal_is_lowercase(self, star: Star) -> None:
        r = star.execute_query("RETURN toString(false) AS r")
        assert r["r"].iloc[0] == "false"

    def test_true_column_is_lowercase(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN toString(p.flag) AS r"
        )
        assert r["r"].iloc[0] == "true"

    def test_false_column_is_lowercase(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' RETURN toString(p.flag) AS r"
        )
        assert r["r"].iloc[0] == "false"

    def test_null_column_is_null(self, star: Star) -> None:
        r = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Carol' RETURN toString(p.flag) AS r"
        )
        assert pd.isna(r["r"].iloc[0])

    def test_integer_unchanged(self, star: Star) -> None:
        r = star.execute_query("RETURN toString(42) AS r")
        assert r["r"].iloc[0] == "42"

    def test_float_unchanged(self, star: Star) -> None:
        r = star.execute_query("RETURN toString(3.14) AS r")
        assert r["r"].iloc[0] == "3.14"

    def test_string_unchanged(self, star: Star) -> None:
        r = star.execute_query("RETURN toString('hello') AS r")
        assert r["r"].iloc[0] == "hello"

    def test_null_literal_is_null(self, star: Star) -> None:
        r = star.execute_query("RETURN toString(null) AS r")
        assert pd.isna(r["r"].iloc[0])
