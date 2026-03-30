"""Integration tests for NullCheck and StringPredicate WHERE clause predicates.

Tests via Star.execute_query() (end-to-end via BindingFrame path).

Dataset (six rows):

  id  name    email                    tag      score
   1  Alice   alice@example.com        admin     85
   2  Bob     bob@test.org             user      72
   3  Carol   carol@example.com        admin     91
   4  None    dave@test.org            guest     68      <- name IS NULL
   5  Eve     None                     user      79      <- email IS NULL
   6  Frank   frank@example.com        guest     55
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
def pred_df():
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Carol", None, "Eve", "Frank"],
            "email": [
                "alice@example.com",
                "bob@test.org",
                "carol@example.com",
                "dave@test.org",
                None,
                "frank@example.com",
            ],
            "tag": ["admin", "user", "admin", "guest", "user", "guest"],
            "score": [85, 72, 91, 68, 79, 55],
        },
    )


@pytest.fixture
def pred_context(pred_df):
    attr_map = {
        "name": "name",
        "email": "email",
        "tag": "tag",
        "score": "score",
    }
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "email", "tag", "score"],
        source_obj_attribute_map=attr_map,
        attribute_map=attr_map,
        source_obj=pred_df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


def execute_cypher(context: Context, where_clause: str) -> list[str]:
    star = Star(context=context)
    result = star.execute_query(
        f"MATCH (p:Person) WHERE {where_clause} RETURN p.name AS name",
    )
    return sorted(result["name"].dropna().tolist())


# ===========================================================================
# Integration tests — via Star.execute_query()
# ===========================================================================


class TestNullCheckIntegration:
    """End-to-end NullCheck tests through the full query pipeline."""

    def test_where_is_null(self, pred_context):
        star = Star(context=pred_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name IS NULL RETURN p.email AS email",
        )
        assert result["email"].tolist() == ["dave@test.org"]

    def test_where_is_not_null(self, pred_context):
        star = Star(context=pred_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name IS NOT NULL RETURN p.name AS name",
        )
        assert sorted(result["name"].tolist()) == [
            "Alice",
            "Bob",
            "Carol",
            "Eve",
            "Frank",
        ]

    def test_where_is_null_combined_with_comparison(self, pred_context):
        # name IS NULL OR score < 60
        star = Star(context=pred_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name IS NULL OR p.score < 60 RETURN p.score AS score",
        )
        # name IS NULL → id 4 (score 68); score < 60 → id 6 (score 55)
        assert sorted(result["score"].tolist()) == [55, 68]

    def test_where_is_not_null_and_comparison(self, pred_context):
        # email IS NOT NULL AND score > 80
        star = Star(context=pred_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.email IS NOT NULL AND p.score > 80 RETURN p.name AS name",
        )
        # email not null: 1,2,3,4,6; score>80: 1(85),3(91) → 1,3
        assert sorted(result["name"].tolist()) == ["Alice", "Carol"]


class TestStringPredicateIntegration:
    """End-to-end StringPredicate tests through the full query pipeline."""

    def test_where_starts_with(self, pred_context):
        names = execute_cypher(pred_context, "p.name STARTS WITH 'A'")
        assert names == ["Alice"]

    def test_where_ends_with(self, pred_context):
        names = execute_cypher(pred_context, "p.name ENDS WITH 'e'")
        assert names == ["Alice", "Eve"]

    def test_where_contains(self, pred_context):
        # Bob (B-o-b) and Carol (C-a-r-o-l) contain "o"; Frank (F-r-a-n-k) does not
        names = execute_cypher(pred_context, "p.name CONTAINS 'o'")
        assert names == ["Bob", "Carol"]

    def test_where_regex(self, pred_context):
        names = execute_cypher(pred_context, "p.name =~ '.*ol'")
        assert names == ["Carol"]

    def test_where_in_string_list(self, pred_context):
        names = execute_cypher(pred_context, "p.tag IN ['admin', 'guest']")
        # null name (id 4) → dropna() in execute_cypher excludes it from name list
        assert names == ["Alice", "Carol", "Frank"]

    def test_where_in_integer_list(self, pred_context):
        star = Star(context=pred_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.score IN [85, 91, 72] RETURN p.name AS name",
        )
        assert sorted(result["name"].tolist()) == ["Alice", "Bob", "Carol"]

    def test_where_starts_with_and_is_not_null(self, pred_context):
        names = execute_cypher(
            pred_context,
            "p.email STARTS WITH 'alice' AND p.name IS NOT NULL",
        )
        assert names == ["Alice"]

    def test_where_contains_or_is_null(self, pred_context):
        star = Star(context=pred_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name CONTAINS 'a' OR p.email IS NULL RETURN p.score AS score",
        )
        # contains-a: Carol(91), Frank(55); email null: Eve(79) → scores 55,79,91
        assert sorted(result["score"].tolist()) == [55, 79, 91]

    def test_where_in_with_comparison(self, pred_context):
        names = execute_cypher(
            pred_context,
            "p.tag IN ['user'] AND p.score > 75",
        )
        # user: Bob(72), Eve(79); >75: Eve(79) only
        assert names == ["Eve"]

    def test_where_ends_with_email_domain(self, pred_context):
        star = Star(context=pred_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.email ENDS WITH '.org' RETURN p.email AS email",
        )
        assert sorted(result["email"].tolist()) == [
            "bob@test.org",
            "dave@test.org",
        ]
