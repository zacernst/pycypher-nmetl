"""OPTIONAL MATCH → LEFT JOIN (out-of-core).

Verifies OPTIONAL MATCH extends the pattern via a LEFT join (unmatched rows keep
the bound side with nulls on the optional side), matching the pandas oracle.
A second required MATCH, an optional pattern from an unbound node, and
aggregation combined with OPTIONAL remain ineligible (fall back).
"""

from __future__ import annotations

import pandas as pd
from pycypher.ast_converter import ASTConverter
from pycypher.relation_engine import is_relation_eligible
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

ID_COLUMN = "__ID__"


def _ctx(backend: str) -> Context:
    # Alice(1)->Bob(2), Alice(1)->Carol(3); Bob/Carol/Eve have no outgoing KNOWS.
    persons = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3, 4], "name": ["Alice", "Bob", "Carol", "Eve"]},
    )
    knows = pd.DataFrame(
        {ID_COLUMN: [10, 11], "__SOURCE__": [1, 1], "__TARGET__": [2, 3], "since": [2020, 2021]},
    )
    person = EntityTable.from_dataframe("Person", persons)
    knows_t = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=list(knows.columns),
        source_obj_attribute_map={"since": "since"},
        attribute_map={"since": "since"},
        source_obj=knows,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person}),
        relationship_mapping=RelationshipMapping(mapping={"KNOWS": knows_t}),
        backend=backend,
    )


def _records(df: pd.DataFrame) -> list[dict]:
    return [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in df.to_dict("records")
    ]


def _assert_parity(query: str, sort_cols: list[str]) -> None:
    oracle = Star(context=_ctx("pandas")).execute_query(query)
    ctx = _ctx("duckdb")
    ctx._relation_engine_enabled = True
    got = Star(context=ctx).execute_query(query)
    assert set(oracle.columns) == set(got.columns), (list(oracle.columns), list(got.columns))
    got = got[oracle.columns]
    o = _records(oracle.sort_values(sort_cols, na_position="last").reset_index(drop=True))
    g = _records(got.sort_values(sort_cols, na_position="last").reset_index(drop=True))
    assert o == g


class TestEligibility:
    def test_optional_eligible(self) -> None:
        assert is_relation_eligible(
            ASTConverter.from_cypher(
                "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a.name AS an, b.name AS bn",
            ),
            _ctx("duckdb"),
        )

    def test_second_required_match_ineligible(self) -> None:
        assert not is_relation_eligible(
            ASTConverter.from_cypher(
                "MATCH (a:Person) MATCH (b:Person) RETURN a.name AS an, b.name AS bn",
            ),
            _ctx("duckdb"),
        )

    def test_optional_from_unbound_node_ineligible(self) -> None:
        # Left node of the optional pattern is not already bound.
        assert not is_relation_eligible(
            ASTConverter.from_cypher(
                "MATCH (a:Person) OPTIONAL MATCH (x:Person)-[:KNOWS]->(y:Person) RETURN a.name AS an",
            ),
            _ctx("duckdb"),
        )

    def test_optional_with_aggregation_ineligible(self) -> None:
        assert not is_relation_eligible(
            ASTConverter.from_cypher(
                "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a.name AS an, count(b) AS c",
            ),
            _ctx("duckdb"),
        )


class TestParity:
    def test_basic_optional(self) -> None:
        _assert_parity(
            "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) "
            "RETURN a.name AS an, b.name AS bn",
            ["an", "bn"],
        )

    def test_optional_left_direction(self) -> None:
        _assert_parity(
            "MATCH (a:Person) OPTIONAL MATCH (a)<-[:KNOWS]-(b:Person) "
            "RETURN a.name AS an, b.name AS bn",
            ["an", "bn"],
        )

    def test_optional_rel_variable(self) -> None:
        _assert_parity(
            "MATCH (a:Person) OPTIONAL MATCH (a)-[r:KNOWS]->(b:Person) "
            "RETURN a.name AS an, b.name AS bn, r.since AS since",
            ["an", "bn"],
        )

    def test_leading_where_plus_optional(self) -> None:
        _assert_parity(
            "MATCH (a:Person) WHERE a.name <> 'Eve' "
            "OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a.name AS an, b.name AS bn",
            ["an", "bn"],
        )
