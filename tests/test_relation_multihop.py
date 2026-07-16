"""Phase (multi-hop) — fixed-length directed paths of two or more relationships.

Verifies chained relationship patterns like (a)-[:KNOWS]->(b)-[:KNOWS]->(c) run
through the relation engine as a chain of DuckDB joins and match the pandas
oracle. Undirected / variable-length paths remain ineligible.

See docs/duckdb_out_of_core_design.md (Phase 8 generalisation).
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
    # Chain: Alice(1)->Bob(2)->Dave(4); Alice(1)->Carol(3)->Dave(4)
    persons = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
        },
    )
    knows = pd.DataFrame(
        {
            ID_COLUMN: [100, 101, 102, 103],
            "__SOURCE__": [1, 1, 2, 3],
            "__TARGET__": [2, 3, 4, 4],
        },
    )
    person = EntityTable.from_dataframe("Person", persons)
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=list(knows.columns),
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person}),
        relationship_mapping=RelationshipMapping(mapping={"KNOWS": knows_table}),
        backend=backend,
    )


def _assert_parity(query: str, sort_cols: list[str]) -> None:
    oracle = Star(context=_ctx("pandas")).execute_query(query)
    ctx = _ctx("duckdb")
    ctx._relation_engine_enabled = True
    got = Star(context=ctx).execute_query(query)
    assert set(oracle.columns) == set(got.columns)
    o = oracle.sort_values(sort_cols).reset_index(drop=True)
    g = got[oracle.columns].sort_values(sort_cols).reset_index(drop=True)
    pd.testing.assert_frame_equal(o, g, check_dtype=False)


class TestEligibility:
    def test_two_hop_eligible(self) -> None:
        assert is_relation_eligible(
            ASTConverter.from_cypher(
                "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
                "RETURN a.name AS an, c.name AS cn",
            ),
            _ctx("duckdb"),
        )

    def test_three_hop_eligible(self) -> None:
        assert is_relation_eligible(
            ASTConverter.from_cypher(
                "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person) "
                "RETURN a.name AS an, d.name AS dn",
            ),
            _ctx("duckdb"),
        )

    def test_variable_length_ineligible(self) -> None:
        assert not is_relation_eligible(
            ASTConverter.from_cypher(
                "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name AS x",
            ),
            _ctx("duckdb"),
        )


class TestParity:
    def test_two_hop(self) -> None:
        _assert_parity(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN a.name AS an, b.name AS bn, c.name AS cn",
            ["an", "bn", "cn"],
        )

    def test_two_hop_with_where(self) -> None:
        _assert_parity(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "WHERE a.name = 'Alice' RETURN a.name AS an, c.name AS cn",
            ["an", "cn"],
        )

    def test_two_hop_aggregate(self) -> None:
        _assert_parity(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "RETURN c.name AS cn, count(*) AS paths",
            ["cn"],
        )
