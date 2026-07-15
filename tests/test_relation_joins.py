"""Phase 8 (out-of-core DuckDB) — relationship MATCH via SQL joins.

Verifies single directed relationship patterns run through the relation engine
as DuckDB joins and match the pandas oracle: both directions, same-label
endpoints, relationship-variable property access, and WHERE over multiple
variables.  Multi-hop / undirected / OPTIONAL remain ineligible (fall back).

See docs/duckdb_out_of_core_design.md, Phase 8.
"""

from __future__ import annotations

import pandas as pd
import pytest
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
    # Alice(1)->Bob(2), Alice(1)->Carol(3), Bob(2)->Dave(4); Eve(5) isolated.
    persons = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [30, 25, 35, 28, 22],
        },
    )
    knows = pd.DataFrame(
        {
            ID_COLUMN: [100, 101, 102],
            "__SOURCE__": [1, 1, 2],
            "__TARGET__": [2, 3, 4],
            "since": [2020, 2021, 2019],
        },
    )
    person = EntityTable.from_dataframe("Person", persons)
    knows_table = RelationshipTable(
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
    def test_directed_rel_eligible(self) -> None:
        assert is_relation_eligible(
            ASTConverter.from_cypher(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS an, b.name AS bn",
            ),
            _ctx("duckdb"),
        )

    @pytest.mark.parametrize(
        "query",
        [
            # multi-hop (2 rels) — not yet supported
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.name AS x",
            # undirected — not supported
            "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.name AS x, b.name AS y",
            # variable-length — not supported
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name AS x",
            # unknown relationship label
            "MATCH (a:Person)-[:WORKS_WITH]->(b:Person) RETURN a.name AS x, b.name AS y",
        ],
    )
    def test_ineligible_rel_shapes(self, query: str) -> None:
        assert not is_relation_eligible(ASTConverter.from_cypher(query), _ctx("duckdb"))

    def test_bare_join_returns_eligible_qualified(self) -> None:
        # Bare property lookups in a join are named var.property (like the
        # pandas engine), so they don't collide and are eligible.
        assert is_relation_eligible(
            ASTConverter.from_cypher("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"),
            _ctx("duckdb"),
        )

    def test_duplicate_output_alias_ineligible(self) -> None:
        # Two returns explicitly aliased to the same name collide → ineligible.
        assert not is_relation_eligible(
            ASTConverter.from_cypher(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS x, b.name AS x",
            ),
            _ctx("duckdb"),
        )

    def test_bare_join_return_parity(self) -> None:
        # Column names are qualified (a.name / b.name), matching the oracle.
        _assert_parity(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
            ["a.name", "b.name"],
        )


class TestParity:
    def test_directed_right(self) -> None:
        _assert_parity(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS an, b.name AS bn",
            ["an", "bn"],
        )

    def test_directed_left(self) -> None:
        _assert_parity(
            "MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a.name AS an, b.name AS bn",
            ["an", "bn"],
        )

    def test_where_over_two_vars(self) -> None:
        _assert_parity(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE a.age > 26 AND b.age < 30 RETURN a.name AS an, b.name AS bn",
            ["an", "bn"],
        )

    def test_rel_variable_property(self) -> None:
        _assert_parity(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name AS an, b.name AS bn, r.since AS since",
            ["an", "bn"],
        )

    def test_projection_expression_over_join(self) -> None:
        _assert_parity(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "RETURN a.name AS an, b.age + 1 AS bage",
            ["an", "bage"],
        )
