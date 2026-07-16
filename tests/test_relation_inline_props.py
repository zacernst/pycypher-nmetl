"""Inline node properties (out-of-core) — MATCH (n:Label {prop: value}).

Verifies inline node properties desugar to equality predicates and match the
pandas oracle, including in multi-hop patterns and combined with WHERE.
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
    persons = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "dept": ["eng", "eng", "mktg", "eng"],
            "active": [True, False, True, True],
        },
    )
    knows = pd.DataFrame(
        {ID_COLUMN: [10, 11], "__SOURCE__": [1, 3], "__TARGET__": [2, 4]},
    )
    person = EntityTable.from_dataframe("Person", persons)
    knows_t = RelationshipTable(
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
        relationship_mapping=RelationshipMapping(mapping={"KNOWS": knows_t}),
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
    def test_inline_prop_eligible(self) -> None:
        assert is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person {dept: 'eng'}) RETURN n.name AS name"),
            _ctx("duckdb"),
        )

    def test_unknown_inline_prop_ineligible(self) -> None:
        assert not is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person {nope: 'x'}) RETURN n.name AS name"),
            _ctx("duckdb"),
        )


class TestParity:
    def test_single_inline_prop(self) -> None:
        _assert_parity("MATCH (n:Person {dept: 'eng'}) RETURN n.name AS name", ["name"])

    def test_multiple_inline_props(self) -> None:
        _assert_parity(
            "MATCH (n:Person {dept: 'eng', active: true}) RETURN n.name AS name",
            ["name"],
        )

    def test_inline_prop_plus_where(self) -> None:
        _assert_parity(
            "MATCH (n:Person {dept: 'eng'}) WHERE n.name <> 'Bob' RETURN n.name AS name",
            ["name"],
        )

    def test_inline_prop_in_multihop(self) -> None:
        _assert_parity(
            "MATCH (a:Person {dept: 'eng'})-[:KNOWS]->(b:Person) RETURN a.name AS an, b.name AS bn",
            ["an", "bn"],
        )

    def test_inline_prop_with_aggregation(self) -> None:
        _assert_parity(
            "MATCH (n:Person {active: true}) RETURN n.dept AS dept, count(*) AS c",
            ["dept"],
        )
