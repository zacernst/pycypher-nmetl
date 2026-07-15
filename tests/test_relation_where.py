"""Phase 6 (out-of-core DuckDB) — WHERE as a SQL predicate.

Verifies the expression compiler and WHERE support: eligible WHERE queries run
through the relation engine and match the pandas oracle (including null
semantics), while unsupported predicates (functions) fall back.

See docs/duckdb_out_of_core_design.md, Phase 6.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_converter import ASTConverter
from pycypher.relation_engine import is_relation_eligible
from pycypher.relation_sql import compile_expression
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star

ID_COLUMN = "__ID__"


def _ctx(backend: str) -> Context:
    people = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "age": [30, 25, 35, None],  # Dave has NULL age
            "dept": ["eng", "eng", "mktg", "eng"],
        },
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": EntityTable.from_dataframe("Person", people)}),
        relationship_mapping=RelationshipMapping(mapping={}),
        backend=backend,
    )


def _both(query: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    oracle = Star(context=_ctx("pandas")).execute_query(query)
    ctx = _ctx("duckdb")
    ctx._relation_engine_enabled = True
    got = Star(context=ctx).execute_query(query)
    return oracle, got


def _assert_parity(query: str, sort_col: str = "name") -> None:
    oracle, got = _both(query)
    o = oracle.sort_values(sort_col).reset_index(drop=True)
    g = got.sort_values(sort_col).reset_index(drop=True)
    pd.testing.assert_frame_equal(o, g, check_dtype=False)


class TestCompiler:
    def test_supported_predicate_compiles(self) -> None:
        attr = {"age": "age", "name": "name"}
        where = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE n.age >= 30 AND n.name <> 'Bob' RETURN n.age AS a",
        ).clauses[0].where
        assert compile_expression(where, "n", attr) is not None

    def test_unsupported_function_returns_none(self) -> None:
        attr = {"name": "name"}
        where = ASTConverter.from_cypher(
            "MATCH (n:Person) WHERE upper(n.name) = 'ALICE' RETURN n.name AS x",
        ).clauses[0].where
        assert compile_expression(where, "n", attr) is None


class TestEligibility:
    def test_where_eligible(self) -> None:
        ctx = _ctx("duckdb")
        assert is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) WHERE n.age > 28 RETURN n.name AS name"),
            ctx,
        )

    def test_where_with_function_ineligible(self) -> None:
        ctx = _ctx("duckdb")
        assert not is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) WHERE upper(n.name) = 'A' RETURN n.name AS name"),
            ctx,
        )


class TestParity:
    @pytest.mark.parametrize(
        "query",
        [
            "MATCH (n:Person) WHERE n.age > 28 RETURN n.name AS name",
            "MATCH (n:Person) WHERE n.age >= 30 AND n.dept = 'eng' RETURN n.name AS name",
            "MATCH (n:Person) WHERE n.age < 30 OR n.name = 'Carol' RETURN n.name AS name",
            "MATCH (n:Person) WHERE NOT n.dept = 'eng' RETURN n.name AS name",
            "MATCH (n:Person) WHERE n.age IS NULL RETURN n.name AS name",
            "MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n.name AS name",
        ],
    )
    def test_where_parity(self, query: str) -> None:
        _assert_parity(query)

    def test_null_comparison_semantics(self) -> None:
        # Dave has NULL age; `age > 0` must exclude him in BOTH engines
        # (SQL/Cypher three-valued logic: NULL > 0 is unknown → excluded).
        oracle, got = _both("MATCH (n:Person) WHERE n.age > 0 RETURN n.name AS name")
        assert "Dave" not in oracle["name"].tolist()
        assert sorted(got["name"].tolist()) == sorted(oracle["name"].tolist())
