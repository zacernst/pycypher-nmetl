"""Phase 7 (out-of-core DuckDB) — RETURN expressions via the compiler.

Verifies that RETURN items beyond bare property lookups (arithmetic, literals)
are eligible when explicitly aliased and match the pandas oracle; non-property
expressions without an alias remain ineligible.

See docs/duckdb_out_of_core_design.md, Phase 7.
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
)
from pycypher.star import Star

ID_COLUMN = "__ID__"


def _ctx(backend: str) -> Context:
    people = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": EntityTable.from_dataframe("Person", people)}),
        relationship_mapping=RelationshipMapping(mapping={}),
        backend=backend,
    )


def _assert_parity(query: str, sort_col: str) -> None:
    oracle = Star(context=_ctx("pandas")).execute_query(query)
    ctx = _ctx("duckdb")
    ctx._relation_engine_enabled = True
    got = Star(context=ctx).execute_query(query)
    pd.testing.assert_frame_equal(
        oracle.sort_values(sort_col).reset_index(drop=True),
        got.sort_values(sort_col).reset_index(drop=True),
        check_dtype=False,
    )


class TestEligibility:
    def test_aliased_arithmetic_eligible(self) -> None:
        assert is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) RETURN n.age + 1 AS next_age"),
            _ctx("duckdb"),
        )

    def test_unaliased_arithmetic_ineligible(self) -> None:
        # Non-property expression without an alias → ambiguous name → ineligible.
        assert not is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) RETURN n.age + 1"),
            _ctx("duckdb"),
        )

    def test_unsupported_function_ineligible(self) -> None:
        assert not is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) RETURN upper(n.name) AS u"),
            _ctx("duckdb"),
        )


class TestParity:
    @pytest.mark.parametrize(
        ("query", "sort_col"),
        [
            ("MATCH (n:Person) RETURN n.name AS name, n.age + 1 AS next_age", "name"),
            ("MATCH (n:Person) RETURN n.name AS name, n.age * 2 AS double_age", "name"),
            ("MATCH (n:Person) RETURN n.name AS name, n.age - 5 AS x", "name"),
            ("MATCH (n:Person) WHERE n.age > 26 RETURN n.name AS name, n.age + 100 AS y", "name"),
        ],
    )
    def test_return_expr_parity(self, query: str, sort_col: str) -> None:
        _assert_parity(query, sort_col)

    def test_arithmetic_values(self) -> None:
        ctx = _ctx("duckdb")
        ctx._relation_engine_enabled = True
        got = Star(context=ctx).execute_query(
            "MATCH (n:Person) RETURN n.name AS name, n.age + 1 AS next_age",
        ).sort_values("name").reset_index(drop=True)
        assert got["next_age"].tolist() == [31, 26, 36]
