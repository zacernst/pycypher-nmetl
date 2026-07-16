"""UNWIND (out-of-core) — list expansion via DuckDB UNNEST.

Verifies a leading UNWIND of a list and UNWIND of a scalar list column (post
WITH) expand to rows and match the pandas oracle.  UNWIND right after MATCH
(pattern scope) is deferred (ineligible → fallback).
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
    people = pd.DataFrame({ID_COLUMN: [1, 2], "name": ["Alice", "Bob"]})
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": EntityTable.from_dataframe("Person", people)}),
        relationship_mapping=RelationshipMapping(mapping={}),
        backend=backend,
    )


def _assert_parity(query: str, sort_cols: list[str]) -> None:
    oracle = Star(context=_ctx("pandas")).execute_query(query)
    ctx = _ctx("duckdb")
    ctx._relation_engine_enabled = True
    got = Star(context=ctx).execute_query(query)
    assert set(oracle.columns) == set(got.columns), (list(oracle.columns), list(got.columns))
    o = oracle.sort_values(sort_cols).reset_index(drop=True)
    g = got[oracle.columns].sort_values(sort_cols).reset_index(drop=True)
    pd.testing.assert_frame_equal(o, g, check_dtype=False)


class TestEligibility:
    @pytest.mark.parametrize(
        "query",
        [
            "UNWIND [1, 2, 3] AS x RETURN x AS n",
            "UNWIND [1, 2, 3] AS x RETURN x * 10 AS big",
            "WITH [10, 20, 30] AS lst UNWIND lst AS x RETURN x AS v",
        ],
    )
    def test_eligible(self, query: str) -> None:
        assert is_relation_eligible(ASTConverter.from_cypher(query), _ctx("duckdb"))

    def test_unwind_after_match_ineligible(self) -> None:
        # UNWIND in pattern scope (right after MATCH) is deferred.
        assert not is_relation_eligible(
            ASTConverter.from_cypher(
                "MATCH (p:Person) UNWIND [1, 2] AS x RETURN p.name AS name, x AS n",
            ),
            _ctx("duckdb"),
        )


class TestParity:
    def test_leading_unwind_ints(self) -> None:
        _assert_parity("UNWIND [1, 2, 3] AS x RETURN x AS n", ["n"])

    def test_leading_unwind_expression(self) -> None:
        _assert_parity("UNWIND [1, 2, 3] AS x RETURN x * 10 AS big", ["big"])

    def test_leading_unwind_strings(self) -> None:
        _assert_parity("UNWIND ['a', 'b', 'c'] AS s RETURN s AS letter", ["letter"])

    def test_leading_unwind_then_filter(self) -> None:
        _assert_parity(
            "UNWIND [1, 2, 3, 4] AS x WITH x AS y WHERE y > 2 RETURN y AS z",
            ["z"],
        )

    def test_unwind_after_with(self) -> None:
        _assert_parity(
            "WITH [10, 20, 30] AS lst UNWIND lst AS x RETURN x AS v",
            ["v"],
        )

    def test_leading_unwind_order_limit(self) -> None:
        _assert_parity(
            "UNWIND [3, 1, 2, 5, 4] AS x RETURN x AS n ORDER BY n DESC LIMIT 2",
            ["n"],
        )
