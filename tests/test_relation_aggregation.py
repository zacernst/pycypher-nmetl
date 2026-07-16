"""Phase 9 (out-of-core DuckDB) — aggregation via GROUP BY.

Verifies count(*)/count/sum/avg/min/max aggregations (full-table and grouped,
with DISTINCT) run through the relation engine and match the pandas oracle.

See docs/duckdb_out_of_core_design.md, Phase 9.
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
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "dept": ["eng", "eng", "mktg", "eng", "mktg"],
            "age": [30, 25, 35, 28, 22],
        },
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": EntityTable.from_dataframe("Person", people)}),
        relationship_mapping=RelationshipMapping(mapping={}),
        backend=backend,
    )


def _assert_parity(query: str, sort_cols: list[str] | None) -> None:
    oracle = Star(context=_ctx("pandas")).execute_query(query)
    ctx = _ctx("duckdb")
    ctx._relation_engine_enabled = True
    got = Star(context=ctx).execute_query(query)
    assert set(oracle.columns) == set(got.columns)
    if sort_cols:
        oracle = oracle.sort_values(sort_cols).reset_index(drop=True)
        got = got[oracle.columns].sort_values(sort_cols).reset_index(drop=True)
    else:
        got = got[oracle.columns]
    pd.testing.assert_frame_equal(
        oracle.reset_index(drop=True), got.reset_index(drop=True), check_dtype=False,
    )


class TestEligibility:
    def test_full_table_count_eligible(self) -> None:
        assert is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) RETURN count(*) AS c"),
            _ctx("duckdb"),
        )

    def test_grouped_agg_eligible(self) -> None:
        assert is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) RETURN n.dept AS d, count(*) AS c"),
            _ctx("duckdb"),
        )

    def test_unaliased_aggregate_ineligible(self) -> None:
        assert not is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) RETURN count(*)"),
            _ctx("duckdb"),
        )

    def test_unsupported_agg_ineligible(self) -> None:
        # collect() not supported yet.
        assert not is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) RETURN collect(n.name) AS names"),
            _ctx("duckdb"),
        )


class TestParity:
    @pytest.mark.parametrize(
        ("query", "sort_cols"),
        [
            ("MATCH (n:Person) RETURN count(*) AS c", None),
            ("MATCH (n:Person) RETURN count(n) AS c", None),
            ("MATCH (n:Person) RETURN sum(n.age) AS total, avg(n.age) AS avg_age", None),
            ("MATCH (n:Person) RETURN min(n.age) AS lo, max(n.age) AS hi", None),
            ("MATCH (n:Person) RETURN n.dept AS d, count(*) AS c", ["d"]),
            ("MATCH (n:Person) RETURN n.dept AS d, avg(n.age) AS avg_age", ["d"]),
            ("MATCH (n:Person) RETURN n.dept AS d, count(n.age) AS n_age, sum(n.age) AS total", ["d"]),
            ("MATCH (n:Person) WHERE n.age > 24 RETURN n.dept AS d, count(*) AS c", ["d"]),
            ("MATCH (n:Person) RETURN count(DISTINCT n.dept) AS depts", None),
            ("MATCH (n:Person) RETURN n.dept, count(*) AS c", ["dept"]),  # bare group key
        ],
    )
    def test_aggregation_parity(self, query: str, sort_cols: list[str] | None) -> None:
        _assert_parity(query, sort_cols)
