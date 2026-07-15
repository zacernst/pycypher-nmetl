"""Phase 10 (out-of-core DuckDB) — ORDER BY / SKIP / LIMIT / DISTINCT.

Verifies these RETURN modifiers run through the relation engine and match the
pandas oracle (including null ordering).  WITH chaining and SKIP-without-LIMIT
remain ineligible (fall back).

See docs/duckdb_out_of_core_design.md, Phase 10.
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
            "age": [30, 25, 35, None, 25],  # Dave NULL; Bob/Eve tie at 25
        },
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": EntityTable.from_dataframe("Person", people)}),
        relationship_mapping=RelationshipMapping(mapping={}),
        backend=backend,
    )


def _records(df: pd.DataFrame) -> list[dict]:
    # Normalise null representation (pandas None vs DuckDB NaN) so comparison
    # focuses on values/order, not the in-memory null flavour.
    return [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in df.to_dict("records")
    ]


def _assert_parity(query: str, *, ordered: bool) -> None:
    oracle = Star(context=_ctx("pandas")).execute_query(query)
    ctx = _ctx("duckdb")
    ctx._relation_engine_enabled = True
    got = Star(context=ctx).execute_query(query)
    assert set(oracle.columns) == set(got.columns)
    got = got[oracle.columns]
    if ordered:
        # Order is meaningful — compare row sequence as-is.
        assert _records(oracle) == _records(got)
    else:
        sort_cols = list(oracle.columns)
        oracle = oracle.sort_values(sort_cols).reset_index(drop=True)
        got = got.sort_values(sort_cols).reset_index(drop=True)
        assert _records(oracle) == _records(got)


class TestEligibility:
    @pytest.mark.parametrize(
        "query",
        [
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
            "MATCH (n:Person) RETURN DISTINCT n.dept AS dept",
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name LIMIT 2",
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name SKIP 1 LIMIT 2",
            "MATCH (n:Person) RETURN n.dept AS dept, count(*) AS c ORDER BY c DESC",
        ],
    )
    def test_eligible(self, query: str) -> None:
        assert is_relation_eligible(ASTConverter.from_cypher(query), _ctx("duckdb"))

    def test_skip_without_limit_ineligible(self) -> None:
        assert not is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) RETURN n.name AS name ORDER BY name SKIP 2"),
            _ctx("duckdb"),
        )

    def test_order_by_non_output_ineligible(self) -> None:
        # Ordering by a property that isn't returned → not an output column → fall back.
        assert not is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) RETURN n.name AS name ORDER BY n.age"),
            _ctx("duckdb"),
        )


class TestParity:
    def test_distinct(self) -> None:
        _assert_parity("MATCH (n:Person) RETURN DISTINCT n.dept AS dept", ordered=False)

    def test_order_asc_with_null(self) -> None:
        _assert_parity(
            "MATCH (n:Person) RETURN n.name AS name, n.age AS age ORDER BY age ASC, name ASC",
            ordered=True,
        )

    def test_order_desc(self) -> None:
        _assert_parity(
            "MATCH (n:Person) RETURN n.name AS name, n.age AS age ORDER BY age DESC, name ASC",
            ordered=True,
        )

    def test_order_limit(self) -> None:
        _assert_parity(
            "MATCH (n:Person) RETURN n.name AS name, n.age AS age ORDER BY name ASC LIMIT 3",
            ordered=True,
        )

    def test_order_skip_limit(self) -> None:
        _assert_parity(
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name ASC SKIP 1 LIMIT 2",
            ordered=True,
        )

    def test_grouped_agg_order_by_aggregate(self) -> None:
        _assert_parity(
            "MATCH (n:Person) RETURN n.dept AS dept, count(*) AS c ORDER BY c DESC, dept ASC",
            ordered=True,
        )
