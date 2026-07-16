"""Phase 10c (out-of-core DuckDB) — WITH chaining.

Verifies multi-part read queries (MATCH … WITH … [WHERE] … RETURN …) run through
the relation engine as a pipeline of relations and match the pandas oracle:
node pass-through filters, aggregate-then-HAVING, scalar projections, ORDER/LIMIT
across stages, and chained WITHs.  A MATCH after a WITH remains ineligible.

See docs/duckdb_out_of_core_design.md, Phase 10c.
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


def _records(df: pd.DataFrame) -> list[dict]:
    return [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in df.to_dict("records")
    ]


def _assert_parity(query: str, *, ordered: bool) -> None:
    oracle = Star(context=_ctx("pandas")).execute_query(query)
    ctx = _ctx("duckdb")
    ctx._relation_engine_enabled = True
    got = Star(context=ctx).execute_query(query)
    assert set(oracle.columns) == set(got.columns), (list(oracle.columns), list(got.columns))
    got = got[oracle.columns]
    if ordered:
        assert _records(oracle) == _records(got)
    else:
        sc = list(oracle.columns)
        assert _records(oracle.sort_values(sc).reset_index(drop=True)) == _records(
            got.sort_values(sc).reset_index(drop=True),
        )


class TestEligibility:
    @pytest.mark.parametrize(
        "query",
        [
            "MATCH (n:Person) WITH n WHERE n.age > 28 RETURN n.name AS name",
            "MATCH (n:Person) WITH n.dept AS dept, count(*) AS c WHERE c > 1 RETURN dept AS d, c AS cnt",
            "MATCH (n:Person) WITH n.age AS age WHERE age > 25 RETURN age AS a",
            "MATCH (n:Person) WITH n.dept AS d, n.age AS a WITH d AS dept, a AS age RETURN dept, age",
        ],
    )
    def test_with_eligible(self, query: str) -> None:
        assert is_relation_eligible(ASTConverter.from_cypher(query), _ctx("duckdb"))

    def test_match_after_with_ineligible(self) -> None:
        # A second MATCH after a WITH is not supported.
        assert not is_relation_eligible(
            ASTConverter.from_cypher(
                "MATCH (n:Person) WITH n.name AS name MATCH (m:Person) RETURN name AS x",
            ),
            _ctx("duckdb"),
        )


class TestParity:
    def test_passthrough_filter(self) -> None:
        _assert_parity(
            "MATCH (n:Person) WITH n WHERE n.age > 28 RETURN n.name AS name",
            ordered=False,
        )

    def test_aggregate_then_having(self) -> None:
        _assert_parity(
            "MATCH (n:Person) WITH n.dept AS dept, count(*) AS c WHERE c > 1 "
            "RETURN dept AS d, c AS cnt",
            ordered=False,
        )

    def test_scalar_projection_then_filter(self) -> None:
        _assert_parity(
            "MATCH (n:Person) WITH n.name AS name, n.age AS age WHERE age > 25 "
            "RETURN name AS nm, age AS a",
            ordered=False,
        )

    def test_with_order_limit_then_return(self) -> None:
        _assert_parity(
            "MATCH (n:Person) WITH n.name AS name, n.age AS age ORDER BY age DESC LIMIT 2 "
            "RETURN name AS nm, age AS a",
            ordered=True,
        )

    def test_chained_with(self) -> None:
        _assert_parity(
            "MATCH (n:Person) WITH n.dept AS d, n.age AS a WITH d AS dept, a AS age "
            "WHERE age > 25 RETURN dept, age",
            ordered=False,
        )

    def test_aggregate_having_order(self) -> None:
        _assert_parity(
            "MATCH (n:Person) WITH n.dept AS dept, count(*) AS c "
            "WHERE c >= 1 RETURN dept AS d, c AS cnt ORDER BY cnt DESC, d ASC",
            ordered=True,
        )
