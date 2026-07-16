"""Phase 10b (out-of-core DuckDB) — user Python functions as DuckDB UDFs.

Verifies register_relation_udf makes a scalar Python function callable from
eligible out-of-core queries (in RETURN and WHERE), producing correct results;
unregistered function names remain ineligible (fall back).

See docs/duckdb_out_of_core_design.md, Phase 10b.
"""

from __future__ import annotations

import pandas as pd
from pycypher.ast_converter import ASTConverter
from pycypher.relation_engine import (
    is_relation_eligible,
    register_relation_udf,
)
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star

ID_COLUMN = "__ID__"


def _ctx() -> Context:
    people = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["alice", "bob", "carol"],
            "age": [30, 25, 35],
        },
    )
    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": EntityTable.from_dataframe("Person", people)}),
        relationship_mapping=RelationshipMapping(mapping={}),
        backend="duckdb",
    )
    ctx._relation_engine_enabled = True
    return ctx


def _shout(s: str) -> str:
    return s.upper() + "!"


def _plus_ten(n: int) -> int:
    return n + 10


class TestUnregistered:
    def test_unregistered_function_ineligible(self) -> None:
        # No UDF registered → function call → ineligible.
        assert not is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) RETURN shout(n.name) AS s"),
            _ctx(),
        )


class TestRegisteredUDF:
    def test_udf_in_return(self) -> None:
        ctx = _ctx()
        register_relation_udf(ctx, "shout", _shout, param_types=["VARCHAR"], return_type="VARCHAR")
        assert is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) RETURN shout(n.name) AS s"),
            ctx,
        )
        got = Star(context=ctx).execute_query(
            "MATCH (n:Person) RETURN n.name AS name, shout(n.name) AS s",
        ).sort_values("name").reset_index(drop=True)
        assert got["s"].tolist() == ["ALICE!", "BOB!", "CAROL!"]

    def test_udf_in_where(self) -> None:
        ctx = _ctx()
        register_relation_udf(ctx, "plus_ten", _plus_ten, param_types=["BIGINT"], return_type="BIGINT")
        got = Star(context=ctx).execute_query(
            "MATCH (n:Person) WHERE plus_ten(n.age) > 39 RETURN n.name AS name",
        )
        # plus_ten(age) > 39  → age > 29 → alice(30), carol(35)
        assert sorted(got["name"].tolist()) == ["alice", "carol"]

    def test_udf_numeric_return(self) -> None:
        ctx = _ctx()
        register_relation_udf(ctx, "plus_ten", _plus_ten, param_types=["BIGINT"], return_type="BIGINT")
        got = Star(context=ctx).execute_query(
            "MATCH (n:Person) RETURN n.name AS name, plus_ten(n.age) AS a",
        ).sort_values("name").reset_index(drop=True)
        assert got["a"].tolist() == [40, 35, 45]
