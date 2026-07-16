"""UDFs composed with WITH chaining and aggregation (out-of-core).

Locks in that registered scalar UDFs work in WITH items, WITH WHERE, over
post-WITH scalar columns, and — after threading `functions` through
compile_aggregate — inside aggregate arguments.  Also checks count(scalar)
counts non-null values (not COUNT(*)).
"""

from __future__ import annotations

import pandas as pd
import pytest
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
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["alice", "bob", "carol", "dave"],
            "age": [30, 25, 35, None],  # Dave: NULL age
        },
    )
    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": EntityTable.from_dataframe("Person", people)}),
        relationship_mapping=RelationshipMapping(mapping={}),
        backend="duckdb",
    )
    ctx._relation_engine_enabled = True
    register_relation_udf(ctx, "shout", lambda s: s.upper() + "!", param_types=["VARCHAR"], return_type="VARCHAR")
    # age is nullable → DuckDB types the column DOUBLE, so the UDF takes DOUBLE.
    register_relation_udf(ctx, "plus_ten", lambda n: n + 10, param_types=["DOUBLE"], return_type="DOUBLE")
    return ctx


def _run(query: str) -> pd.DataFrame:
    return Star(context=_ctx()).execute_query(query)


class TestEligibility:
    @pytest.mark.parametrize(
        "query",
        [
            "MATCH (n:Person) WITH shout(n.name) AS s RETURN s AS out",
            "MATCH (n:Person) WITH n WHERE plus_ten(n.age) > 39 RETURN n.name AS name",
            "MATCH (n:Person) WITH n.name AS nm WITH shout(nm) AS s RETURN s AS out",
            "MATCH (n:Person) WITH sum(plus_ten(n.age)) AS s RETURN s AS total",
        ],
    )
    def test_eligible(self, query: str) -> None:
        assert is_relation_eligible(ASTConverter.from_cypher(query), _ctx())

    def test_unregistered_udf_ineligible(self) -> None:
        assert not is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) WITH mystery(n.name) AS s RETURN s AS out"),
            _ctx(),
        )


class TestValues:
    def test_udf_in_with_item(self) -> None:
        out = _run("MATCH (n:Person) WITH shout(n.name) AS s RETURN s AS out")
        assert sorted(out["out"].tolist()) == ["ALICE!", "BOB!", "CAROL!", "DAVE!"]

    def test_udf_in_with_where(self) -> None:
        out = _run("MATCH (n:Person) WITH n WHERE plus_ten(n.age) > 39 RETURN n.name AS name")
        assert sorted(out["name"].tolist()) == ["alice", "carol"]

    def test_udf_over_post_with_scalar(self) -> None:
        out = _run("MATCH (n:Person) WITH n.name AS nm WITH shout(nm) AS s RETURN s AS out")
        assert sorted(out["out"].tolist()) == ["ALICE!", "BOB!", "CAROL!", "DAVE!"]

    def test_udf_inside_aggregate_sum(self) -> None:
        # plus_ten(age) over 30,25,35,NULL → 40,35,45,NULL → SUM ignores NULL = 120.
        out = _run("MATCH (n:Person) WITH sum(plus_ten(n.age)) AS s RETURN s AS total")
        assert out["total"].iloc[0] == 120

    def test_udf_inside_aggregate_count(self) -> None:
        # count(plus_ten(age)) counts non-null → 3 (Dave's NULL excluded).
        out = _run("MATCH (n:Person) WITH count(plus_ten(n.age)) AS c RETURN c AS cnt")
        assert out["cnt"].iloc[0] == 3


class TestCountScalar:
    def test_count_scalar_counts_non_null(self) -> None:
        # count(age) over a post-WITH scalar must be COUNT("age") (non-null=3),
        # not COUNT(*) (which would be 4).
        out = _run("MATCH (n:Person) WITH n.age AS age WITH count(age) AS c RETURN c AS cnt")
        assert out["cnt"].iloc[0] == 3
