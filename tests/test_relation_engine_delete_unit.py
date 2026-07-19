"""Phase 2 slice 2 (out-of-core DuckDB parity) — DELETE as native DELETE FROM.

Verifies the narrow single-table DELETE-eligibility classifier and the
DELETE-statement compilation against a registered streaming source, plus
that every ineligible shape falls back to the unchanged pandas DELETE path.

See docs/duckdb_full_parity_design.md, Phase 2.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_converter import ASTConverter
from pycypher.ingestion.data_sources import data_source_from_uri
from pycypher.relation_engine import (
    execute_relation_delete,
    is_relation_delete_eligible,
    register_streaming_source,
)
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star

ID_COLUMN = "__ID__"


def _ast(query: str):
    return ASTConverter.from_cypher(query)


@pytest.fixture
def people_parquet(tmp_path):
    path = tmp_path / "people.parquet"
    pd.DataFrame(
        {"name": ["Alice", "Bob", "Carol"], "age": [30, 25, 35]},
    ).to_parquet(path)
    return path


def _streaming_ctx() -> Context:
    ctx = Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
        backend="duckdb",
    )
    ctx._relation_engine_enabled = True
    return ctx


def _streaming_people_ctx(people_parquet) -> Context:
    ctx = _streaming_ctx()
    register_streaming_source(ctx, "Person", data_source_from_uri(str(people_parquet)))
    return ctx


def _entity_ctx(backend: str = "duckdb") -> Context:
    people = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3], "name": ["Alice", "Bob", "Carol"], "age": [30, 25, 35]},
    )
    et = EntityTable.from_dataframe("Person", people)
    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": et}),
        relationship_mapping=RelationshipMapping(mapping={}),
        backend=backend,
    )
    ctx._relation_engine_enabled = True
    return ctx


class TestEligibility:
    def test_eligible_simple_delete(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert is_relation_delete_eligible(
            _ast("MATCH (p:Person) WHERE p.age > 28 DELETE p"),
            ctx,
        )

    def test_eligible_no_where(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert is_relation_delete_eligible(
            _ast("MATCH (p:Person) DELETE p"),
            ctx,
        )

    def test_ineligible_detach_delete(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert not is_relation_delete_eligible(
            _ast("MATCH (p:Person) DETACH DELETE p"),
            ctx,
        )

    def test_ineligible_optional_match(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert not is_relation_delete_eligible(
            _ast("OPTIONAL MATCH (p:Person) DELETE p"),
            ctx,
        )

    def test_ineligible_two_hop_pattern(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert not is_relation_delete_eligible(
            _ast("MATCH (p:Person)-[k:KNOWS]->(q:Person) DELETE p"),
            ctx,
        )

    def test_ineligible_delete_unbound_variable(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert not is_relation_delete_eligible(
            _ast("MATCH (p:Person) DELETE q"),
            ctx,
        )

    def test_ineligible_no_streaming_source(self) -> None:
        ctx = _entity_ctx()
        assert not is_relation_delete_eligible(
            _ast("MATCH (p:Person) DELETE p"),
            ctx,
        )

    def test_ineligible_pandas_backend(self) -> None:
        ctx = _entity_ctx(backend="pandas")
        assert not is_relation_delete_eligible(
            _ast("MATCH (p:Person) DELETE p"),
            ctx,
        )


class TestExecution:
    def test_execute_relation_delete_removes_matching_rows_only(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        query = _ast("MATCH (p:Person) WHERE p.age > 28 DELETE p")
        assert is_relation_delete_eligible(query, ctx)
        execute_relation_delete(query, ctx)

        got = (
            ctx.backend.connection.execute(
                'SELECT name FROM "_streaming_source_Person" ORDER BY name',
            )
            .fetchdf()
        )
        assert list(got["name"]) == ["Bob"]

    def test_execute_query_dispatches_through_pipeline(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        out = Star(context=ctx).execute_query(
            "MATCH (p:Person) WHERE p.age > 28 DELETE p",
        )
        assert out.empty

        got = (
            ctx.backend.connection.execute(
                'SELECT name FROM "_streaming_source_Person" ORDER BY name',
            )
            .fetchdf()
        )
        assert list(got["name"]) == ["Bob"]


class TestFallback:
    def test_ineligible_delete_still_applies_via_pandas(self) -> None:
        ctx = _entity_ctx()
        out = Star(context=ctx).execute_query(
            "MATCH (p:Person) WHERE p.age > 28 DELETE p",
        )
        assert out.empty

        all_out = Star(context=ctx).execute_query(
            "MATCH (p:Person) RETURN p.name AS name",
        )
        assert sorted(all_out["name"]) == ["Bob"]
