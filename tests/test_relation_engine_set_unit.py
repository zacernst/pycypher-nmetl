"""Phase 2 slice 1 (out-of-core DuckDB parity) — SET as native UPDATE.

Verifies the narrow single-table SET-eligibility classifier and the
UPDATE-statement compilation against a registered streaming source, plus
that every ineligible shape falls back to the unchanged pandas SET path.

See docs/duckdb_full_parity_design.md, Phase 2.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_converter import ASTConverter
from pycypher.ingestion.data_sources import data_source_from_uri
from pycypher.relation_engine import (
    execute_relation_set,
    is_relation_set_eligible,
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
        {"name": ["Alice", "Bob", "Carol"], "age": [30, 25, 35], "status": ["", "", ""]},
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
    def test_eligible_simple_set(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert is_relation_set_eligible(
            _ast("MATCH (p:Person) WHERE p.age > 28 SET p.status = 'senior'"),
            ctx,
        )

    def test_eligible_no_where(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert is_relation_set_eligible(
            _ast("MATCH (p:Person) SET p.status = 'everyone'"),
            ctx,
        )

    def test_ineligible_optional_match(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert not is_relation_set_eligible(
            _ast("OPTIONAL MATCH (p:Person) SET p.status = 'senior'"),
            ctx,
        )

    def test_ineligible_two_hop_pattern(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert not is_relation_set_eligible(
            _ast(
                "MATCH (p:Person)-[k:KNOWS]->(q:Person) SET p.status = 'senior'",
            ),
            ctx,
        )

    def test_ineligible_set_labels_item(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert not is_relation_set_eligible(
            _ast("MATCH (p:Person) SET p:Senior"),
            ctx,
        )

    def test_ineligible_set_targets_unbound_variable(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert not is_relation_set_eligible(
            _ast("MATCH (p:Person) SET q.status = 'senior'"),
            ctx,
        )

    def test_ineligible_no_streaming_source(self) -> None:
        ctx = _entity_ctx()
        assert not is_relation_set_eligible(
            _ast("MATCH (p:Person) SET p.status = 'senior'"),
            ctx,
        )

    def test_ineligible_pandas_backend(self) -> None:
        ctx = _entity_ctx(backend="pandas")
        assert not is_relation_set_eligible(
            _ast("MATCH (p:Person) SET p.status = 'senior'"),
            ctx,
        )


class TestExecution:
    def test_execute_relation_set_updates_matching_rows_only(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        query = _ast("MATCH (p:Person) WHERE p.age > 28 SET p.status = 'senior'")
        assert is_relation_set_eligible(query, ctx)
        execute_relation_set(query, ctx)

        got = (
            ctx.backend.connection.execute(
                'SELECT name, status FROM "_streaming_source_Person" ORDER BY name',
            )
            .fetchdf()
        )
        expected = {
            "Alice": "senior",
            "Bob": "",
            "Carol": "senior",
        }
        assert dict(zip(got["name"], got["status"])) == expected

    def test_execute_query_dispatches_through_pipeline(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        out = Star(context=ctx).execute_query(
            "MATCH (p:Person) WHERE p.age > 28 SET p.status = 'senior'",
        )
        assert out.empty

        got = (
            ctx.backend.connection.execute(
                'SELECT name, status FROM "_streaming_source_Person" ORDER BY name',
            )
            .fetchdf()
        )
        expected = {
            "Alice": "senior",
            "Bob": "",
            "Carol": "senior",
        }
        assert dict(zip(got["name"], got["status"])) == expected


class TestFallback:
    def test_ineligible_set_still_applies_via_pandas(self) -> None:
        ctx = _entity_ctx()
        out = Star(context=ctx).execute_query(
            "MATCH (p:Person) WHERE p.age > 28 SET p.status = 'senior' RETURN p.name AS name, p.status AS status",
        )
        got = dict(zip(out["name"], out["status"]))
        assert got == {"Alice": "senior", "Carol": "senior"}

        all_out = Star(context=ctx).execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.status AS status",
        )
        all_got = dict(zip(all_out["name"], all_out["status"]))
        assert all_got["Bob"] != "senior"
