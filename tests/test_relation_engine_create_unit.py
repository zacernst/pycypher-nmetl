"""Phase 2 slice 3 (out-of-core DuckDB parity) — CREATE as native INSERT.

Verifies the narrow standalone single-node CREATE-eligibility classifier and
the INSERT-statement compilation (including DuckDB SEQUENCE-based ID
generation) against a registered streaming source, plus that every
ineligible shape falls back to the unchanged pandas CREATE path.

See docs/duckdb_full_parity_design.md, Phase 2.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_converter import ASTConverter
from pycypher.ingestion.data_sources import data_source_from_uri
from pycypher.relation_engine import (
    execute_relation_create,
    is_relation_create_eligible,
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
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"], "age": [30, 25, 35]},
    ).to_parquet(path)
    return path


@pytest.fixture
def people_no_id_parquet(tmp_path):
    path = tmp_path / "people_no_id.parquet"
    pd.DataFrame(
        {"name": ["Alice", "Bob", "Carol"], "age": [30, 25, 35]},
    ).to_parquet(path)
    return path


@pytest.fixture
def people_string_id_parquet(tmp_path):
    path = tmp_path / "people_string_id.parquet"
    pd.DataFrame({"id": ["a1", "a2"], "name": ["Alice", "Bob"]}).to_parquet(path)
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
    register_streaming_source(
        ctx, "Person", data_source_from_uri(str(people_parquet)), id_col="id",
    )
    return ctx


def _streaming_people_no_id_ctx(people_no_id_parquet) -> Context:
    ctx = _streaming_ctx()
    register_streaming_source(ctx, "Person", data_source_from_uri(str(people_no_id_parquet)))
    return ctx


def _streaming_people_string_id_ctx(people_string_id_parquet) -> Context:
    ctx = _streaming_ctx()
    register_streaming_source(
        ctx, "Person", data_source_from_uri(str(people_string_id_parquet)), id_col="id",
    )
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
    def test_eligible_simple_create(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert is_relation_create_eligible(
            _ast("CREATE (p:Person {name: 'Dave', age: 40})"),
            ctx,
        )

    def test_eligible_no_id_col(self, people_no_id_parquet) -> None:
        ctx = _streaming_people_no_id_ctx(people_no_id_parquet)
        assert is_relation_create_eligible(
            _ast("CREATE (p:Person {name: 'Dave', age: 40})"),
            ctx,
        )

    def test_ineligible_preceded_by_match(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert not is_relation_create_eligible(
            _ast("MATCH (q:Person) CREATE (p:Person {name: 'Dave', age: 40})"),
            ctx,
        )

    def test_ineligible_relationship_pattern(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert not is_relation_create_eligible(
            _ast(
                "CREATE (p:Person {name: 'Dave'})-[k:KNOWS]->(q:Person {name: 'Eve'})",
            ),
            ctx,
        )

    def test_ineligible_unknown_property_key(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        assert not is_relation_create_eligible(
            _ast("CREATE (p:Person {unknown: 'x'})"),
            ctx,
        )

    def test_ineligible_non_integer_id_col(self, people_string_id_parquet) -> None:
        ctx = _streaming_people_string_id_ctx(people_string_id_parquet)
        assert not is_relation_create_eligible(
            _ast("CREATE (p:Person {name: 'Dave'})"),
            ctx,
        )

    def test_ineligible_no_streaming_source(self) -> None:
        ctx = _entity_ctx()
        assert not is_relation_create_eligible(
            _ast("CREATE (p:Person {name: 'Dave', age: 40})"),
            ctx,
        )

    def test_ineligible_pandas_backend(self) -> None:
        ctx = _entity_ctx(backend="pandas")
        assert not is_relation_create_eligible(
            _ast("CREATE (p:Person {name: 'Dave', age: 40})"),
            ctx,
        )


class TestExecution:
    def test_execute_relation_create_inserts_row(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        query = _ast("CREATE (p:Person {name: 'Dave', age: 40})")
        assert is_relation_create_eligible(query, ctx)
        execute_relation_create(query, ctx)

        got = (
            ctx.backend.connection.execute(
                'SELECT id, name, age FROM "_streaming_source_Person" ORDER BY id',
            )
            .fetchdf()
        )
        assert list(got["name"]) == ["Alice", "Bob", "Carol", "Dave"]
        assert got["id"].tolist()[-1] == 4

    def test_sequence_auto_increments_across_multiple_creates(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        for name in ("Dave", "Eve"):
            query = _ast(f"CREATE (p:Person {{name: '{name}', age: 1}})")
            execute_relation_create(query, ctx)

        got = (
            ctx.backend.connection.execute(
                'SELECT id, name FROM "_streaming_source_Person" ORDER BY id',
            )
            .fetchdf()
        )
        assert got["id"].tolist()[-2:] == [4, 5]
        assert got["name"].tolist()[-2:] == ["Dave", "Eve"]

    def test_execute_relation_create_without_id_col(self, people_no_id_parquet) -> None:
        ctx = _streaming_people_no_id_ctx(people_no_id_parquet)
        query = _ast("CREATE (p:Person {name: 'Dave', age: 40})")
        assert is_relation_create_eligible(query, ctx)
        execute_relation_create(query, ctx)

        got = (
            ctx.backend.connection.execute(
                'SELECT name, age FROM "_streaming_source_Person" ORDER BY name',
            )
            .fetchdf()
        )
        assert list(got["name"]) == ["Alice", "Bob", "Carol", "Dave"]

    def test_execute_query_dispatches_through_pipeline(self, people_parquet) -> None:
        ctx = _streaming_people_ctx(people_parquet)
        out = Star(context=ctx).execute_query(
            "CREATE (p:Person {name: 'Dave', age: 40})",
        )
        assert out.empty

        got = (
            ctx.backend.connection.execute(
                'SELECT name FROM "_streaming_source_Person" ORDER BY id',
            )
            .fetchdf()
        )
        assert list(got["name"]) == ["Alice", "Bob", "Carol", "Dave"]


class TestFallback:
    def test_ineligible_create_still_applies_via_pandas(self) -> None:
        ctx = _entity_ctx()
        out = Star(context=ctx).execute_query(
            "CREATE (p:Person {name: 'Dave', age: 40})",
        )
        assert out.empty

        all_out = Star(context=ctx).execute_query(
            "MATCH (p:Person) RETURN p.name AS name",
        )
        assert sorted(all_out["name"]) == ["Alice", "Bob", "Carol", "Dave"]
