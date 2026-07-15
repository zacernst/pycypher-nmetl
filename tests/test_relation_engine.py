"""Phase 4 (out-of-core DuckDB) — relation engine scaffold + fallback dispatch.

Verifies the opt-in relation engine: eligibility classification, result parity
with the pandas BindingFrame engine for the eligible subset, fallback for
ineligible queries, and that it is inert when disabled (the default).

See docs/duckdb_out_of_core_design.md, Phase 4.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_converter import ASTConverter
from pycypher.relation_engine import (
    RelationBindings,
    is_relation_eligible,
    relation_engine_enabled,
)
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star

ID_COLUMN = "__ID__"


def _ctx(backend: str = "duckdb") -> Context:
    people = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    et = EntityTable.from_dataframe("Person", people)
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": et}),
        relationship_mapping=RelationshipMapping(mapping={}),
        backend=backend,
    )


def _ast(query: str):
    return ASTConverter.from_cypher(query)


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------


class TestEligibility:
    def test_eligible_simple_aliased_projection(self) -> None:
        ctx = _ctx()
        assert is_relation_eligible(
            _ast("MATCH (n:Person) RETURN n.name AS name, n.age AS age"),
            ctx,
        )

    def test_eligible_implicit_alias(self) -> None:
        # A bare property lookup is eligible; its column is named after the
        # property (matching the pandas engine), so no explicit alias needed.
        assert is_relation_eligible(
            _ast("MATCH (n:Person) RETURN n.name"),
            _ctx(),
        )

    def test_ineligible_on_pandas_backend(self) -> None:
        ctx = _ctx(backend="pandas")
        assert not is_relation_eligible(
            _ast("MATCH (n:Person) RETURN n.name AS name"),
            ctx,
        )

    @pytest.mark.parametrize(
        "query",
        [
            "MATCH (n:Person) WHERE n.age > 20 RETURN n.name AS name",  # WHERE
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name",  # ORDER BY
            "MATCH (n:Person) RETURN DISTINCT n.name AS name",  # DISTINCT
            "MATCH (n:Person) RETURN count(n) AS c",  # aggregation
            "MATCH (n:Person) RETURN n.name AS name LIMIT 2",  # LIMIT
            "MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN n.name AS name",  # rel
            "MATCH (n:Person) RETURN n.missing_prop AS x",  # unknown property
            "MATCH (n:Unknown) RETURN n.name AS name",  # unknown label
        ],
    )
    def test_ineligible_shapes_fall_outside_subset(self, query: str) -> None:
        assert not is_relation_eligible(_ast(query), _ctx())


# ---------------------------------------------------------------------------
# Enable flag
# ---------------------------------------------------------------------------


class TestEnableFlag:
    def test_disabled_by_default(self) -> None:
        assert relation_engine_enabled(_ctx()) is False

    def test_context_attribute_enables(self) -> None:
        ctx = _ctx()
        ctx._relation_engine_enabled = True
        assert relation_engine_enabled(ctx) is True

    def test_env_var_enables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PYCYPHER_DUCKDB_RELATION_ENGINE", "1")
        assert relation_engine_enabled(_ctx()) is True


# ---------------------------------------------------------------------------
# Parity with the pandas engine (the oracle)
# ---------------------------------------------------------------------------


class TestParity:
    QUERY = "MATCH (n:Person) RETURN n.name AS name, n.age AS age"

    def test_relation_matches_pandas_engine(self) -> None:
        # Oracle: pandas backend, existing engine.
        oracle = Star(context=_ctx(backend="pandas")).execute_query(self.QUERY)

        # Relation engine enabled on a duckdb-backed context.
        ctx = _ctx(backend="duckdb")
        ctx._relation_engine_enabled = True
        got = Star(context=ctx).execute_query(self.QUERY)

        oracle_sorted = oracle.sort_values("name").reset_index(drop=True)
        got_sorted = got.sort_values("name").reset_index(drop=True)
        pd.testing.assert_frame_equal(oracle_sorted, got_sorted, check_dtype=False)

    def test_columns_are_aliases(self) -> None:
        ctx = _ctx(backend="duckdb")
        ctx._relation_engine_enabled = True
        got = Star(context=ctx).execute_query(self.QUERY)
        assert set(got.columns) == {"name", "age"}

    def test_implicit_alias_matches_pandas_engine(self) -> None:
        query = "MATCH (n:Person) RETURN n.name, n.age AS a"
        oracle = Star(context=_ctx(backend="pandas")).execute_query(query)

        ctx = _ctx(backend="duckdb")
        ctx._relation_engine_enabled = True
        got = Star(context=ctx).execute_query(query)

        assert list(oracle.columns) == list(got.columns)  # implicit col == "name"
        pd.testing.assert_frame_equal(
            oracle.sort_values("name").reset_index(drop=True),
            got.sort_values("name").reset_index(drop=True),
            check_dtype=False,
        )


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------


class TestFallback:
    def test_ineligible_query_still_correct_when_enabled(self) -> None:
        # WHERE makes it ineligible; must fall back to the pandas engine and
        # still return the right answer.
        ctx = _ctx(backend="duckdb")
        ctx._relation_engine_enabled = True
        got = Star(context=ctx).execute_query(
            "MATCH (n:Person) WHERE n.age > 28 RETURN n.name AS name",
        )
        assert sorted(got["name"].tolist()) == ["Alice", "Carol"]

    def test_disabled_uses_existing_engine(self) -> None:
        # Eligible shape, but engine disabled → existing engine, correct result.
        ctx = _ctx(backend="duckdb")
        got = Star(context=ctx).execute_query(
            "MATCH (n:Person) RETURN n.name AS name, n.age AS age",
        )
        assert sorted(got["name"].tolist()) == ["Alice", "Bob", "Carol"]


# ---------------------------------------------------------------------------
# RelationBindings
# ---------------------------------------------------------------------------


class TestRelationBindings:
    def test_to_pandas_and_columns(self) -> None:
        from pycypher.backends.duckdb_backend import (
            DuckDBLazyFrame,
            create_duckdb_connection,
        )

        con = create_duckdb_connection()
        try:
            con.register("t", pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}))
            rb = RelationBindings(DuckDBLazyFrame(con.sql('SELECT * FROM "t"'), con))
            assert set(rb.columns) == {"a", "b"}
            out = rb.to_pandas()
            assert len(out) == 2
        finally:
            con.close()
