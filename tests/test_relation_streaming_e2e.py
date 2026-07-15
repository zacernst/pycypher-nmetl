"""Phase 5 (out-of-core DuckDB) — end-to-end streaming for eligible queries.

Proves the full streaming spine: a file source read as a lazy relation
(read_relation) → an eligible relation query (projection) → streamed to a sink
via COPY, with no full pandas materialisation, including under a low
memory_limit.

See docs/duckdb_out_of_core_design.md, Phase 5.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pycypher.ingestion.data_sources import data_source_from_uri
from pycypher.relation_engine import (
    is_relation_eligible,
    register_streaming_source,
)
from pycypher.relational_models import (
    Context,
    EntityMapping,
    RelationshipMapping,
)
from pycypher.star import Star


def _streaming_ctx() -> Context:
    ctx = Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
        backend="duckdb",
    )
    ctx._relation_engine_enabled = True
    return ctx


@pytest.fixture
def people_parquet(tmp_path):
    path = tmp_path / "people.parquet"
    pd.DataFrame(
        {"name": ["Alice", "Bob", "Carol"], "age": [30, 25, 35]},
    ).to_parquet(path)
    return path


class TestStreamingSource:
    def test_eligible_against_streaming_source(self, people_parquet) -> None:
        ctx = _streaming_ctx()
        register_streaming_source(ctx, "Person", data_source_from_uri(str(people_parquet)))
        from pycypher.ast_converter import ASTConverter

        assert is_relation_eligible(
            ASTConverter.from_cypher("MATCH (n:Person) RETURN n.name AS name"),
            ctx,
        )

    def test_query_result_from_streaming_source(self, people_parquet) -> None:
        ctx = _streaming_ctx()
        register_streaming_source(ctx, "Person", data_source_from_uri(str(people_parquet)))
        out = Star(context=ctx).execute_query(
            "MATCH (n:Person) RETURN n.name AS name, n.age AS age",
        )
        assert sorted(out["name"].tolist()) == ["Alice", "Bob", "Carol"]
        assert set(out.columns) == {"name", "age"}


class TestStreamToSink:
    def test_stream_query_to_uri_end_to_end(self, people_parquet, tmp_path) -> None:
        ctx = _streaming_ctx()
        register_streaming_source(ctx, "Person", data_source_from_uri(str(people_parquet)))
        out = tmp_path / "adults.parquet"

        streamed = Star(context=ctx).stream_query_to_uri(
            "MATCH (n:Person) RETURN n.name AS name, n.age AS age",
            str(out),
        )
        assert streamed is True
        assert out.exists()
        got = pd.read_parquet(out).sort_values("name").reset_index(drop=True)
        assert got["name"].tolist() == ["Alice", "Bob", "Carol"]

    def test_stream_query_to_uri_returns_false_when_ineligible(
        self, people_parquet, tmp_path,
    ) -> None:
        ctx = _streaming_ctx()
        register_streaming_source(ctx, "Person", data_source_from_uri(str(people_parquet)))
        out = tmp_path / "x.parquet"
        # WHERE => ineligible => not streamed.
        streamed = Star(context=ctx).stream_query_to_uri(
            "MATCH (n:Person) WHERE n.age > 20 RETURN n.name AS name",
            str(out),
        )
        assert streamed is False
        assert not out.exists()

    def test_stream_query_to_uri_false_when_disabled(self, people_parquet, tmp_path) -> None:
        ctx = Context(
            entity_mapping=EntityMapping(mapping={}),
            relationship_mapping=RelationshipMapping(mapping={}),
            backend="duckdb",
        )
        register_streaming_source(ctx, "Person", data_source_from_uri(str(people_parquet)))
        # engine disabled (default) => not streamed.
        streamed = Star(context=ctx).stream_query_to_uri(
            "MATCH (n:Person) RETURN n.name AS name",
            str(tmp_path / "x.parquet"),
        )
        assert streamed is False


class TestOutOfCore:
    @pytest.mark.slow
    def test_stream_larger_than_memory_limit(
        self, tmp_path, monkeypatch,
    ) -> None:
        # Source parquet with many rows; a low memory_limit + temp_directory on
        # the backend connection (via env, per Phase 1).  The full path
        # file -> relation -> projection -> COPY must complete correctly
        # without loading the source into a pandas frame.
        monkeypatch.setenv("PYCYPHER_DUCKDB_MEMORY_LIMIT", "300MB")
        monkeypatch.setenv("PYCYPHER_DUCKDB_TEMP_DIRECTORY", str(tmp_path / "spill"))

        n = 5_000_000
        src = tmp_path / "big.parquet"
        pd.DataFrame(
            {"name": np.arange(n).astype(str), "v": np.ones(n, dtype="int64")},
        ).to_parquet(src)

        ctx = _streaming_ctx()
        register_streaming_source(ctx, "Rec", data_source_from_uri(str(src)))
        out = tmp_path / "out.parquet"

        streamed = Star(context=ctx).stream_query_to_uri(
            "MATCH (r:Rec) RETURN r.name AS name, r.v AS v",
            str(out),
        )
        assert streamed is True
        # Verify row count via a lazy DuckDB scan of the output (no full load).
        import duckdb

        con = duckdb.connect()
        try:
            (count,) = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [str(out)],
            ).fetchone()
        finally:
            con.close()
        assert count == n
