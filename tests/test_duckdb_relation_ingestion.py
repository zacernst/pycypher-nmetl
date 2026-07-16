"""Phase 2 (out-of-core DuckDB) — relation-returning ingestion.

Verifies ``DataSource.read_relation(con)`` returns a lazy ``DuckDBLazyFrame``
over a caller-owned persistent connection, that ``FileDataSource`` scans files
directly (no full Arrow materialisation), that schema hints and user queries
still apply, that multiple sources share one connection without collision, and
that the existing ``.read()`` Arrow path is unchanged.

See docs/duckdb_out_of_core_design.md, Phase 2.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.backends.duckdb_backend import (
    DuckDBLazyFrame,
    create_duckdb_connection,
)
from pycypher.ingestion.data_sources import data_source_from_uri


@pytest.fixture
def con():
    c = create_duckdb_connection()
    yield c
    c.close()


@pytest.fixture
def people_parquet(tmp_path):
    path = tmp_path / "people.parquet"
    pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "age": [30, 25, 35, 28],
        },
    ).to_parquet(path)
    return path


@pytest.fixture
def people_csv(tmp_path):
    path = tmp_path / "people.csv"
    path.write_text("id,name,age\n1,Alice,30\n2,Bob,25\n3,Carol,35\n")
    return path


class TestFileRelation:
    def test_returns_lazy_frame(self, con, people_parquet) -> None:
        ds = data_source_from_uri(str(people_parquet))
        rel = ds.read_relation(con)
        assert isinstance(rel, DuckDBLazyFrame)

    def test_columns_without_materialising(self, con, people_parquet) -> None:
        ds = data_source_from_uri(str(people_parquet))
        rel = ds.read_relation(con)
        # .columns is answered from the relation schema, not a full read.
        assert set(rel.columns) == {"id", "name", "age"}
        # And the lazy frame has not cached a materialised DataFrame yet.
        assert rel._materialised is None

    def test_relation_matches_read(self, con, people_parquet) -> None:
        ds = data_source_from_uri(str(people_parquet))
        via_relation = ds.read_relation(con).to_pandas().sort_values("id").reset_index(drop=True)
        via_arrow = ds.read().to_pandas().sort_values("id").reset_index(drop=True)
        pd.testing.assert_frame_equal(via_relation, via_arrow)

    def test_csv_relation(self, con, people_csv) -> None:
        ds = data_source_from_uri(str(people_csv))
        out = ds.read_relation(con).to_pandas()
        assert sorted(out["name"].tolist()) == ["Alice", "Bob", "Carol"]

    def test_row_count_without_full_materialise(self, con, people_parquet) -> None:
        ds = data_source_from_uri(str(people_parquet))
        rel = ds.read_relation(con)
        assert len(rel) == 4  # COUNT(*), no fetchdf
        assert rel._materialised is None


class TestSchemaHintsAndQuery:
    def test_schema_hint_cast(self, con, people_csv) -> None:
        ds = data_source_from_uri(str(people_csv), schema_hints={"age": "VARCHAR"})
        out = ds.read_relation(con).to_pandas().sort_values("id")
        # age was hinted to VARCHAR, so values come back as strings not ints.
        assert out["age"].tolist() == ["30", "25", "35"]

    def test_user_query_applied_via_cte(self, con, people_parquet) -> None:
        ds = data_source_from_uri(
            str(people_parquet),
            query="SELECT id, name FROM source WHERE age > 29",
        )
        out = ds.read_relation(con).to_pandas().sort_values("id")
        assert out["name"].tolist() == ["Alice", "Carol"]
        assert "age" not in out.columns


class TestSharedConnection:
    def test_two_sources_no_collision(self, con, people_parquet, people_csv) -> None:
        # Both sources use a `source` CTE; on one shared connection they must
        # not collide.
        a = data_source_from_uri(str(people_parquet), query="SELECT id FROM source WHERE age > 29")
        b = data_source_from_uri(str(people_csv), query="SELECT id FROM source WHERE age < 30")
        out_a = a.read_relation(con).to_pandas()
        out_b = b.read_relation(con).to_pandas()
        assert sorted(out_a["id"].tolist()) == [1, 3]
        assert sorted(out_b["id"].tolist()) == [2]


class TestArrowReadUnchanged:
    def test_read_still_returns_arrow(self, people_parquet) -> None:
        import pyarrow as pa

        ds = data_source_from_uri(str(people_parquet))
        table = ds.read()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 4


class TestLargerThanMemory:
    @pytest.mark.slow
    def test_aggregate_under_low_memory_limit(self, tmp_path) -> None:
        # Generate a parquet with enough rows that a fully-materialised pandas
        # copy is non-trivial, set a low memory_limit + temp_directory, and
        # confirm a scan+aggregate over the *relation* returns the correct
        # result (DuckDB streams/spills instead of loading it all into RAM).
        import numpy as np

        n = 5_000_000
        path = tmp_path / "big.parquet"
        pd.DataFrame(
            {"g": np.arange(n) % 10, "v": np.ones(n, dtype="int64")},
        ).to_parquet(path)

        c = create_duckdb_connection(
            memory_limit="300MB",
            temp_directory=str(tmp_path / "spill"),
        )
        try:
            ds = data_source_from_uri(str(path))
            rel = ds.read_relation(c)
            grouped = (
                rel.relation.aggregate("g, COUNT(*) AS n", "g")
                .fetchdf()
                .sort_values("g")
                .reset_index(drop=True)
            )
            assert grouped["n"].tolist() == [n // 10] * 10
        finally:
            c.close()
