"""Phase 5b (out-of-core DuckDB) — nmetl run streaming integration.

Verifies the run_impl streaming fast path: with the relation engine enabled and
a duckdb backend, an all-eligible pipeline streams each query file->sink via
DuckDB (out-of-core); ineligible pipelines and the disabled default fall back to
the normal in-memory path unchanged.

See docs/duckdb_out_of_core_design.md, Phase 5b.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from click.testing import CliRunner
from pycypher.nmetl_cli import cli

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def _write_people(tmp_path: Path) -> Path:
    src = tmp_path / "people.parquet"
    pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"], "age": [30, 25, 35]},
    ).to_parquet(src)
    return src


def _config(tmp_path: Path, out: Path, query: str) -> Path:
    src = _write_people(tmp_path)
    cfg = tmp_path / "pipeline.yaml"
    cfg.write_text(
        f"""\
version: "1.0"
backend_engine: duckdb
sources:
  entities:
    - id: people_src
      uri: "{src}"
      entity_type: Person
      id_col: id
queries:
  - id: q1
    inline: "{query}"
output:
  - query_id: q1
    uri: "{out}"
    format: parquet
""",
    )
    return cfg


class TestStreamingRun:
    def test_eligible_pipeline_streams(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PYCYPHER_DUCKDB_RELATION_ENGINE", "1")
        out = tmp_path / "out.parquet"
        cfg = _config(tmp_path, out, "MATCH (n:Person) RETURN n.name AS name, n.age AS age")

        result = CliRunner().invoke(cli, ["run", str(cfg)])
        assert result.exit_code == 0, result.output
        assert "out-of-core" in result.output  # took the streaming path
        got = pd.read_parquet(out).sort_values("name").reset_index(drop=True)
        assert got["name"].tolist() == ["Alice", "Bob", "Carol"]
        assert set(got.columns) == {"name", "age"}


class TestFallback:
    def test_ineligible_pipeline_falls_back(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PYCYPHER_DUCKDB_RELATION_ENGINE", "1")
        out = tmp_path / "out.parquet"
        # SKIP without LIMIT is ineligible (WHERE/ORDER BY alone would now be
        # eligible) => normal in-memory path; ORDER BY keeps it deterministic.
        cfg = _config(
            tmp_path,
            out,
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name SKIP 2",
        )
        result = CliRunner().invoke(cli, ["run", str(cfg)])
        assert result.exit_code == 0, result.output
        assert "out-of-core" not in result.output  # fell back
        got = pd.read_parquet(out)
        assert got["name"].tolist() == ["Carol"]

    def test_disabled_by_default_uses_normal_path(self, tmp_path: Path) -> None:
        # No env flag => streaming never attempted => normal path, correct output.
        out = tmp_path / "out.parquet"
        cfg = _config(tmp_path, out, "MATCH (n:Person) RETURN n.name AS name, n.age AS age")
        result = CliRunner().invoke(cli, ["run", str(cfg)])
        assert result.exit_code == 0, result.output
        assert "out-of-core" not in result.output
        got = pd.read_parquet(out)
        assert sorted(got["name"].tolist()) == ["Alice", "Bob", "Carol"]
