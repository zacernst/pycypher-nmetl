"""Auto-bridge: registry user functions → DuckDB UDFs for out-of-core use.

Verifies bridge_user_functions exposes annotated user scalar functions to the
relation engine (so functions: config UDFs work out-of-core), skips unannotated
ones, and that nmetl run streams a pipeline using a bridged function.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pytest
from click.testing import CliRunner
from pycypher.ast_converter import ASTConverter
from pycypher.nmetl_cli import cli
from pycypher.relation_engine import (
    bridge_user_functions,
    is_relation_eligible,
)
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.scalar_functions.user_functions import register_user_function
from pycypher.star import Star

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def _fresh_registry():
    """Isolate the singleton registry per test so registered funcs don't leak."""
    reg = ScalarFunctionRegistry.get_instance()
    saved = dict(reg._functions)
    yield
    reg._functions.clear()
    reg._functions.update(saved)


def _ctx() -> Context:
    people = pd.DataFrame({"__ID__": [1, 2, 3], "name": ["a", "bb", "ccc"], "n": [1, 2, 3]})
    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": EntityTable.from_dataframe("Person", people)}),
        relationship_mapping=RelationshipMapping(mapping={}),
        backend="duckdb",
    )
    ctx._relation_engine_enabled = True
    return ctx


def _annotated(x: int) -> int:
    return x * 100


def _unannotated(x):
    return x * 100


class TestBridge:
    def test_annotated_udf_bridged_and_usable(self) -> None:
        register_user_function(_annotated, name="hundredx")
        ctx = _ctx()
        bridge_user_functions(ctx)
        q = "MATCH (p:Person) RETURN p.name AS name, hundredx(p.n) AS big"
        assert is_relation_eligible(ASTConverter.from_cypher(q), ctx)
        out = Star(context=ctx).execute_query(q).sort_values("name").reset_index(drop=True)
        assert out["big"].tolist() == [100, 200, 300]

    def test_unannotated_udf_not_bridged(self) -> None:
        register_user_function(_unannotated, name="noanno")
        ctx = _ctx()
        bridge_user_functions(ctx)
        # Not bridged (no annotations) → query using it is ineligible → fallback.
        q = "MATCH (p:Person) RETURN noanno(p.n) AS big"
        assert not is_relation_eligible(ASTConverter.from_cypher(q), ctx)

    def test_bridge_is_idempotent(self) -> None:
        register_user_function(_annotated, name="hundredx")
        ctx = _ctx()
        bridge_user_functions(ctx)
        bridge_user_functions(ctx)  # must not raise on re-register
        assert is_relation_eligible(
            ASTConverter.from_cypher("MATCH (p:Person) RETURN hundredx(p.n) AS b"), ctx,
        )


class TestNmetlRun:
    def test_run_streams_with_bridged_function(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PYCYPHER_DUCKDB_RELATION_ENGINE", "1")
        # A functions module the pipeline can import.
        mod = tmp_path / "udfs.py"
        mod.write_text("def hundredx(x: int) -> int:\n    return x * 100\n")
        monkeypatch.syspath_prepend(str(tmp_path))

        src = tmp_path / "people.parquet"
        pd.DataFrame({"id": [1, 2, 3], "n": [1, 2, 3]}).to_parquet(src)
        out = tmp_path / "out.parquet"
        cfg = tmp_path / "pipe.yaml"
        cfg.write_text(
            f"""\
version: "1.0"
backend_engine: duckdb
functions:
  - callable: "udfs.hundredx"
sources:
  entities:
    - id: people_src
      uri: "{src}"
      entity_type: Person
      id_col: id
queries:
  - id: q1
    inline: "MATCH (p:Person) RETURN hundredx(p.n) AS big"
output:
  - query_id: q1
    uri: "{out}"
    format: parquet
""",
        )
        result = CliRunner().invoke(cli, ["run", str(cfg)])
        assert result.exit_code == 0, result.output
        assert "out-of-core" in result.output  # streamed via the bridged UDF
        got = pd.read_parquet(out)
        assert sorted(got["big"].tolist()) == [100, 200, 300]
