"""Phase 8 — end-to-end ``nmetl run`` with the Spark backend.

Exercises the full pipeline run path (config load → ContextBuilder.build with
``backend_engine: spark`` → Star.execute_query → CSV sink) via Click's
CliRunner, and verifies the run-path ``context.backend.close()`` does not
tear down a shared SparkSession.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pytest
from click.testing import CliRunner
from pycypher.nmetl_cli import cli

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.spark


def _write_config(tmp_path: Path, out_path: Path) -> Path:
    csv = tmp_path / "people.csv"
    csv.write_text("id,name,age\n1,alice,30\n2,bob,40\n3,carol,50\n")
    cfg = tmp_path / "pipeline.yaml"
    cfg.write_text(
        f"""\
version: "1.0"
backend_engine: spark
sources:
  entities:
    - id: people_src
      uri: "{csv}"
      entity_type: Person
      id_col: id
queries:
  - id: adults
    inline: "MATCH (p:Person) WHERE p.age > 35 RETURN p.name AS name, p.age AS age"
output:
  - query_id: adults
    uri: "{out_path}"
    format: csv
""",
    )
    return cfg


@pytest.mark.usefixtures("spark_session")
def test_nmetl_run_with_spark_backend(tmp_path: Path) -> None:
    out_path = tmp_path / "out.csv"
    cfg = _write_config(tmp_path, out_path)

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(cfg)])

    assert result.exit_code == 0, result.output
    assert out_path.exists()
    df = pd.read_csv(out_path)
    assert set(df["name"]) == {"bob", "carol"}
    assert sorted(df["age"].tolist()) == [40, 50]


def test_run_path_close_does_not_stop_shared_session(
    spark_session,
    tmp_path: Path,
) -> None:
    # A session already exists (fixture).  The pipeline's SparkBackend must not
    # own it, so run_impl's context.backend.close() must leave it usable.
    out_path = tmp_path / "out2.csv"
    cfg = _write_config(tmp_path, out_path)

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(cfg)])
    assert result.exit_code == 0, result.output

    # Shared session still works after the run completed and closed its backend.
    assert spark_session.createDataFrame(
        pd.DataFrame({"x": [1, 2]}),
    ).count() == 2
