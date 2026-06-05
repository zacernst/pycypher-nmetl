"""End-to-end test: property-level query dependencies survive ``nmetl run``.

Mirrors the fastopendata OSM scenario that motivated property-level dependency
tracking — one query SETs a property, another reads it. Without the fix, YAML
definition order determines execution order and the reader runs before the
writer, producing an empty column. With the fix, the analyzer creates a
dependency edge from reader → writer regardless of YAML order.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
from click.testing import CliRunner
from pycypher.nmetl_cli import cli
from pycypher.scalar_functions import ScalarFunctionRegistry


def _reset_registry() -> None:
    ScalarFunctionRegistry._instance = None


class TestPropertyDependencyEndToEnd:
    def test_writer_runs_before_reader_despite_yaml_order(self) -> None:
        _reset_registry()
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp).resolve()

            csv_path = tmp_dir / "people.csv"
            csv_path.write_text("__ID__,name\n1,alice\n2,bob\n")

            output_path = tmp_dir / "out.csv"
            config_path = tmp_dir / "pipeline.yaml"
            # The reader is defined BEFORE the writer in the YAML.  Without
            # property-level dependency tracking, the reader would run first
            # and observe a missing property; with tracking, the analyzer
            # forces writer-first execution.
            config_path.write_text(
                f"""
version: "1.0"
sources:
  entities:
    - id: people
      uri: file://{csv_path}
      entity_type: Person
      id_col: __ID__
functions:
  - callable: tests.fixtures.user_scalar_module.shout
queries:
  - id: reader
    inline: |
      MATCH (p:Person)
      RETURN id(p) AS pid, p.loud_name AS loud
  - id: writer
    inline: |
      MATCH (p:Person)
      SET p.loud_name = shout(p.name)
output:
  - query_id: reader
    uri: file://{output_path}
""",
            )

            result = runner.invoke(cli, ["run", str(config_path)])

            assert result.exit_code == 0, result.output
            df = pd.read_csv(output_path).sort_values("pid").reset_index(
                drop=True,
            )
            # The whole point: the loud column is populated, not empty.
            assert df["loud"].tolist() == ["ALICE", "BOB"]

    def test_dry_run_lists_writer_before_reader(self) -> None:
        # Verify the dependency analyzer the CLI uses agrees on order.
        _reset_registry()
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp).resolve()
            csv_path = tmp_dir / "p.csv"
            csv_path.write_text("__ID__,name\n1,a\n")

            config_path = tmp_dir / "pipeline.yaml"
            config_path.write_text(
                f"""
version: "1.0"
sources:
  entities:
    - id: people
      uri: file://{csv_path}
      entity_type: Person
      id_col: __ID__
queries:
  - id: reader
    inline: MATCH (p:Person) RETURN p.foo
  - id: writer
    inline: MATCH (p:Person) SET p.foo = 1
output: []
""",
            )

            result = runner.invoke(
                cli,
                ["list-queries", str(config_path), "--deps"],
            )

            assert result.exit_code == 0, result.output
            output = result.output
            # Both queries appear, and writer comes before reader in the
            # printed execution order.
            assert "writer" in output
            assert "reader" in output
            # Find an "Execution order:" line and check writer precedes reader.
            order_line = next(
                (
                    line
                    for line in output.splitlines()
                    if "Execution order" in line
                ),
                "",
            )
            assert order_line, output
            assert order_line.index("writer") < order_line.index("reader")
