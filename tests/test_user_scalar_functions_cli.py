"""Integration test for ``functions:`` YAML field driven through ``nmetl run``.

A YAML pipeline that registers a function from a Python module is run end-to-end
via the CLI; the test asserts the registered function actually executes inside
the query.
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


class TestUserFunctionsViaCli:
    def test_callable_form_runs_through_query(self) -> None:
        _reset_registry()
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp).resolve()
            csv_path = tmp_dir / "people.csv"
            csv_path.write_text("__ID__,name\n1,Alice\n2,Bob\n")

            cypher_path = tmp_dir / "shout.cypher"
            cypher_path.write_text(
                "MATCH (p:Person) RETURN shout(p.name) AS loud",
            )

            output_path = tmp_dir / "out.csv"
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
functions:
  - callable: tests.fixtures.user_scalar_module.shout
queries:
  - id: q1
    source: file://{cypher_path}
output:
  - query_id: q1
    uri: file://{output_path}
""",
            )

            result = runner.invoke(cli, ["run", str(config_path)])

            assert result.exit_code == 0, result.output
            assert "registered tests.fixtures.user_scalar_module.shout" in (
                result.output
            )
            df = pd.read_csv(output_path)
            assert sorted(df["loud"].tolist()) == ["ALICE", "BOB"]

    def test_module_form_with_explicit_names(self) -> None:
        _reset_registry()
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp).resolve()
            csv_path = tmp_dir / "people.csv"
            csv_path.write_text("__ID__,age\n1,20\n2,30\n")

            cypher_path = tmp_dir / "plus.cypher"
            # Use the default n=1 by passing a single argument.
            cypher_path.write_text(
                "MATCH (p:Person) RETURN plus_n(p.age) AS bumped",
            )

            output_path = tmp_dir / "out.csv"
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
functions:
  - module: tests.fixtures.user_scalar_module
    names:
      - plus_n
queries:
  - id: q1
    source: file://{cypher_path}
output:
  - query_id: q1
    uri: file://{output_path}
""",
            )

            result = runner.invoke(cli, ["run", str(config_path)])

            assert result.exit_code == 0, result.output
            df = pd.read_csv(output_path)
            assert sorted(df["bumped"].tolist()) == [21, 31]

    def test_module_wildcard_skips_underscore_names(self) -> None:
        _reset_registry()
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp).resolve()
            csv_path = tmp_dir / "p.csv"
            csv_path.write_text("__ID__,name\n1,x\n")

            cypher_path = tmp_dir / "q.cypher"
            cypher_path.write_text("MATCH (p:Person) RETURN p.name AS name")

            output_path = tmp_dir / "out.csv"
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
functions:
  - module: tests.fixtures.user_scalar_module
    names: "*"
queries:
  - id: q1
    source: file://{cypher_path}
output:
  - query_id: q1
    uri: file://{output_path}
""",
            )

            result = runner.invoke(cli, ["run", str(config_path)])

            assert result.exit_code == 0, result.output
            # Public functions registered, underscore-prefixed skipped.
            assert "registered tests.fixtures.user_scalar_module.shout" in (
                result.output
            )
            assert "registered tests.fixtures.user_scalar_module.plus_n" in (
                result.output
            )
            assert "_private_helper" not in result.output

    def test_callable_with_missing_attribute_errors(self) -> None:
        _reset_registry()
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp).resolve()
            csv_path = tmp_dir / "p.csv"
            csv_path.write_text("__ID__,name\n1,x\n")

            cypher_path = tmp_dir / "q.cypher"
            cypher_path.write_text("MATCH (p:Person) RETURN p.name")

            output_path = tmp_dir / "out.csv"
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
functions:
  - callable: tests.fixtures.user_scalar_module.does_not_exist
queries:
  - id: q1
    source: file://{cypher_path}
output:
  - query_id: q1
    uri: file://{output_path}
""",
            )

            result = runner.invoke(cli, ["run", str(config_path)])

            assert result.exit_code != 0
            assert "does_not_exist" in result.output
