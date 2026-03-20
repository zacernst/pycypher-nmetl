"""TDD tests for dependency hygiene (Loop 186).

Problem: ``packages/pycypher/pyproject.toml`` lists four dependencies that
have zero imports anywhere in the production or test codebase:

- ``ply>=3.11,<4.0.0``          — leftover from a PLY-based parser that was
                                   replaced by Lark; the file that used it
                                   (``cypher_parser.py``) no longer exists.
- ``python-constraint>=1.4.0``  — leftover from an early SAT-solver approach
                                   that was abandoned.
- ``networkx==3.4.2,<4.0.0``    — BFS and path operations are implemented
                                   directly in ``star.py``; networkx is never
                                   imported.
- ``ibis-framework>=11.0.0``    — the DuckDB integration uses the ``duckdb``
                                   package directly; ibis is never imported.

Also: two lower bounds are stale relative to what is actually tested:

- ``pandas>=2.0.0``  — the installed and tested version is 3.0.1; pandas 2.x
                        behaviour has never been validated.
- ``duckdb>=0.10.0`` — the installed and tested version is 1.5.0; pre-1.0
                        DuckDB had a completely different API.

Fix:
1. Remove ``ply``, ``python-constraint``, ``networkx``, ``ibis-framework``.
2. Update ``pandas>=2.0.0`` → ``pandas>=3.0.0``.
3. Update ``duckdb>=0.10.0`` → ``duckdb>=1.0.0``.
4. Clean up the ``[tool.pyrefly]`` section that excludes the non-existent
   ``cypher_parser.py``.

All tests in this file will FAIL before the edits and PASS after.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PYPROJECT_PATH = (
    Path(__file__).parent.parent / "packages" / "pycypher" / "pyproject.toml"
)


def _load_pyproject() -> dict:
    return tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))


def _dependency_names(pyproject: dict) -> list[str]:
    """Return the bare package names from the dependencies list."""
    deps: list[str] = pyproject.get("project", {}).get("dependencies", [])
    names = []
    for dep in deps:
        # Strip version specifier — take the package name before any >=, ==, <, etc.
        name = (
            dep.split(">")[0].split("=")[0].split("<")[0].split("[")[0].strip()
        )
        names.append(name.lower())
    return names


# ===========================================================================
# Category 1 — Dead dependency removal
# ===========================================================================


class TestDeadDependenciesRemoved:
    """Removed packages must no longer appear in pyproject.toml dependencies."""

    def test_ply_not_in_dependencies(self) -> None:
        """ply must not be listed as a dependency (it has no importers)."""
        names = _dependency_names(_load_pyproject())
        assert "ply" not in names, (
            "ply is still listed as a dependency but is never imported. "
            "Remove 'ply>=3.11,<4.0.0' from packages/pycypher/pyproject.toml."
        )

    def test_python_constraint_not_in_dependencies(self) -> None:
        """python-constraint must not be listed (it has no importers)."""
        names = _dependency_names(_load_pyproject())
        assert "python-constraint" not in names, (
            "python-constraint is still listed but is never imported. "
            "Remove 'python-constraint>=1.4.0,<2.0.0' from pyproject.toml."
        )

    def test_networkx_not_in_dependencies(self) -> None:
        """networkx must not be listed (BFS is implemented in star.py directly)."""
        names = _dependency_names(_load_pyproject())
        assert "networkx" not in names, (
            "networkx is still listed but is never imported. "
            "Remove 'networkx==3.4.2,<4.0.0' from pyproject.toml."
        )

    def test_ibis_framework_not_in_dependencies(self) -> None:
        """ibis-framework must not be listed (DuckDB is used directly)."""
        names = _dependency_names(_load_pyproject())
        assert "ibis-framework" not in names, (
            "ibis-framework is still listed but is never imported. "
            "Remove 'ibis-framework>=11.0.0' from pyproject.toml."
        )


# ===========================================================================
# Category 2 — Lower-bound accuracy
# ===========================================================================


class TestLowerBoundsAccurate:
    """Lower bounds must reflect what the code is actually tested against."""

    def test_pandas_lower_bound_is_3(self) -> None:
        """pandas lower bound must be >=3.0.0 (only 3.x is tested)."""
        deps: list[str] = (
            _load_pyproject().get("project", {}).get("dependencies", [])
        )
        pandas_dep = next(
            (d for d in deps if d.lower().startswith("pandas")), None
        )
        assert pandas_dep is not None, (
            "pandas must still be listed as a dependency"
        )
        assert "3" in pandas_dep.split(">=")[-1].split(",")[0].split(".")[0], (
            f"pandas lower bound should be >=3.0.0 (tested on 3.x), "
            f"got: {pandas_dep!r}. Update to 'pandas>=3.0.0'."
        )

    def test_duckdb_lower_bound_is_1(self) -> None:
        """duckdb lower bound must be >=1.0.0 (pre-1.0 API is incompatible)."""
        deps: list[str] = (
            _load_pyproject().get("project", {}).get("dependencies", [])
        )
        duckdb_dep = next(
            (d for d in deps if d.lower().startswith("duckdb")), None
        )
        assert duckdb_dep is not None, (
            "duckdb must still be listed as a dependency"
        )
        lower = duckdb_dep.split(">=")[-1].split(",")[0].strip()
        major = int(lower.split(".")[0])
        assert major >= 1, (
            f"duckdb lower bound should be >=1.0.0 (tested on 1.x), "
            f"got lower={lower!r} in {duckdb_dep!r}. "
            "Update to 'duckdb>=1.0.0'."
        )


# ===========================================================================
# Category 3 — Core functionality still works after removal
# ===========================================================================


class TestCoreFunctionalityIntact:
    """Core query execution must work after removing dead dependencies."""

    def test_star_execute_query_works(self) -> None:
        """Basic MATCH + RETURN query executes correctly."""
        import pandas as pd
        from pycypher.relational_models import (
            ID_COLUMN,
            Context,
            EntityMapping,
            EntityTable,
            RelationshipMapping,
        )
        from pycypher.star import Star

        df = pd.DataFrame({ID_COLUMN: [1, 2], "name": ["Alice", "Bob"]})
        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name"],
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=df,
        )
        star = Star(
            context=Context(
                entity_mapping=EntityMapping(mapping={"Person": table}),
                relationship_mapping=RelationshipMapping(mapping={}),
            )
        )
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name ORDER BY p.name"
        )
        assert list(result["name"]) == ["Alice", "Bob"]

    def test_grammar_parser_works(self) -> None:
        """Grammar parser can parse a complex query without ply installed."""
        from pycypher.grammar_parser import get_default_parser

        parser = get_default_parser()
        tree = parser.parse(
            "MATCH (n:Person)-[:KNOWS]->(m:Person) "
            "WHERE n.age > 30 "
            "RETURN n.name, count(m) AS friends"
        )
        assert tree is not None

    def test_context_builder_works(self) -> None:
        """ContextBuilder.from_dict works without networkx or ibis installed."""
        import pandas as pd
        from pycypher.ingestion.context_builder import ContextBuilder

        cb = ContextBuilder.from_dict(
            {
                "Person": pd.DataFrame({"__ID__": [1, 2], "name": ["X", "Y"]}),
            }
        )
        assert cb is not None

    def test_scalar_functions_work(self) -> None:
        """Scalar function registry initialises and executes correctly."""
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            RelationshipMapping,
        )
        from pycypher.star import Star

        star = Star(
            context=Context(
                entity_mapping=EntityMapping(mapping={}),
                relationship_mapping=RelationshipMapping(mapping={}),
            )
        )
        result = star.execute_query("RETURN toUpper('hello') AS v")
        assert result["v"].iloc[0] == "HELLO"


# ===========================================================================
# Category 4 — Stale pyrefly exclusion removed
# ===========================================================================


class TestPyreflyCleanup:
    """The pyrefly section must not exclude non-existent files."""

    def test_cypher_parser_exclusion_removed(self) -> None:
        """The pyrefly project_excludes must not reference cypher_parser.py."""
        pyproject = _load_pyproject()
        pyrefly = pyproject.get("tool", {}).get("pyrefly", {})
        excludes = pyrefly.get("project_excludes", [])
        cypher_parser_refs = [e for e in excludes if "cypher_parser" in e]
        assert not cypher_parser_refs, (
            f"pyrefly still excludes {cypher_parser_refs!r} but "
            "cypher_parser.py does not exist. Remove the stale exclusion."
        )
