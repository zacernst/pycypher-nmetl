"""Shared pytest fixtures for pycypher tests."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Suppress known upstream deprecation warnings
# ---------------------------------------------------------------------------
# (neo4j asyncio.iscoroutinefunction suppression removed — fixed in neo4j 6.1.0)


# ---------------------------------------------------------------------------
# Per-marker test timeouts (override the global 30s default)
# ---------------------------------------------------------------------------
_MARKER_TIMEOUTS = {
    "integration": 120,
    "slow": 300,
}


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Apply per-marker timeouts so integration/slow tests aren't killed early."""
    for item in items:
        if item.get_closest_marker("timeout"):
            continue  # explicit timeout takes precedence
        for marker_name, seconds in _MARKER_TIMEOUTS.items():
            if item.get_closest_marker(marker_name):
                item.add_marker(pytest.mark.timeout(seconds))
                break


FIXTURES_DATA = Path(__file__).parent / "fixtures" / "data"
ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Parquet fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def sample_parquet_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Write sample.parquet to a session-scoped temp dir and return its path."""
    tmp = tmp_path_factory.mktemp("fixtures")
    p = tmp / "sample.parquet"
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["Alice", "Bob"], type=pa.string()),
            "value": pa.array([1.1, 2.2], type=pa.float64()),
        }
    )
    pq.write_table(table, str(p))
    return p


# ---------------------------------------------------------------------------
# Scalar function registry (shared across ~25 test files)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def scalar_registry() -> ScalarFunctionRegistry:
    """Shared ScalarFunctionRegistry singleton."""
    return ScalarFunctionRegistry.get_instance()


# ---------------------------------------------------------------------------
# Common DataFrames (shared across many test files)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _people_df_template() -> pd.DataFrame:
    """Session-scoped template — never mutated, shared across all tests."""
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "age": [30, 25, 35, 28],
            "dept": ["eng", "mktg", "eng", "sales"],
            "salary": [100_000, 80_000, 110_000, 90_000],
        }
    )


@pytest.fixture()
def people_df(_people_df_template: pd.DataFrame) -> pd.DataFrame:
    """Four-person DataFrame used across 13+ test files (copy per test)."""
    return _people_df_template.copy()


@pytest.fixture(scope="session")
def _knows_df_template() -> pd.DataFrame:
    """Session-scoped template — never mutated, shared across all tests."""
    return pd.DataFrame(
        {
            ID_COLUMN: [101, 102, 103],
            "__SOURCE__": [1, 2, 3],
            "__TARGET__": [2, 3, 1],
            "since": [2020, 2021, 2019],
        }
    )


@pytest.fixture()
def knows_df(_knows_df_template: pd.DataFrame) -> pd.DataFrame:
    """Friendship relationships between people (copy per test)."""
    return _knows_df_template.copy()


# ---------------------------------------------------------------------------
# Context and Star builders (shared patterns across 30+ test files)
# ---------------------------------------------------------------------------


@pytest.fixture()
def person_entity_table(people_df: pd.DataFrame) -> EntityTable:
    """EntityTable for Person nodes."""
    return EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "dept", "salary"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "dept": "dept",
            "salary": "salary",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "dept": "dept",
            "salary": "salary",
        },
        source_obj=people_df,
    )


@pytest.fixture()
def knows_rel_table(knows_df: pd.DataFrame) -> RelationshipTable:
    """RelationshipTable for KNOWS edges."""
    return RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__", "since"],
        source_obj_attribute_map={"since": "since"},
        attribute_map={"since": "since"},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )


@pytest.fixture()
def person_context(person_entity_table: EntityTable) -> Context:
    """Context with Person entities only (no relationships)."""
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_entity_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture()
def social_context(
    person_entity_table: EntityTable,
    knows_rel_table: RelationshipTable,
) -> Context:
    """Context with Person entities and KNOWS relationships."""
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_entity_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_rel_table}
        ),
    )


@pytest.fixture()
def person_star(person_context: Context) -> Star:
    """Star with Person entities only."""
    return Star(context=person_context)


@pytest.fixture()
def social_star(social_context: Context) -> Star:
    """Star with Person entities and KNOWS relationships."""
    return Star(context=social_context)


@pytest.fixture()
def empty_context() -> Context:
    """Empty context with no entities or relationships."""
    return Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture()
def empty_star(empty_context: Context) -> Star:
    """Star with empty context."""
    return Star(context=empty_context)


# ---------------------------------------------------------------------------
# Factory fixtures for DRY test setup
# ---------------------------------------------------------------------------


def make_entity_table(
    entity_type: str,
    data: dict[str, list[object]],
    id_col: str = ID_COLUMN,
) -> EntityTable:
    """Create an EntityTable from a column dict.

    Automatically includes *id_col* in the attribute map and column names.
    All keys in *data* except *id_col* become properties.

    Args:
        entity_type: Label for the entity (e.g. ``"Person"``).
        data: Column dict passed to :class:`pandas.DataFrame`.
        id_col: Name of the ID column (default ``"__ID__"``).

    Returns:
        A ready-to-use :class:`EntityTable`.

    Example::

        et = make_entity_table("Person", {"__ID__": [1, 2], "name": ["A", "B"]})

    """
    df = pd.DataFrame(data)
    props = [c for c in df.columns if c != id_col]
    attr_map = {p: p for p in props}
    return EntityTable(
        entity_type=entity_type,
        identifier=entity_type,
        column_names=list(df.columns),
        source_obj_attribute_map=attr_map,
        attribute_map=attr_map,
        source_obj=df,
    )


def make_rel_table(
    rel_type: str,
    data: dict[str, list[object]],
    source_entity_type: str = "",
    target_entity_type: str = "",
) -> RelationshipTable:
    """Create a RelationshipTable from a column dict.

    *data* must include ``__ID__``, ``__SOURCE__``, and ``__TARGET__``
    columns.  All other columns become relationship properties.

    Args:
        rel_type: Relationship type label (e.g. ``"KNOWS"``).
        data: Column dict passed to :class:`pandas.DataFrame`.
        source_entity_type: Source entity label.
        target_entity_type: Target entity label.

    Returns:
        A ready-to-use :class:`RelationshipTable`.

    """
    df = pd.DataFrame(data)
    reserved = {ID_COLUMN, "__SOURCE__", "__TARGET__"}
    props = [c for c in df.columns if c not in reserved]
    attr_map = {p: p for p in props}
    return RelationshipTable(
        relationship_type=rel_type,
        identifier=rel_type,
        column_names=list(df.columns),
        source_obj_attribute_map=attr_map,
        attribute_map=attr_map,
        source_obj=df,
        source_entity_type=source_entity_type,
        target_entity_type=target_entity_type,
    )


def make_context(
    entities: dict[str, dict[str, list[object]]],
    relationships: dict[str, dict[str, list[object]]] | None = None,
) -> Context:
    """Build a Context from dicts of column data.

    Args:
        entities: ``{label: column_dict}`` for each entity type.
        relationships: ``{rel_type: column_dict}`` for each relationship type.

    Returns:
        A :class:`Context` ready for query execution.

    Example::

        ctx = make_context(
            {"Person": {"__ID__": [1, 2], "name": ["A", "B"]}},
            {"KNOWS": {"__ID__": [10], "__SOURCE__": [1], "__TARGET__": [2]}},
        )

    """
    entity_tables = {
        label: make_entity_table(label, data)
        for label, data in entities.items()
    }
    rel_tables = {
        rt: make_rel_table(rt, data)
        for rt, data in (relationships or {}).items()
    }
    return Context(
        entity_mapping=EntityMapping(mapping=entity_tables),
        relationship_mapping=RelationshipMapping(mapping=rel_tables),
    )


def make_star(
    entities: dict[str, dict[str, list[object]]],
    relationships: dict[str, dict[str, list[object]]] | None = None,
) -> Star:
    """One-liner Star creation from column dicts.

    Args:
        entities: ``{label: column_dict}`` for each entity type.
        relationships: ``{rel_type: column_dict}`` for each relationship type.

    Returns:
        A :class:`Star` ready for ``execute_query()``.

    Example::

        star = make_star({"Person": {"__ID__": [1, 2], "name": ["A", "B"]}})
        result = star.execute_query("MATCH (p:Person) RETURN p.name")

    """
    return Star(context=make_context(entities, relationships))


# ---------------------------------------------------------------------------
# Spark fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark_session():  # type: ignore[return]
    """Session-scoped SparkSession.

    Skips if PySpark is not installed or the cluster is not reachable.
    Defaults to local[*] when SPARK_MASTER_URL is not set, so unit tests
    can run without a cluster.
    """
    pytest.importorskip("pyspark", reason="pyspark not installed")
    from pyspark.sql import SparkSession  # type: ignore[import-untyped]

    master = os.environ.get("SPARK_MASTER_URL", "local[*]")
    try:
        spark = (
            SparkSession.builder.appName("pycypher-test")
            .master(master)
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        yield spark
        spark.stop()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Could not connect to Spark ({master}): {exc}")


# ---------------------------------------------------------------------------
# Neo4j fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def neo4j_driver():  # type: ignore[return]
    """Session-scoped Neo4j driver.

    Skips if the neo4j driver package is absent or the database is not
    reachable.
    """
    pytest.importorskip("neo4j", reason="neo4j driver not installed")
    from neo4j import GraphDatabase  # type: ignore[import-untyped]

    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    pw = os.environ.get("NEO4J_PASSWORD", "pycypher")
    try:
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        driver.verify_connectivity()
        yield driver
        driver.close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Could not connect to Neo4j ({uri}): {exc}")


@pytest.fixture
def neo4j_session(neo4j_driver):  # type: ignore[return]
    """Function-scoped Neo4j session.

    Wipes the graph before each test for a clean starting state.
    """
    with neo4j_driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        yield session
