"""Shared pytest fixtures for pycypher tests."""

from __future__ import annotations

import os
import signal
import threading
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
# SIGALRM cleanup — prevent alarm leakage between tests
# ---------------------------------------------------------------------------
# Some tests arm SIGALRM via execute_query(timeout_seconds=...).  If a query
# times out or runs close to the deadline, the pending alarm can fire during
# a later, unrelated test — causing a spurious QueryTimeoutError.  Clearing
# the alarm after every test eliminates this cross-test contamination.


@pytest.fixture(autouse=True)
def _clear_pending_sigalrm():
    """Cancel any pending SIGALRM after each test to prevent leakage."""
    yield
    if (
        hasattr(signal, "SIGALRM")
        and threading.current_thread() is threading.main_thread()
    ):
        signal.alarm(0)


# ---------------------------------------------------------------------------
# Logger level cleanup — prevent log level contamination between tests
# ---------------------------------------------------------------------------
# Tests that use caplog.at_level(logging.DEBUG, logger="shared.logger") can
# leave the shared logger at DEBUG if they fail or don't properly restore.
# This fixture saves and restores the level after each test.


@pytest.fixture(autouse=True)
def _restore_shared_logger_level():
    """Restore the shared logger level after each test."""
    import logging

    try:
        from shared.logger import LOGGER
    except ImportError:
        yield
        return
    original_level = LOGGER.level
    yield
    if LOGGER.level != original_level:
        LOGGER.setLevel(original_level)

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
    """Auto-apply markers based on file paths and apply per-marker timeouts.

    File-path conventions:
    - tests/benchmarks/          → ``performance``
    - tests/large_dataset/       → ``integration``, ``slow``
    - tests/load_testing/        → ``performance``, ``slow``
    - tests/property_based/      → ``unit``
    - *_e2e_*.py / *_end_to_end* → ``integration``
    - *_performance_*.py         → ``performance``
    - *_security_*.py            → ``security``

    These are only applied when the test does **not** already carry the
    marker, so explicit ``@pytest.mark.X`` always wins.
    """
    _PATH_MARKERS: list[tuple[str, list[str]]] = [
        ("benchmarks/", ["performance"]),
        ("large_dataset/", ["integration", "slow"]),
        ("load_testing/", ["performance", "slow"]),
        ("property_based/", ["unit"]),
    ]
    _NAME_MARKERS: list[tuple[str, list[str]]] = [
        ("_e2e_", ["integration"]),
        ("_end_to_end", ["integration"]),
        ("_performance_", ["performance"]),
        ("_security_", ["security"]),
    ]

    for item in items:
        fspath = str(item.fspath)

        # Auto-apply markers from directory conventions
        for pattern, markers in _PATH_MARKERS:
            if pattern in fspath:
                for m in markers:
                    if not item.get_closest_marker(m):
                        item.add_marker(getattr(pytest.mark, m))

        # Auto-apply markers from filename conventions
        fname = Path(fspath).name
        for pattern, markers in _NAME_MARKERS:
            if pattern in fname:
                for m in markers:
                    if not item.get_closest_marker(m):
                        item.add_marker(getattr(pytest.mark, m))

        # Apply per-marker timeouts
        if item.get_closest_marker("timeout"):
            continue  # explicit timeout takes precedence
        for marker_name, seconds in _MARKER_TIMEOUTS.items():
            if item.get_closest_marker(marker_name):
                item.add_marker(pytest.mark.timeout(seconds))
                break


FIXTURES_DATA = Path(__file__).parent / "fixtures" / "data"
ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Performance test tolerance — CI runners are slower than local machines
# ---------------------------------------------------------------------------
# Use perf_threshold() instead of hard-coded timing assertions so that tests
# pass reliably on GitHub Actions (shared runners), local laptops, and
# parallel test sessions.  The multiplier is applied when the CI environment
# variable is set (GitHub Actions, most CI systems).

_CI_PERF_MULTIPLIER = float(os.environ.get("PYCYPHER_PERF_MULTIPLIER", "3.0"))
_IN_CI = os.environ.get("CI", "").lower() in ("true", "1", "yes")
# pytest-xdist sets PYTEST_XDIST_WORKER on worker processes
_IN_XDIST = "PYTEST_XDIST_WORKER" in os.environ
_XDIST_PERF_MULTIPLIER = float(os.environ.get("PYCYPHER_XDIST_PERF_MULTIPLIER", "2.0"))


def perf_threshold(seconds: float) -> float:
    """Return *seconds* scaled up when running in CI or under heavy load.

    On a local machine this returns ``seconds`` unchanged.  In CI it returns
    ``seconds * _CI_PERF_MULTIPLIER`` (default 3x) to absorb variance from
    shared runners, parallel test sessions, and Python version differences.

    When running under pytest-xdist (parallel workers), applies a 2x
    multiplier to account for CPU contention between workers.

    Override the multiplier via ``PYCYPHER_PERF_MULTIPLIER=5.0`` if needed.
    """
    if _IN_CI:
        return seconds * _CI_PERF_MULTIPLIER
    if _IN_XDIST:
        return seconds * _XDIST_PERF_MULTIPLIER
    return seconds


# ---------------------------------------------------------------------------
# Safe tracemalloc — prevent state coupling between tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def safe_tracemalloc():
    """Start tracemalloc cleanly, stop it after the test.

    Handles the case where tracemalloc is already running from a prior test
    that failed mid-execution (which would leave tracing enabled).
    """
    import tracemalloc as _tm

    was_tracing = _tm.is_tracing()
    if was_tracing:
        _tm.stop()
    _tm.start()
    yield _tm
    if _tm.is_tracing():
        _tm.stop()
    # Restore prior state if tracemalloc was already running
    if was_tracing:
        _tm.start()


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
        },
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
        },
    )


@pytest.fixture
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
        },
    )


@pytest.fixture
def knows_df(_knows_df_template: pd.DataFrame) -> pd.DataFrame:
    """Friendship relationships between people (copy per test)."""
    return _knows_df_template.copy()


# ---------------------------------------------------------------------------
# Context and Star builders (shared patterns across 30+ test files)
# ---------------------------------------------------------------------------


@pytest.fixture
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


@pytest.fixture
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


@pytest.fixture
def person_context(person_entity_table: EntityTable) -> Context:
    """Context with Person entities only (no relationships)."""
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_entity_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture
def social_context(
    person_entity_table: EntityTable,
    knows_rel_table: RelationshipTable,
) -> Context:
    """Context with Person entities and KNOWS relationships."""
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_entity_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_rel_table},
        ),
    )


@pytest.fixture
def person_star(person_context: Context) -> Star:
    """Star with Person entities only."""
    return Star(context=person_context)


@pytest.fixture
def social_star(social_context: Context) -> Star:
    """Star with Person entities and KNOWS relationships."""
    return Star(context=social_context)


@pytest.fixture
def empty_context() -> Context:
    """Empty context with no entities or relationships."""
    return Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture
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
    except Exception as exc:
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
    except Exception as exc:
        pytest.skip(f"Could not connect to Neo4j ({uri}): {exc}")


@pytest.fixture
def neo4j_session(neo4j_driver):  # type: ignore[return]
    """Function-scoped Neo4j session.

    Wipes the graph before each test for a clean starting state.
    """
    with neo4j_driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        yield session
