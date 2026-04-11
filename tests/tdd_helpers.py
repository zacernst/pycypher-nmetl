"""TDD helper utilities for pycypher feature development.

Provides reusable test builders, assertion helpers, and performance
regression guards that support Test-Driven Development workflows across
all feature epics.

Usage::

    from tdd_helpers import (
        QueryTestCase,
        assert_query_result,
        assert_performance_within,
        assert_no_api_breakage,
        build_scaled_star,
    )

"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Query test case builder (TDD-friendly)
# ---------------------------------------------------------------------------


@dataclass
class QueryTestCase:
    """Declarative test case for Cypher query execution.

    Encapsulates a complete test scenario: graph data, query, and expected
    results. Designed for TDD red-green-refactor cycles.

    Example::

        case = QueryTestCase(
            name="simple_match",
            entities={"Person": {"__ID__": [1, 2], "name": ["Alice", "Bob"]}},
            query='MATCH (p:Person) RETURN p.name ORDER BY p.name',
            expected_columns=["p.name"],
            expected_rows=[["Alice"], ["Bob"]],
        )
        case.run()  # Raises AssertionError if result doesn't match

    """

    name: str
    query: str
    entities: dict[str, dict[str, list[Any]]] = field(default_factory=dict)
    relationships: dict[str, dict[str, list[Any]]] = field(default_factory=dict)
    expected_columns: list[str] | None = None
    expected_rows: list[list[Any]] | None = None
    expected_row_count: int | None = None
    expected_empty: bool = False
    max_duration_seconds: float | None = None

    def build_star(self) -> Star:
        """Build a Star from the test case data."""
        entity_tables = {}
        for label, data in self.entities.items():
            df = pd.DataFrame(data)
            props = [c for c in df.columns if c != ID_COLUMN]
            attr_map = {p: p for p in props}
            entity_tables[label] = EntityTable(
                entity_type=label,
                identifier=label,
                column_names=list(df.columns),
                source_obj_attribute_map=attr_map,
                attribute_map=attr_map,
                source_obj=df,
            )

        rel_tables = {}
        for rt, data in self.relationships.items():
            df = pd.DataFrame(data)
            reserved = {ID_COLUMN, "__SOURCE__", "__TARGET__"}
            props = [c for c in df.columns if c not in reserved]
            attr_map = {p: p for p in props}
            rel_tables[rt] = RelationshipTable(
                relationship_type=rt,
                identifier=rt,
                column_names=list(df.columns),
                source_obj_attribute_map=attr_map,
                attribute_map=attr_map,
                source_obj=df,
                source_entity_type="",
                target_entity_type="",
            )

        ctx = Context(
            entity_mapping=EntityMapping(mapping=entity_tables),
            relationship_mapping=RelationshipMapping(mapping=rel_tables),
        )
        return Star(context=ctx)

    def run(self) -> pd.DataFrame:
        """Execute the test case and validate all assertions.

        Returns the result DataFrame for further inspection if needed.
        """
        star = self.build_star()

        start = time.perf_counter()
        result = star.execute_query(self.query)
        elapsed = time.perf_counter() - start

        if self.expected_empty:
            assert result.empty, (
                f"[{self.name}] Expected empty result, got {len(result)} rows"
            )
            return result

        if self.expected_columns is not None:
            actual_cols = list(result.columns)
            assert actual_cols == self.expected_columns, (
                f"[{self.name}] Columns mismatch: {actual_cols} != {self.expected_columns}"
            )

        if self.expected_row_count is not None:
            assert len(result) == self.expected_row_count, (
                f"[{self.name}] Row count: {len(result)} != {self.expected_row_count}"
            )

        if self.expected_rows is not None:
            actual_rows = result.values.tolist()
            assert actual_rows == self.expected_rows, (
                f"[{self.name}] Rows mismatch:\n  actual:   {actual_rows}\n  expected: {self.expected_rows}"
            )

        if self.max_duration_seconds is not None:
            assert elapsed <= self.max_duration_seconds, (
                f"[{self.name}] Too slow: {elapsed:.3f}s > {self.max_duration_seconds}s"
            )

        return result


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------


def assert_query_result(
    star: Star,
    query: str,
    *,
    expected_columns: list[str] | None = None,
    expected_rows: list[list[Any]] | None = None,
    expected_row_count: int | None = None,
    expected_values: dict[str, list[Any]] | None = None,
    msg: str = "",
) -> pd.DataFrame:
    """Execute a query and validate results in one call.

    Args:
        star: Star instance to execute against.
        query: Cypher query string.
        expected_columns: Expected column names (order matters).
        expected_rows: Expected row values (order matters).
        expected_row_count: Expected number of rows.
        expected_values: Dict of column -> expected values (unordered).
        msg: Optional message prefix for assertion errors.

    Returns:
        The result DataFrame.
    """
    result = star.execute_query(query)
    prefix = f"[{msg}] " if msg else ""

    if expected_columns is not None:
        assert list(result.columns) == expected_columns, (
            f"{prefix}Columns: {list(result.columns)} != {expected_columns}"
        )

    if expected_row_count is not None:
        assert len(result) == expected_row_count, (
            f"{prefix}Row count: {len(result)} != {expected_row_count}"
        )

    if expected_rows is not None:
        assert result.values.tolist() == expected_rows, (
            f"{prefix}Rows mismatch"
        )

    if expected_values is not None:
        for col, expected in expected_values.items():
            actual = result[col].tolist()
            assert sorted(actual) == sorted(expected), (
                f"{prefix}Column '{col}': {sorted(actual)} != {sorted(expected)}"
            )

    return result


def assert_performance_within(
    star: Star,
    query: str,
    *,
    max_seconds: float,
    warmup_runs: int = 1,
    measured_runs: int = 3,
    msg: str = "",
) -> float:
    """Assert that a query executes within a time budget.

    Runs warmup iterations, then measures the median of measured_runs.

    Args:
        star: Star to execute against.
        query: Cypher query.
        max_seconds: Maximum allowed median execution time.
        warmup_runs: Number of warmup iterations.
        measured_runs: Number of measured iterations.
        msg: Optional message prefix.

    Returns:
        The median execution time in seconds.
    """
    for _ in range(warmup_runs):
        star.execute_query(query)

    times = []
    for _ in range(measured_runs):
        start = time.perf_counter()
        star.execute_query(query)
        times.append(time.perf_counter() - start)

    median_time = sorted(times)[len(times) // 2]
    prefix = f"[{msg}] " if msg else ""
    assert median_time <= max_seconds, (
        f"{prefix}Query too slow: median {median_time:.4f}s > {max_seconds}s "
        f"(times: {[f'{t:.4f}' for t in times]})"
    )
    return median_time


def assert_no_api_breakage() -> tuple[set[str], set[str]]:
    """Check that no public API names have been removed.

    Returns:
        Tuple of (added_names, removed_names). Raises AssertionError
        if any names were removed.
    """
    from pathlib import Path

    baseline_path = Path(__file__).parent.parent / "scripts" / "api_surface_baseline.txt"
    import pycypher

    current = set(pycypher.__all__)

    if not baseline_path.exists():
        return (current, set())

    baseline = {
        line.strip()
        for line in baseline_path.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }

    removed = baseline - current
    added = current - baseline

    assert not removed, (
        f"API breakage detected! Removed: {sorted(removed)}"
    )
    return (added, removed)


# ---------------------------------------------------------------------------
# Scaled data generators for performance testing
# ---------------------------------------------------------------------------


def build_scaled_star(
    n_persons: int = 100,
    avg_degree: int = 5,
    seed: int = 42,
) -> Star:
    """Build a Star with synthetic graph data at a given scale.

    Args:
        n_persons: Number of Person nodes.
        avg_degree: Average edges per node.
        seed: Random seed for reproducibility.

    Returns:
        A Star with Person entities and KNOWS relationships.
    """
    rng = np.random.default_rng(seed)

    depts = ["eng", "mktg", "sales", "ops", "hr"]
    persons_df = pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_persons + 1),
            "name": [f"Person_{i}" for i in range(1, n_persons + 1)],
            "age": rng.integers(18, 65, size=n_persons),
            "dept": rng.choice(depts, size=n_persons),
            "salary": rng.integers(40_000, 200_000, size=n_persons),
        },
    )

    n_edges = n_persons * avg_degree
    sources = rng.integers(1, n_persons + 1, size=n_edges)
    targets = rng.integers(1, n_persons + 1, size=n_edges)
    mask = sources != targets
    sources, targets = sources[mask], targets[mask]
    n_actual = len(sources)
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_actual + 1),
            "__SOURCE__": sources,
            "__TARGET__": targets,
            "since": rng.integers(2000, 2026, size=n_actual),
        },
    )

    person_props = ["name", "age", "dept", "salary"]
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, *person_props],
        source_obj_attribute_map={p: p for p in person_props},
        attribute_map={p: p for p in person_props},
        source_obj=persons_df,
    )

    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__", "since"],
        source_obj_attribute_map={"since": "since"},
        attribute_map={"since": "since"},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )

    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={"KNOWS": knows_table}),
    )
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# Multi-entity graph builder for complex integration tests
# ---------------------------------------------------------------------------


def build_multi_entity_star(
    entity_specs: dict[str, dict[str, list[Any]]],
    relationship_specs: dict[str, dict[str, list[Any]]] | None = None,
) -> Star:
    """Build a Star with multiple entity types and relationships.

    Args:
        entity_specs: {label: {col: [values]}} for each entity type.
        relationship_specs: {rel_type: {col: [values]}} for relationships.
            Must include __ID__, __SOURCE__, __TARGET__ columns.

    Returns:
        A Star ready for query execution.
    """
    entity_tables = {}
    for label, data in entity_specs.items():
        df = pd.DataFrame(data)
        props = [c for c in df.columns if c != ID_COLUMN]
        attr_map = {p: p for p in props}
        entity_tables[label] = EntityTable(
            entity_type=label,
            identifier=label,
            column_names=list(df.columns),
            source_obj_attribute_map=attr_map,
            attribute_map=attr_map,
            source_obj=df,
        )

    rel_tables = {}
    for rt, data in (relationship_specs or {}).items():
        df = pd.DataFrame(data)
        reserved = {ID_COLUMN, "__SOURCE__", "__TARGET__"}
        props = [c for c in df.columns if c not in reserved]
        attr_map = {p: p for p in props}
        # Infer source/target entity types from column naming convention
        rel_tables[rt] = RelationshipTable(
            relationship_type=rt,
            identifier=rt,
            column_names=list(df.columns),
            source_obj_attribute_map=attr_map,
            attribute_map=attr_map,
            source_obj=df,
            source_entity_type="",
            target_entity_type="",
        )

    ctx = Context(
        entity_mapping=EntityMapping(mapping=entity_tables),
        relationship_mapping=RelationshipMapping(mapping=rel_tables),
    )
    return Star(context=ctx)
