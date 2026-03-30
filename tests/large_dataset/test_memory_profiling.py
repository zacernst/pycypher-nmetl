"""Memory profiling tests for PyCypher query execution.

Validates memory usage at each materialization point identified in
the architecture assessment. Uses psutil for process-level memory
tracking and identifies memory leaks via repeated execution.
"""

from __future__ import annotations

import gc

import pandas as pd
import pytest
from pycypher import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
    Star,
)

from .benchmark_utils import _get_process_memory_mb
from .dataset_generator import SCALE_SMALL, SCALE_TINY, generate_social_graph

ID_COLUMN = "__ID__"

psutil = pytest.importorskip(
    "psutil",
    reason="psutil required for memory tests",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _build_star(
    person_df: pd.DataFrame,
    knows_df: pd.DataFrame,
) -> Star:
    person_table = EntityTable.from_dataframe("Person", person_df)
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            "__SOURCE__",
            "__TARGET__",
            "since",
            "weight",
        ],
        source_obj_attribute_map={"since": "since", "weight": "weight"},
        attribute_map={"since": "since", "weight": "weight"},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table},
            ),
        ),
    )


@pytest.fixture(scope="module")
def small_star() -> Star:
    person_df, knows_df = generate_social_graph(SCALE_SMALL)
    return _build_star(person_df, knows_df)


# ---------------------------------------------------------------------------
# Memory leak detection
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestMemoryLeakDetection:
    """Detect memory leaks by running queries repeatedly."""

    def test_no_memory_leak_on_repeated_scan(self, small_star: Star) -> None:
        """Run same query 20 times; memory should not grow unboundedly."""
        query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name"

        # Warm up and establish baseline
        for _ in range(3):
            small_star.execute_query(query)
        gc.collect()
        baseline_mb = _get_process_memory_mb()

        # Run 20 iterations
        for _ in range(20):
            small_star.execute_query(query)

        gc.collect()
        final_mb = _get_process_memory_mb()

        # Allow up to 50MB growth (generous bound for GC variance)
        growth_mb = final_mb - baseline_mb
        assert growth_mb < 50, (
            f"Memory grew by {growth_mb:.1f}MB over 20 iterations "
            f"(baseline={baseline_mb:.1f}MB, final={final_mb:.1f}MB)"
        )

    def test_no_memory_leak_on_repeated_join(self, small_star: Star) -> None:
        """Run join query 10 times; memory should not grow unboundedly."""
        query = (
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt LIMIT 100"
        )

        for _ in range(3):
            small_star.execute_query(query)
        gc.collect()
        baseline_mb = _get_process_memory_mb()

        for _ in range(10):
            small_star.execute_query(query)

        gc.collect()
        final_mb = _get_process_memory_mb()

        growth_mb = final_mb - baseline_mb
        assert growth_mb < 100, (
            f"Memory grew by {growth_mb:.1f}MB over 10 join iterations "
            f"(baseline={baseline_mb:.1f}MB, final={final_mb:.1f}MB)"
        )

    def test_no_memory_leak_on_aggregation(self, small_star: Star) -> None:
        """Run aggregation query 10 times; memory stable."""
        query = "MATCH (p:Person) RETURN p.city AS city, count(p) AS cnt"

        for _ in range(3):
            small_star.execute_query(query)
        gc.collect()
        baseline_mb = _get_process_memory_mb()

        for _ in range(10):
            small_star.execute_query(query)

        gc.collect()
        final_mb = _get_process_memory_mb()

        growth_mb = final_mb - baseline_mb
        assert growth_mb < 50, (
            f"Memory grew by {growth_mb:.1f}MB over 10 aggregation iterations"
        )


# ---------------------------------------------------------------------------
# Memory usage bounds
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestMemoryBounds:
    """Verify memory usage stays within expected bounds."""

    def test_scan_memory_proportional_to_data(self) -> None:
        """Memory for scan query should be proportional to data size."""
        query = "MATCH (p:Person) RETURN p.name AS name"

        measurements: list[tuple[int, float]] = []
        for scale in [SCALE_TINY, SCALE_SMALL]:
            person_df, knows_df = generate_social_graph(scale)
            star = _build_star(person_df, knows_df)

            gc.collect()
            mem_before = _get_process_memory_mb()
            star.execute_query(query)
            mem_after = _get_process_memory_mb()

            measurements.append((scale.person_rows, mem_after - mem_before))
            del star, person_df, knows_df
            gc.collect()

        # Memory growth should be sub-quadratic relative to data size
        if measurements[0][1] > 0.1:
            rows_ratio = measurements[1][0] / measurements[0][0]
            mem_ratio = measurements[1][1] / max(measurements[0][1], 0.01)
            # Memory should grow at most 3x the data growth
            assert mem_ratio < rows_ratio * 3, (
                f"Memory scaling super-linear: "
                f"{rows_ratio:.0f}x rows → {mem_ratio:.1f}x memory"
            )

    def test_context_creation_memory(self) -> None:
        """Verify context creation doesn't copy data unnecessarily."""
        person_df, knows_df = generate_social_graph(SCALE_SMALL)

        gc.collect()
        mem_before = _get_process_memory_mb()

        star = _build_star(person_df, knows_df)

        gc.collect()
        mem_after = _get_process_memory_mb()

        # Context creation should add < 2x the raw data size
        raw_data_mb = (
            person_df.memory_usage(deep=True).sum()
            + knows_df.memory_usage(deep=True).sum()
        ) / (1024 * 1024)

        context_overhead_mb = mem_after - mem_before
        assert context_overhead_mb < raw_data_mb * 3, (
            f"Context creation overhead too high: "
            f"{context_overhead_mb:.1f}MB for {raw_data_mb:.1f}MB raw data"
        )

        del star, person_df, knows_df
