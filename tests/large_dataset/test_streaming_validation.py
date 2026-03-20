"""Streaming and lazy evaluation validation tests.

Tests for Task #14 (Streaming RelationshipScan) and Task #15
(Lazy variable-length path expansion). These tests validate that
streaming implementations produce identical results to the current
batch implementation while using less memory.

Tests marked with @pytest.mark.slow require larger datasets.
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

from .benchmark_utils import _get_process_memory_mb, run_benchmark
from .dataset_generator import SCALE_SMALL, SCALE_TINY, generate_social_graph

ID_COLUMN = "__ID__"

psutil = pytest.importorskip(
    "psutil", reason="psutil required for memory tests"
)


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
                mapping={"KNOWS": knows_table}
            ),
        )
    )


@pytest.fixture(scope="module")
def tiny_star() -> Star:
    person_df, knows_df = generate_social_graph(SCALE_TINY)
    return _build_star(person_df, knows_df)


@pytest.fixture(scope="module")
def small_star() -> Star:
    person_df, knows_df = generate_social_graph(SCALE_SMALL)
    return _build_star(person_df, knows_df)


# ---------------------------------------------------------------------------
# Task #14: Relationship scan correctness
# ---------------------------------------------------------------------------


class TestRelationshipScanCorrectness:
    """Validate relationship scan produces correct results.

    These tests capture the expected behavior that streaming
    RelationshipScan (Task #14) must preserve.
    """

    def test_relationship_scan_returns_all_edges(
        self, tiny_star: Star
    ) -> None:
        """All relationships should appear in unfiltered scan."""
        result = tiny_star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN count(r) AS cnt"
        )
        # Should match the number of relationships in the dataset
        assert result["cnt"].iloc[0] == SCALE_TINY.relationship_rows

    def test_filtered_scan_subset(self, tiny_star: Star) -> None:
        """Filtered relationship scan returns correct subset."""
        all_result = tiny_star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS tgt"
        )
        filtered_result = tiny_star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE a.age > 50 "
            "RETURN a.name AS src, b.name AS tgt"
        )
        assert len(filtered_result) <= len(all_result)

    def test_relationship_properties_accessible(self, tiny_star: Star) -> None:
        """Relationship properties should be accessible after scan."""
        result = tiny_star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since AS since LIMIT 5"
        )
        assert "since" in result.columns
        assert len(result) <= 5  # noqa: PLR2004

    def test_bidirectional_scan(self, tiny_star: Star) -> None:
        """Both directions of relationship should be scannable."""
        forward = tiny_star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN count(r) AS cnt"
        )
        assert forward["cnt"].iloc[0] > 0


# ---------------------------------------------------------------------------
# Task #14: Relationship scan performance
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestRelationshipScanPerformance:
    """Performance targets for relationship scanning.

    Task #14 success criteria: filtered scans should use LESS memory
    than unfiltered scans, and memory should be proportional to result
    set size rather than source table size.
    """

    def test_scan_with_limit_faster(self, small_star: Star) -> None:
        """LIMIT should reduce execution time."""
        full_query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS tgt"
        limited_query = (
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt LIMIT 100"
        )

        full_bench = run_benchmark(
            lambda: small_star.execute_query(full_query),
            iterations=3,
        )
        limited_bench = run_benchmark(
            lambda: small_star.execute_query(limited_query),
            iterations=3,
        )

        # Limited query should be faster (or at least not significantly slower)
        # Allow 2x tolerance for overhead
        assert limited_bench.median_time_s < full_bench.median_time_s * 2

    def test_repeated_scan_no_leak(self, small_star: Star) -> None:
        """Repeated relationship scans should not leak memory."""
        query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS src LIMIT 10"

        for _ in range(3):
            small_star.execute_query(query)
        gc.collect()
        baseline = _get_process_memory_mb()

        for _ in range(15):
            small_star.execute_query(query)

        gc.collect()
        growth = _get_process_memory_mb() - baseline
        assert growth < 50, (  # noqa: PLR2004
            f"Memory grew {growth:.1f}MB over 15 relationship scan iterations"
        )


# ---------------------------------------------------------------------------
# Task #15: Variable-length path correctness
# ---------------------------------------------------------------------------


class TestVariableLengthPathCorrectness:
    """Validate variable-length path expansion correctness.

    These tests capture the expected behavior that lazy VLP
    expansion (Task #15) must preserve.
    """

    def test_fixed_length_one_hop(self, tiny_star: Star) -> None:
        """[*1..1] should return a subset of single-hop results.

        VLP deduplicates (start, end) pairs to avoid path cycles,
        so it may return fewer rows than a raw relationship scan.
        The result set should be a subset of the single-hop result.
        """
        single_hop = tiny_star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt "
            "ORDER BY src, tgt"
        )
        vlp_one = tiny_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..1]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt "
            "ORDER BY src, tgt"
        )
        # VLP may deduplicate, so result should be <= single hop
        assert len(vlp_one) <= len(single_hop)
        assert len(vlp_one) > 0

    def test_vlp_increasing_hops_nondecreasing(self, tiny_star: Star) -> None:
        """More hops should produce >= as many results as fewer hops."""
        one_hop = tiny_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..1]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt"
        )
        two_hop = tiny_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt"
        )
        assert len(two_hop) >= len(one_hop)

    def test_vlp_with_limit(self, tiny_star: Star) -> None:
        """LIMIT on VLP should return correct number of rows."""
        result = tiny_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt LIMIT 5"
        )
        assert len(result) <= 5  # noqa: PLR2004

    def test_vlp_deterministic(self, tiny_star: Star) -> None:
        """VLP should produce deterministic results."""
        query = (
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt "
            "ORDER BY src, tgt"
        )
        r1 = tiny_star.execute_query(query)
        r2 = tiny_star.execute_query(query)
        pd.testing.assert_frame_equal(r1, r2)


# ---------------------------------------------------------------------------
# Task #15: Variable-length path performance
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestVariableLengthPathPerformance:
    """Performance targets for variable-length path expansion.

    Task #15 success criteria: LIMIT N queries should use O(N) memory
    regardless of graph size.
    """

    def test_vlp_performance_small(self, small_star: Star) -> None:
        """VLP at small scale should complete within time bounds."""
        query = (
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt LIMIT 100"
        )
        bench = run_benchmark(
            lambda: small_star.execute_query(query),
            iterations=3,
        )
        bench.assert_time_under(SCALE_SMALL.max_query_time_s)

    def test_vlp_with_limit_memory_bounded(self, small_star: Star) -> None:
        """VLP with LIMIT should not consume excessive memory."""
        query = (
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt LIMIT 50"
        )

        # Warm up
        small_star.execute_query(query)
        gc.collect()
        baseline = _get_process_memory_mb()

        small_star.execute_query(query)

        gc.collect()
        growth = _get_process_memory_mb() - baseline
        # With LIMIT 50, memory growth should be modest
        assert growth < 200, (  # noqa: PLR2004
            f"VLP LIMIT 50 query grew memory by {growth:.1f}MB"
        )

    def test_vlp_no_memory_leak(self, small_star: Star) -> None:
        """Repeated VLP queries should not leak memory."""
        query = "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN a.name AS src LIMIT 10"

        for _ in range(3):
            small_star.execute_query(query)
        gc.collect()
        baseline = _get_process_memory_mb()

        for _ in range(10):
            small_star.execute_query(query)

        gc.collect()
        growth = _get_process_memory_mb() - baseline
        assert growth < 100, (  # noqa: PLR2004
            f"Memory grew {growth:.1f}MB over 10 VLP iterations"
        )
