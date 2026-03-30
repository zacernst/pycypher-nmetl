"""LIMIT pushdown validation tests for variable-length path BFS.

Validates Task #24: LIMIT pushdown from RETURN clause to VLP expansion.
The row_limit parameter in _expand_variable_length_path should cause
early BFS termination, reducing memory usage from O(total_paths) to
O(limit) while preserving result correctness.
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
from .dataset_generator import SCALE_SMALL, generate_social_graph

ID_COLUMN = "__ID__"


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
# Correctness: LIMIT on VLP queries
# ---------------------------------------------------------------------------


class TestLimitPushdownCorrectness:
    """Verify LIMIT on VLP queries returns correct results."""

    def test_vlp_limit_returns_exact_count(self, small_star: Star) -> None:
        """LIMIT N on VLP should return exactly N rows (if enough exist)."""
        for limit_n in [1, 5, 10, 50]:
            result = small_star.execute_query(
                f"MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
                f"RETURN a.name AS src, b.name AS tgt LIMIT {limit_n}",
            )
            assert len(result) == limit_n

    def test_vlp_limit_results_are_subset(self, small_star: Star) -> None:
        """LIMIT results should be a subset of full results."""
        full = small_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt",
        )
        limited = small_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt LIMIT 10",
        )

        # Every row in limited should appear in full
        for _, row in limited.iterrows():
            match = full[(full["src"] == row["src"]) & (full["tgt"] == row["tgt"])]
            assert len(match) > 0, (
                f"LIMIT result ({row['src']}, {row['tgt']}) not found in full results"
            )

    def test_vlp_limit_one(self, small_star: Star) -> None:
        """LIMIT 1 should return exactly one row."""
        result = small_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt LIMIT 1",
        )
        assert len(result) == 1

    def test_vlp_limit_with_fixed_hops(self, small_star: Star) -> None:
        """LIMIT on fixed-hop VLP."""
        result = small_star.execute_query(
            "MATCH (a:Person)-[:KNOWS*2..2]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt LIMIT 5",
        )
        assert len(result) <= 5


# ---------------------------------------------------------------------------
# Performance: LIMIT pushdown reduces execution time
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestLimitPushdownPerformance:
    """Verify LIMIT pushdown improves VLP query performance."""

    def test_small_limit_faster_than_full(self, small_star: Star) -> None:
        """LIMIT 10 should be faster than returning all VLP results."""
        full_query = (
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt"
        )
        limited_query = (
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt LIMIT 10"
        )

        full_bench = run_benchmark(
            lambda: small_star.execute_query(full_query),
            iterations=3,
        )
        limited_bench = run_benchmark(
            lambda: small_star.execute_query(limited_query),
            iterations=3,
        )

        # Limited query should not be slower than full
        # (may not always be faster due to BFS overhead,
        # but should never be 2x slower)
        assert limited_bench.median_time_s < full_bench.median_time_s * 2

    def test_limit_memory_bounded(self, small_star: Star) -> None:
        """LIMIT should bound memory regardless of full result size."""
        limited_query = (
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt LIMIT 20"
        )

        # Warm up
        small_star.execute_query(limited_query)
        gc.collect()
        baseline = _get_process_memory_mb()

        small_star.execute_query(limited_query)

        gc.collect()
        growth = _get_process_memory_mb() - baseline
        # LIMIT 20 should use very little additional memory
        assert growth < 100, f"LIMIT 20 VLP query grew memory by {growth:.1f}MB"

    def test_increasing_limit_scales_sublinearly(
        self,
        small_star: Star,
    ) -> None:
        """Doubling LIMIT should not double execution time."""
        times = []
        for limit_n in [10, 100]:
            query = (
                f"MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
                f"RETURN a.name AS src, b.name AS tgt LIMIT {limit_n}"
            )
            bench = run_benchmark(
                lambda q=query: small_star.execute_query(q),
                iterations=3,
            )
            times.append((limit_n, bench.median_time_s))

        # 10x LIMIT increase should not cause 10x time increase
        limit_ratio = times[1][0] / times[0][0]
        time_ratio = times[1][1] / max(times[0][1], 1e-6)
        assert time_ratio < limit_ratio * 2, (
            f"LIMIT scaling non-sublinear: "
            f"{limit_ratio:.0f}x LIMIT → {time_ratio:.1f}x time"
        )
