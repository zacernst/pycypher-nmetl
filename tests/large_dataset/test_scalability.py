"""Scalability tests for PyCypher query execution.

Validates that query execution scales acceptably with data size,
query complexity, and concurrent usage patterns.
"""

from __future__ import annotations

import gc
import time

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

from .benchmark_utils import run_benchmark
from .dataset_generator import (
    SCALE_SMALL,
    generate_company_dataframe,
    generate_person_dataframe,
    generate_relationship_dataframe,
    generate_social_graph,
)

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


def _build_multi_type_star() -> Star:
    """Build a Star with Person, Company, KNOWS, and WORKS_AT."""
    person_df = generate_person_dataframe(1000, seed=42)
    company_df = generate_company_dataframe(100, seed=43)
    knows_df = generate_relationship_dataframe(5000, 1000, seed=44)
    works_at_df = generate_relationship_dataframe(
        1000,
        1000,
        100,
        seed=45,
        target_id_offset=10_000_001,
    )

    person_table = EntityTable.from_dataframe("Person", person_df)
    company_table = EntityTable.from_dataframe("Company", company_df)

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
    works_at_table = RelationshipTable(
        relationship_type="WORKS_AT",
        identifier="WORKS_AT",
        column_names=[
            ID_COLUMN,
            "__SOURCE__",
            "__TARGET__",
            "since",
            "weight",
        ],
        source_obj_attribute_map={"since": "since", "weight": "weight"},
        attribute_map={"since": "since", "weight": "weight"},
        source_obj=works_at_df,
        source_entity_type="Person",
        target_entity_type="Company",
    )

    return Star(
        context=Context(
            entity_mapping=EntityMapping(
                mapping={
                    "Person": person_table,
                    "Company": company_table,
                },
            ),
            relationship_mapping=RelationshipMapping(
                mapping={
                    "KNOWS": knows_table,
                    "WORKS_AT": works_at_table,
                },
            ),
        ),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def small_star() -> Star:
    person_df, knows_df = generate_social_graph(SCALE_SMALL)
    return _build_star(person_df, knows_df)


@pytest.fixture(scope="module")
def multi_type_star() -> Star:
    return _build_multi_type_star()


# ---------------------------------------------------------------------------
# Query complexity scaling
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestQueryComplexityScaling:
    """Test how execution time scales with query complexity."""

    def test_multiple_where_conditions(self, small_star: Star) -> None:
        """Multiple WHERE conditions should not cause quadratic slowdown."""
        simple_query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name"
        complex_query = (
            "MATCH (p:Person) WHERE p.age > 30 AND p.age < 60 RETURN p.name AS name"
        )

        simple_result = run_benchmark(
            lambda: small_star.execute_query(simple_query),
            iterations=3,
        )
        complex_result = run_benchmark(
            lambda: small_star.execute_query(complex_query),
            iterations=3,
        )

        # Complex query should be < 5x slower than simple
        ratio = complex_result.median_time_s / max(
            simple_result.median_time_s,
            1e-6,
        )
        assert ratio < 5, (
            f"Complex WHERE is {ratio:.1f}x slower than simple (max 5x expected)"
        )


# ---------------------------------------------------------------------------
# Multi-type graph queries
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestMultiTypeGraphScaling:
    """Test queries across multiple entity/relationship types."""

    def test_multi_type_entity_scan(self, multi_type_star: Star) -> None:
        """Scan queries on different entity types should work."""
        person_result = multi_type_star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt",
        )
        company_result = multi_type_star.execute_query(
            "MATCH (c:Company) RETURN count(c) AS cnt",
        )
        assert person_result["cnt"].iloc[0] == 1000
        assert company_result["cnt"].iloc[0] == 100

    def test_multi_type_relationship_traversal(
        self,
        multi_type_star: Star,
    ) -> None:
        """Traversal across different relationship types."""
        knows_result = multi_type_star.execute_query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN count(r) AS cnt",
        )
        works_result = multi_type_star.execute_query(
            "MATCH (p:Person)-[w:WORKS_AT]->(c:Company) RETURN count(w) AS cnt",
        )
        assert knows_result["cnt"].iloc[0] > 0
        assert works_result["cnt"].iloc[0] > 0


# ---------------------------------------------------------------------------
# Sequential query execution (simulating concurrent-like patterns)
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestSequentialQueryExecution:
    """Test sequential query execution patterns."""

    def test_many_sequential_queries(self, small_star: Star) -> None:
        """Execute 50 different queries sequentially."""
        queries = [
            f"MATCH (p:Person) WHERE p.age > {age} RETURN count(p) AS cnt"
            for age in range(20, 70)
        ]

        start = time.perf_counter()
        for query in queries:
            small_star.execute_query(query)
        elapsed = time.perf_counter() - start

        # 50 queries should complete in reasonable time
        per_query = elapsed / len(queries)
        assert per_query < 2.0, (
            f"Per-query time {per_query:.3f}s too slow for sequential "
            f"execution (50 queries in {elapsed:.1f}s)"
        )

    def test_alternating_read_patterns(self, small_star: Star) -> None:
        """Alternate between scan and aggregation queries."""
        scan_query = "MATCH (p:Person) RETURN p.name AS name LIMIT 10"
        agg_query = "MATCH (p:Person) RETURN count(p) AS cnt"

        start = time.perf_counter()
        for _ in range(20):
            small_star.execute_query(scan_query)
            small_star.execute_query(agg_query)
        elapsed = time.perf_counter() - start

        assert elapsed < 30, f"40 alternating queries took {elapsed:.1f}s (max 30s)"


# ---------------------------------------------------------------------------
# Data size scaling validation
# ---------------------------------------------------------------------------


@pytest.mark.performance
class TestDataSizeScaling:
    """Verify performance at increasing data sizes."""

    def test_entity_count_scaling(self) -> None:
        """Test that doubling entities doesn't 4x the query time."""
        query = "MATCH (p:Person) RETURN count(p) AS cnt"
        times = []

        for n in [1000, 2000, 4000]:
            person_df = generate_person_dataframe(n, seed=42)
            knows_df = generate_relationship_dataframe(n * 2, n, seed=43)
            star = _build_star(person_df, knows_df)

            bench = run_benchmark(
                lambda s=star: s.execute_query(query),
                iterations=3,
                warmup=1,
            )
            times.append((n, bench.median_time_s))
            del star, person_df, knows_df
            gc.collect()

        # Check each doubling step
        for i in range(1, len(times)):
            rows_ratio = times[i][0] / times[i - 1][0]
            time_ratio = times[i][1] / max(times[i - 1][1], 1e-6)
            # Should be sub-quadratic: time ratio < rows_ratio^2
            assert time_ratio < rows_ratio**2, (
                f"Super-quadratic scaling at {times[i][0]} rows: "
                f"{rows_ratio:.1f}x rows → {time_ratio:.1f}x time"
            )
