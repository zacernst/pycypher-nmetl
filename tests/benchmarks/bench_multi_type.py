"""Benchmark: Multi-entity-type graph queries.

Tests performance of queries involving multiple entity types (Person,
Company, Location) and relationship types (KNOWS, WORKS_AT, LIVES_IN)
to ensure cross-type join performance is tracked.

Run via pytest-benchmark::

    uv run pytest tests/benchmarks/bench_multi_type.py -v --benchmark-only

Or directly::

    uv run python tests/benchmarks/bench_multi_type.py
"""

from __future__ import annotations

import time
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
# Fixtures: multi-type graphs at different scales
# ---------------------------------------------------------------------------


def _build_multi_type_context(
    n_persons: int = 1_000,
    n_companies: int = 100,
    n_locations: int = 50,
) -> Context:
    """Build a multi-type graph context."""
    rng = np.random.default_rng(42)
    depts = ["eng", "mktg", "sales", "ops", "hr"]
    industries = ["tech", "finance", "healthcare", "retail", "manufacturing"]
    countries = ["US", "UK", "DE", "FR", "JP", "AU", "CA", "BR", "IN", "CN"]

    # Persons
    persons_df = pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_persons + 1),
            "name": [f"Person_{i}" for i in range(1, n_persons + 1)],
            "age": rng.integers(18, 80, size=n_persons),
            "dept": rng.choice(depts, size=n_persons),
            "salary": rng.integers(30_000, 300_000, size=n_persons),
        },
    )

    # Companies
    companies_df = pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_companies + 1),
            "name": [f"Company_{i}" for i in range(1, n_companies + 1)],
            "industry": rng.choice(industries, size=n_companies),
            "employees": rng.integers(10, 100_000, size=n_companies),
        },
    )

    # Locations
    locations_df = pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_locations + 1),
            "city": [f"City_{i}" for i in range(1, n_locations + 1)],
            "country": rng.choice(countries, size=n_locations),
            "population": rng.integers(1_000, 10_000_000, size=n_locations),
        },
    )

    # KNOWS relationships (Person→Person)
    n_knows = n_persons * 5
    ks = rng.integers(1, n_persons + 1, size=n_knows)
    kt = rng.integers(1, n_persons + 1, size=n_knows)
    mask = ks != kt
    ks, kt = ks[mask], kt[mask]
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, len(ks) + 1),
            "__SOURCE__": ks,
            "__TARGET__": kt,
            "since": rng.integers(2000, 2026, size=len(ks)),
        },
    )

    # WORKS_AT relationships (Person→Company)
    n_workers = int(n_persons * 0.9)
    wa_persons = rng.choice(
        np.arange(1, n_persons + 1), size=n_workers, replace=False
    )
    wa_companies = rng.integers(1, n_companies + 1, size=n_workers)
    roles = ["engineer", "manager", "analyst", "director", "intern"]
    works_at_df = pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_workers + 1),
            "__SOURCE__": wa_persons,
            "__TARGET__": wa_companies,
            "role": rng.choice(roles, size=n_workers),
        },
    )

    # LIVES_IN relationships (Person→Location)
    n_residents = int(n_persons * 0.95)
    li_persons = rng.choice(
        np.arange(1, n_persons + 1), size=n_residents, replace=False
    )
    li_locations = rng.integers(1, n_locations + 1, size=n_residents)
    lives_in_df = pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_residents + 1),
            "__SOURCE__": li_persons,
            "__TARGET__": li_locations,
        },
    )

    def _entity(name: str, df: pd.DataFrame) -> EntityTable:
        attrs = {c: c for c in df.columns if c != ID_COLUMN}
        return EntityTable(
            entity_type=name,
            identifier=name,
            column_names=list(df.columns),
            source_obj_attribute_map=attrs,
            attribute_map=attrs,
            source_obj=df,
        )

    def _rel(
        name: str, df: pd.DataFrame, src: str, tgt: str
    ) -> RelationshipTable:
        reserved = {ID_COLUMN, "__SOURCE__", "__TARGET__"}
        attrs = {c: c for c in df.columns if c not in reserved}
        return RelationshipTable(
            relationship_type=name,
            identifier=name,
            column_names=list(df.columns),
            source_obj_attribute_map=attrs,
            attribute_map=attrs,
            source_obj=df,
            source_entity_type=src,
            target_entity_type=tgt,
        )

    return Context(
        entity_mapping=EntityMapping(
            mapping={
                "Person": _entity("Person", persons_df),
                "Company": _entity("Company", companies_df),
                "Location": _entity("Location", locations_df),
            },
        ),
        relationship_mapping=RelationshipMapping(
            mapping={
                "KNOWS": _rel("KNOWS", knows_df, "Person", "Person"),
                "WORKS_AT": _rel("WORKS_AT", works_at_df, "Person", "Company"),
                "LIVES_IN": _rel(
                    "LIVES_IN", lives_in_df, "Person", "Location"
                ),
            },
        ),
    )


@pytest.fixture(scope="module")
def star_tiny() -> Star:
    """Multi-type graph: 1K persons, 100 companies, 50 locations."""
    ctx = _build_multi_type_context()
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# Group 1: Cross-type relationship traversals
# ---------------------------------------------------------------------------


class TestCrossTypeTraversals:
    """Benchmark queries that traverse across entity types."""

    def test_person_works_at_company(
        self,
        benchmark: Any,
        star_tiny: Star,
    ) -> None:
        """Person→Company via WORKS_AT."""
        result = benchmark(
            star_tiny.execute_query,
            "MATCH (p:Person)-[w:WORKS_AT]->(c:Company) "
            "RETURN p.name, c.name, w.role",
        )
        assert len(result) > 0

    def test_person_lives_in_location(
        self,
        benchmark: Any,
        star_tiny: Star,
    ) -> None:
        """Person→Location via LIVES_IN."""
        result = benchmark(
            star_tiny.execute_query,
            "MATCH (p:Person)-[:LIVES_IN]->(l:Location) "
            "RETURN p.name, l.city, l.country",
        )
        assert len(result) > 0

    def test_person_knows_person(
        self,
        benchmark: Any,
        star_tiny: Star,
    ) -> None:
        """Person→Person via KNOWS in multi-type context."""
        result = benchmark(
            star_tiny.execute_query,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name, b.name, r.since",
        )
        assert len(result) > 0

    def test_filtered_works_at(
        self,
        benchmark: Any,
        star_tiny: Star,
    ) -> None:
        """Filtered cross-type: engineers at tech companies."""
        result = benchmark(
            star_tiny.execute_query,
            "MATCH (p:Person)-[w:WORKS_AT]->(c:Company) "
            "WHERE w.role = 'engineer' AND c.industry = 'tech' "
            "RETURN p.name, c.name",
        )
        assert len(result) >= 0


# ---------------------------------------------------------------------------
# Group 2: Cross-type aggregations
# ---------------------------------------------------------------------------


class TestCrossTypeAggregations:
    """Benchmark aggregation queries across entity types."""

    def test_employees_per_company(
        self,
        benchmark: Any,
        star_tiny: Star,
    ) -> None:
        """Count employees per company."""
        result = benchmark(
            star_tiny.execute_query,
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
            "RETURN c.name, count(p) AS employee_count",
        )
        assert len(result) > 0

    def test_avg_salary_by_industry(
        self,
        benchmark: Any,
        star_tiny: Star,
    ) -> None:
        """Average salary by company industry."""
        result = benchmark(
            star_tiny.execute_query,
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
            "RETURN c.industry, avg(p.salary) AS avg_sal",
        )
        assert len(result) > 0

    def test_residents_per_country(
        self,
        benchmark: Any,
        star_tiny: Star,
    ) -> None:
        """Count residents per country."""
        result = benchmark(
            star_tiny.execute_query,
            "MATCH (p:Person)-[:LIVES_IN]->(l:Location) "
            "RETURN l.country, count(p) AS residents",
        )
        assert len(result) > 0


# ---------------------------------------------------------------------------
# Group 3: Multi-type filtered scans
# ---------------------------------------------------------------------------


class TestMultiTypeFilteredScans:
    """Benchmark filtered entity scans across different types."""

    def test_company_by_industry(
        self,
        benchmark: Any,
        star_tiny: Star,
    ) -> None:
        """Scan companies filtered by industry."""
        result = benchmark(
            star_tiny.execute_query,
            "MATCH (c:Company) WHERE c.industry = 'tech' RETURN c.name, c.employees",
        )
        assert len(result) >= 0

    def test_location_by_country(
        self,
        benchmark: Any,
        star_tiny: Star,
    ) -> None:
        """Scan locations filtered by country."""
        result = benchmark(
            star_tiny.execute_query,
            "MATCH (l:Location) WHERE l.country = 'US' RETURN l.city, l.population",
        )
        assert len(result) >= 0

    def test_person_high_salary(
        self,
        benchmark: Any,
        star_tiny: Star,
    ) -> None:
        """Scan persons with high salary filter."""
        result = benchmark(
            star_tiny.execute_query,
            "MATCH (p:Person) WHERE p.salary > 150000 RETURN p.name, p.salary",
        )
        assert len(result) >= 0


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run multi-type benchmarks from command line."""
    ctx = _build_multi_type_context()
    star = Star(context=ctx)

    queries: dict[str, str] = {
        "person_works_at_company": (
            "MATCH (p:Person)-[w:WORKS_AT]->(c:Company) "
            "RETURN p.name, c.name, w.role"
        ),
        "person_lives_in_location": (
            "MATCH (p:Person)-[:LIVES_IN]->(l:Location) "
            "RETURN p.name, l.city, l.country"
        ),
        "person_knows_person": (
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name, b.name, r.since"
        ),
        "filtered_works_at": (
            "MATCH (p:Person)-[w:WORKS_AT]->(c:Company) "
            "WHERE w.role = 'engineer' AND c.industry = 'tech' "
            "RETURN p.name, c.name"
        ),
        "employees_per_company": (
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
            "RETURN c.name, count(p) AS cnt"
        ),
        "avg_salary_by_industry": (
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
            "RETURN c.industry, avg(p.salary) AS avg_sal"
        ),
        "residents_per_country": (
            "MATCH (p:Person)-[:LIVES_IN]->(l:Location) "
            "RETURN l.country, count(p) AS residents"
        ),
    }

    print("=" * 70)
    print("Multi-Type Graph Benchmark Suite")
    print("  1K persons, 100 companies, 50 locations")
    print("=" * 70)

    for qname, qtext in queries.items():
        timings: list[float] = []
        result_rows = 0
        for _ in range(10):
            t0 = time.perf_counter()
            result = star.execute_query(qtext)
            timings.append(time.perf_counter() - t0)
            result_rows = len(result)

        median_ms = float(np.median(timings)) * 1e3
        print(
            f"  {qname:35s}  "
            f"median={median_ms:>8.2f}ms  "
            f"rows={result_rows:>8,}",
        )


if __name__ == "__main__":
    main()
