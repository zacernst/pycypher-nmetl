"""Synthetic graph dataset generator for PyCypher benchmarking.

Generates multi-type graph data at configurable scales with reproducible
random seeding.  Supports CSV, Parquet, and Arrow output formats.

Usage::

    # Generate a small dataset to Parquet
    uv run python tests/benchmarks/generate_synthetic_graph.py --profile small --format parquet --output /tmp/graph

    # Generate a custom dataset
    uv run python tests/benchmarks/generate_synthetic_graph.py --nodes 50000 --density medium --format csv --output /tmp/graph

    # Load into PyCypher context directly (library use)
    from tests.benchmarks.generate_synthetic_graph import generate_graph, load_into_context
    graph = generate_graph(profile="small")
    ctx = load_into_context(graph)
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Scale profiles
# ---------------------------------------------------------------------------

PROFILES: dict[str, dict[str, Any]] = {
    "tiny": {
        "description": "~1 MB — unit test / smoke test",
        "persons": 1_000,
        "companies": 100,
        "locations": 50,
        "knows_per_person": 5,
        "works_at_fraction": 0.9,
        "lives_in_fraction": 0.95,
    },
    "small": {
        "description": "~100 MB — development benchmarking",
        "persons": 100_000,
        "companies": 5_000,
        "locations": 1_000,
        "knows_per_person": 5,
        "works_at_fraction": 0.9,
        "lives_in_fraction": 0.95,
    },
    "medium": {
        "description": "~1 GB — integration benchmarking",
        "persons": 1_000_000,
        "companies": 50_000,
        "locations": 10_000,
        "knows_per_person": 5,
        "works_at_fraction": 0.9,
        "lives_in_fraction": 0.95,
    },
    "large": {
        "description": "~10 GB — scalability testing",
        "persons": 10_000_000,
        "companies": 500_000,
        "locations": 100_000,
        "knows_per_person": 5,
        "works_at_fraction": 0.9,
        "lives_in_fraction": 0.95,
    },
    "xlarge": {
        "description": "~100 GB — stress testing",
        "persons": 100_000_000,
        "companies": 5_000_000,
        "locations": 1_000_000,
        "knows_per_person": 5,
        "works_at_fraction": 0.9,
        "lives_in_fraction": 0.95,
    },
}

DENSITY_MULTIPLIERS: dict[str, int] = {
    "sparse": 2,
    "medium": 10,
    "dense": 50,
}

# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

FIRST_NAMES: list[str] = [
    "Alice",
    "Bob",
    "Carol",
    "Dave",
    "Eve",
    "Frank",
    "Grace",
    "Heidi",
    "Ivan",
    "Judy",
    "Karl",
    "Linda",
    "Mike",
    "Nancy",
    "Oscar",
    "Peggy",
    "Quinn",
    "Rita",
    "Steve",
    "Tina",
    "Uma",
    "Victor",
    "Wendy",
    "Xander",
    "Yolanda",
    "Zach",
]
LAST_NAMES: list[str] = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
]
DEPARTMENTS: list[str] = [
    "eng",
    "mktg",
    "sales",
    "ops",
    "hr",
    "finance",
    "legal",
    "support",
]
INDUSTRIES: list[str] = [
    "tech",
    "finance",
    "healthcare",
    "retail",
    "manufacturing",
    "energy",
]
COUNTRIES: list[str] = [
    "US",
    "UK",
    "DE",
    "FR",
    "JP",
    "AU",
    "CA",
    "BR",
    "IN",
    "CN",
]


@dataclass
class SyntheticGraph:
    """Container for all generated entity and relationship DataFrames."""

    persons: pd.DataFrame = field(default_factory=pd.DataFrame)
    companies: pd.DataFrame = field(default_factory=pd.DataFrame)
    locations: pd.DataFrame = field(default_factory=pd.DataFrame)
    knows: pd.DataFrame = field(default_factory=pd.DataFrame)
    works_at: pd.DataFrame = field(default_factory=pd.DataFrame)
    lives_in: pd.DataFrame = field(default_factory=pd.DataFrame)
    metadata: dict[str, Any] = field(default_factory=dict)


def _generate_persons(n: int, *, rng: np.random.Generator) -> pd.DataFrame:
    """Generate Person entity data."""
    first = rng.choice(FIRST_NAMES, size=n)
    last = rng.choice(LAST_NAMES, size=n)
    names = np.array([f"{f} {l}" for f, l in zip(first, last)])
    return pd.DataFrame(
        {
            "__ID__": np.arange(1, n + 1),
            "name": names,
            "age": rng.integers(18, 80, size=n),
            "dept": rng.choice(DEPARTMENTS, size=n),
            "salary": rng.integers(30_000, 300_000, size=n),
            "active": rng.choice([True, False], size=n, p=[0.85, 0.15]),
        },
    )


def _generate_companies(n: int, *, rng: np.random.Generator) -> pd.DataFrame:
    """Generate Company entity data."""
    prefixes = [
        "Acme",
        "Global",
        "Tech",
        "Prime",
        "Alpha",
        "Nova",
        "Peak",
        "Core",
    ]
    suffixes = ["Corp", "Inc", "LLC", "Systems", "Labs", "Solutions", "Group"]
    names = [
        f"{rng.choice(prefixes)} {rng.choice(suffixes)} {i}"
        for i in range(1, n + 1)
    ]
    return pd.DataFrame(
        {
            "__ID__": np.arange(1, n + 1),
            "name": names,
            "industry": rng.choice(INDUSTRIES, size=n),
            "employees": rng.integers(10, 100_000, size=n),
            "founded": rng.integers(1950, 2025, size=n),
        },
    )


def _generate_locations(n: int, *, rng: np.random.Generator) -> pd.DataFrame:
    """Generate Location entity data."""
    return pd.DataFrame(
        {
            "__ID__": np.arange(1, n + 1),
            "city": [f"City_{i}" for i in range(1, n + 1)],
            "country": rng.choice(COUNTRIES, size=n),
            "latitude": rng.uniform(-90, 90, size=n).round(4),
            "longitude": rng.uniform(-180, 180, size=n).round(4),
            "population": rng.integers(1_000, 10_000_000, size=n),
        },
    )


def _generate_knows(
    n_persons: int,
    edges_per_person: int,
    *,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate KNOWS relationships (Person→Person)."""
    n_edges = n_persons * edges_per_person
    sources = rng.integers(1, n_persons + 1, size=n_edges)
    targets = rng.integers(1, n_persons + 1, size=n_edges)
    mask = sources != targets
    sources = sources[mask]
    targets = targets[mask]
    # Deduplicate (source, target) pairs
    pairs = np.column_stack([sources, targets])
    _, idx = np.unique(pairs, axis=0, return_index=True)
    pairs = pairs[np.sort(idx)]
    n_actual = len(pairs)
    return pd.DataFrame(
        {
            "__ID__": np.arange(1, n_actual + 1),
            "__SOURCE__": pairs[:, 0],
            "__TARGET__": pairs[:, 1],
            "since": rng.integers(2000, 2026, size=n_actual),
            "strength": rng.uniform(0.0, 1.0, size=n_actual).round(3),
        },
    )


def _generate_works_at(
    n_persons: int,
    n_companies: int,
    fraction: float,
    *,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate WORKS_AT relationships (Person→Company)."""
    n_workers = int(n_persons * fraction)
    person_ids = rng.choice(
        np.arange(1, n_persons + 1),
        size=n_workers,
        replace=False,
    )
    company_ids = rng.integers(1, n_companies + 1, size=n_workers)
    return pd.DataFrame(
        {
            "__ID__": np.arange(1, n_workers + 1),
            "__SOURCE__": person_ids,
            "__TARGET__": company_ids,
            "role": rng.choice(
                ["engineer", "manager", "analyst", "director", "intern"],
                size=n_workers,
            ),
            "start_year": rng.integers(2010, 2026, size=n_workers),
        },
    )


def _generate_lives_in(
    n_persons: int,
    n_locations: int,
    fraction: float,
    *,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate LIVES_IN relationships (Person→Location)."""
    n_residents = int(n_persons * fraction)
    person_ids = rng.choice(
        np.arange(1, n_persons + 1),
        size=n_residents,
        replace=False,
    )
    location_ids = rng.integers(1, n_locations + 1, size=n_residents)
    return pd.DataFrame(
        {
            "__ID__": np.arange(1, n_residents + 1),
            "__SOURCE__": person_ids,
            "__TARGET__": location_ids,
            "since_year": rng.integers(1990, 2026, size=n_residents),
        },
    )


def generate_graph(
    *,
    profile: str | None = None,
    persons: int | None = None,
    companies: int | None = None,
    locations: int | None = None,
    density: str = "medium",
    seed: int = 42,
) -> SyntheticGraph:
    """Generate a complete synthetic graph dataset.

    Args:
        profile: Named scale profile (tiny/small/medium/large/xlarge).
        persons: Override number of Person entities.
        companies: Override number of Company entities.
        locations: Override number of Location entities.
        density: Relationship density (sparse/medium/dense).
        seed: Random seed for reproducibility.

    Returns:
        SyntheticGraph with all entity and relationship DataFrames.

    """
    rng = np.random.default_rng(seed)

    # Resolve configuration
    if profile and profile in PROFILES:
        cfg = PROFILES[profile].copy()
    else:
        cfg = PROFILES["tiny"].copy()

    if persons is not None:
        cfg["persons"] = persons
    if companies is not None:
        cfg["companies"] = companies
    if locations is not None:
        cfg["locations"] = locations

    # Apply density multiplier to knows_per_person
    if density in DENSITY_MULTIPLIERS:
        cfg["knows_per_person"] = DENSITY_MULTIPLIERS[density]

    n_persons = cfg["persons"]
    n_companies = cfg["companies"]
    n_locations = cfg["locations"]

    t0 = time.perf_counter()

    graph = SyntheticGraph(
        persons=_generate_persons(n_persons, rng=rng),
        companies=_generate_companies(n_companies, rng=rng),
        locations=_generate_locations(n_locations, rng=rng),
        knows=_generate_knows(n_persons, cfg["knows_per_person"], rng=rng),
        works_at=_generate_works_at(
            n_persons,
            n_companies,
            cfg["works_at_fraction"],
            rng=rng,
        ),
        lives_in=_generate_lives_in(
            n_persons,
            n_locations,
            cfg["lives_in_fraction"],
            rng=rng,
        ),
        metadata={
            "profile": profile,
            "config": cfg,
            "seed": seed,
            "density": density,
            "generation_seconds": 0.0,
        },
    )

    elapsed = time.perf_counter() - t0
    graph.metadata["generation_seconds"] = round(elapsed, 3)

    return graph


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------


def _to_arrow_table(df: pd.DataFrame) -> pa.Table:
    """Convert pandas DataFrame to Arrow table."""
    return pa.Table.from_pandas(df, preserve_index=False)


def write_graph(
    graph: SyntheticGraph,
    output_dir: Path,
    *,
    fmt: str = "parquet",
) -> None:
    """Write all entity and relationship DataFrames to disk.

    Args:
        graph: The generated graph data.
        output_dir: Directory to write files into.
        fmt: Output format — "parquet", "csv", or "arrow".

    """
    output_dir.mkdir(parents=True, exist_ok=True)

    datasets: dict[str, pd.DataFrame] = {
        "persons": graph.persons,
        "companies": graph.companies,
        "locations": graph.locations,
        "knows": graph.knows,
        "works_at": graph.works_at,
        "lives_in": graph.lives_in,
    }

    for name, df in datasets.items():
        table = _to_arrow_table(df)
        if fmt == "parquet":
            pq.write_table(table, output_dir / f"{name}.parquet")
        elif fmt == "csv":
            pa_csv.write_csv(table, output_dir / f"{name}.csv")
        elif fmt == "arrow":
            with pa.OSFile(str(output_dir / f"{name}.arrow"), "wb") as f:
                writer = pa.ipc.new_file(f, table.schema)
                writer.write_table(table)
                writer.close()
        else:
            msg = f"Unsupported format: {fmt!r}. Use 'parquet', 'csv', or 'arrow'."
            raise ValueError(msg)

    print(f"Wrote {len(datasets)} files to {output_dir} ({fmt})")
    for name, df in datasets.items():
        size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        print(f"  {name}: {len(df):>12,} rows  ({size_mb:.1f} MB in memory)")


# ---------------------------------------------------------------------------
# PyCypher Context integration
# ---------------------------------------------------------------------------


def load_into_context(graph: SyntheticGraph) -> Any:
    """Load a SyntheticGraph into a PyCypher Context.

    Returns:
        A ``pycypher.relational_models.Context`` ready for query execution.

    """
    from pycypher.relational_models import (
        Context,
        EntityMapping,
        EntityTable,
        RelationshipMapping,
        RelationshipTable,
    )

    def _make_entity(name: str, df: pd.DataFrame) -> EntityTable:
        attrs = {c: c for c in df.columns if c != "__ID__"}
        return EntityTable(
            entity_type=name,
            identifier=name,
            column_names=list(df.columns),
            source_obj_attribute_map=attrs,
            attribute_map=attrs,
            source_obj=df,
        )

    def _make_rel(
        name: str,
        df: pd.DataFrame,
        src_type: str,
        tgt_type: str,
    ) -> RelationshipTable:
        reserved = {"__ID__", "__SOURCE__", "__TARGET__"}
        attrs = {c: c for c in df.columns if c not in reserved}
        return RelationshipTable(
            relationship_type=name,
            identifier=name,
            column_names=list(df.columns),
            source_obj_attribute_map=attrs,
            attribute_map=attrs,
            source_obj=df,
            source_entity_type=src_type,
            target_entity_type=tgt_type,
        )

    entity_mapping = EntityMapping(
        mapping={
            "Person": _make_entity("Person", graph.persons),
            "Company": _make_entity("Company", graph.companies),
            "Location": _make_entity("Location", graph.locations),
        },
    )

    relationship_mapping = RelationshipMapping(
        mapping={
            "KNOWS": _make_rel("KNOWS", graph.knows, "Person", "Person"),
            "WORKS_AT": _make_rel(
                "WORKS_AT",
                graph.works_at,
                "Person",
                "Company",
            ),
            "LIVES_IN": _make_rel(
                "LIVES_IN",
                graph.lives_in,
                "Person",
                "Location",
            ),
        },
    )

    return Context(
        entity_mapping=entity_mapping,
        relationship_mapping=relationship_mapping,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for graph generation."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic graph datasets for PyCypher benchmarking",
    )
    parser.add_argument(
        "--profile",
        choices=list(PROFILES.keys()),
        default=None,
        help="Named scale profile",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=None,
        help="Override Person count",
    )
    parser.add_argument(
        "--density",
        choices=list(DENSITY_MULTIPLIERS.keys()),
        default="medium",
        help="Relationship density",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--format",
        choices=["parquet", "csv", "arrow"],
        default="parquet",
        help="Output format",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output directory",
    )

    args = parser.parse_args()

    profile = args.profile or "tiny"
    print(
        f"Generating '{profile}' graph (density={args.density}, seed={args.seed})...",
    )

    graph = generate_graph(
        profile=profile,
        persons=args.nodes,
        density=args.density,
        seed=args.seed,
    )

    print(f"Generated in {graph.metadata['generation_seconds']:.1f}s")
    write_graph(graph, Path(args.output), fmt=args.format)


if __name__ == "__main__":
    main()
