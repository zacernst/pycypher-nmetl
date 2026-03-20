"""Synthetic dataset generator for large dataset testing.

Generates deterministic, reproducible graph datasets at configurable
scales (1K to 100M+ rows) for systematic performance validation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    pass

ID_COLUMN = "__ID__"
SOURCE_COLUMN = "__SOURCE__"
TARGET_COLUMN = "__TARGET__"


@dataclass(frozen=True)
class DatasetScale:
    """Defines a dataset scale tier with row counts and expected limits."""

    name: str
    person_rows: int
    relationship_rows: int
    max_memory_mb: float
    max_query_time_s: float

    @property
    def total_rows(self) -> int:
        """Total rows across entities and relationships."""
        return self.person_rows + self.relationship_rows


# Standard scale tiers matching task #6 success metrics
SCALE_TINY = DatasetScale(
    name="tiny",
    person_rows=100,
    relationship_rows=200,
    max_memory_mb=100,
    max_query_time_s=1,
)
SCALE_SMALL = DatasetScale(
    name="small",
    person_rows=10_000,
    relationship_rows=50_000,
    max_memory_mb=500,
    max_query_time_s=5,
)
SCALE_MEDIUM = DatasetScale(
    name="medium",
    person_rows=100_000,
    relationship_rows=500_000,
    max_memory_mb=2_000,
    max_query_time_s=30,
)
SCALE_LARGE = DatasetScale(
    name="large",
    person_rows=1_000_000,
    relationship_rows=5_000_000,
    max_memory_mb=8_000,
    max_query_time_s=120,
)
SCALE_XLARGE = DatasetScale(
    name="xlarge",
    person_rows=10_000_000,
    relationship_rows=50_000_000,
    max_memory_mb=16_000,
    max_query_time_s=600,
)

ALL_SCALES = [SCALE_TINY, SCALE_SMALL, SCALE_MEDIUM, SCALE_LARGE, SCALE_XLARGE]


def generate_person_dataframe(
    n_rows: int,
    *,
    seed: int = 42,
    n_properties: int = 5,
) -> pd.DataFrame:
    """Generate a Person entity DataFrame with deterministic data.

    Parameters
    ----------
    n_rows
        Number of person rows to generate.
    seed
        Random seed for reproducibility.
    n_properties
        Number of additional properties beyond name and age.

    Returns
    -------
    pd.DataFrame
        DataFrame with __ID__, name, age, and additional properties.
    """
    rng = np.random.default_rng(seed)

    cities = ["NYC", "LA", "CHI", "HOU", "PHX", "PHI", "SA", "SD", "DAL", "SJ"]
    depts = ["eng", "mktg", "sales", "hr", "ops", "fin", "legal", "support"]

    data: dict[str, object] = {
        ID_COLUMN: np.arange(1, n_rows + 1),
        "name": [f"Person{i}" for i in range(1, n_rows + 1)],
        "age": rng.integers(18, 80, size=n_rows),
        "city": [cities[i % len(cities)] for i in range(n_rows)],
        "dept": [depts[i % len(depts)] for i in range(n_rows)],
    }

    # Add extra properties if requested
    for prop_idx in range(max(0, n_properties - 3)):
        col_name = f"prop_{prop_idx}"
        data[col_name] = rng.standard_normal(n_rows)

    return pd.DataFrame(data)


def generate_company_dataframe(
    n_rows: int,
    *,
    seed: int = 43,
) -> pd.DataFrame:
    """Generate a Company entity DataFrame."""
    rng = np.random.default_rng(seed)
    sectors = ["tech", "fin", "health", "mfg", "retail", "energy"]

    return pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_rows + 1) + 10_000_000,
            "name": [f"Company{i}" for i in range(1, n_rows + 1)],
            "sector": [sectors[i % len(sectors)] for i in range(n_rows)],
            "revenue": rng.integers(1_000_000, 1_000_000_000, size=n_rows),
        }
    )


def generate_relationship_dataframe(
    n_relationships: int,
    n_source_rows: int,
    n_target_rows: int | None = None,
    *,
    seed: int = 44,
    source_id_offset: int = 1,
    target_id_offset: int = 1,
) -> pd.DataFrame:
    """Generate a relationship DataFrame with random connections.

    Parameters
    ----------
    n_relationships
        Number of relationships to create.
    n_source_rows
        Number of available source entities.
    n_target_rows
        Number of available target entities (defaults to n_source_rows).
    seed
        Random seed for reproducibility.
    source_id_offset
        Starting ID for source entities.
    target_id_offset
        Starting ID for target entities.

    Returns
    -------
    pd.DataFrame
        DataFrame with __ID__, __SOURCE__, __TARGET__, and since columns.
    """
    if n_target_rows is None:
        n_target_rows = n_source_rows

    rng = np.random.default_rng(seed)

    sources = rng.integers(
        source_id_offset,
        source_id_offset + n_source_rows,
        size=n_relationships,
    )
    targets = rng.integers(
        target_id_offset,
        target_id_offset + n_target_rows,
        size=n_relationships,
    )

    return pd.DataFrame(
        {
            ID_COLUMN: np.arange(1, n_relationships + 1),
            SOURCE_COLUMN: sources,
            TARGET_COLUMN: targets,
            "since": rng.integers(2000, 2025, size=n_relationships),
            "weight": rng.standard_normal(n_relationships),
        }
    )


def generate_social_graph(
    scale: DatasetScale,
    *,
    seed: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate a complete social graph at the given scale.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (person_df, knows_df) tuple.
    """
    person_df = generate_person_dataframe(scale.person_rows, seed=seed)
    knows_df = generate_relationship_dataframe(
        scale.relationship_rows,
        scale.person_rows,
        seed=seed + 1,
    )
    return person_df, knows_df


def generate_multi_type_graph(
    scale: DatasetScale,
    *,
    seed: int = 42,
) -> dict[str, pd.DataFrame]:
    """Generate a multi-type graph with Person, Company, and relationships.

    Returns
    -------
    dict[str, pd.DataFrame]
        Dictionary with keys: Person, Company, KNOWS, WORKS_AT.
    """
    n_companies = max(1, scale.person_rows // 100)
    n_works_at = scale.person_rows  # Each person works at one company

    person_df = generate_person_dataframe(scale.person_rows, seed=seed)
    company_df = generate_company_dataframe(n_companies, seed=seed + 1)
    knows_df = generate_relationship_dataframe(
        scale.relationship_rows,
        scale.person_rows,
        seed=seed + 2,
    )
    works_at_df = generate_relationship_dataframe(
        n_works_at,
        scale.person_rows,
        n_companies,
        seed=seed + 3,
        target_id_offset=10_000_001,
    )

    return {
        "Person": person_df,
        "Company": company_df,
        "KNOWS": knows_df,
        "WORKS_AT": works_at_df,
    }
