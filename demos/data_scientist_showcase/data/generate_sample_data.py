"""Synthetic data generation for the data scientist showcase demos.

Provides deterministic, reproducible datasets at configurable scales for use
across the demonstration scripts.  All generators use ``numpy.random`` with
fixed seeds so that repeated runs produce identical output.

Datasets
--------
- **Customer–Order–Product** (Script 1 & 6): small, clean e-commerce graph
- **Scalable social graph** (Script 2): 1K → 200K+ rows for backend benchmarks
- **Multi-dataset integration** (Script 4): customers + products + orders + reviews
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# PyCypher column conventions
# ---------------------------------------------------------------------------
ID_COL = "__ID__"
SOURCE_COL = "__SOURCE__"
TARGET_COL = "__TARGET__"


# ---------------------------------------------------------------------------
# Scale tiers for benchmarking (Script 2)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Scale:
    """A named scale tier with row counts for entities and relationships."""

    name: str
    entity_rows: int
    relationship_rows: int


SCALE_SMALL = Scale("small", entity_rows=1_000, relationship_rows=3_000)
SCALE_MEDIUM = Scale("medium", entity_rows=50_000, relationship_rows=150_000)
SCALE_LARGE = Scale("large", entity_rows=200_000, relationship_rows=600_000)

ALL_SCALES = [SCALE_SMALL, SCALE_MEDIUM, SCALE_LARGE]


# ---------------------------------------------------------------------------
# Customer–Order–Product dataset  (Scripts 1, 4, 6)
# ---------------------------------------------------------------------------

def generate_customers(n: int = 50, *, seed: int = 42) -> pd.DataFrame:
    """Generate a customer entity DataFrame.

    Columns: __ID__, name, email, city, signup_date, tier
    """
    rng = np.random.default_rng(seed)

    cities = [
        "Atlanta", "New York", "Chicago", "Houston", "Phoenix",
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
    ]
    tiers = ["bronze", "silver", "gold", "platinum"]
    tier_weights = [0.4, 0.3, 0.2, 0.1]

    # Deterministic signup dates spanning 2022–2025
    base_ts = np.datetime64("2022-01-01")
    day_offsets = rng.integers(0, 3 * 365, size=n)
    signup_dates = [str(base_ts + np.timedelta64(int(d), "D")) for d in day_offsets]

    return pd.DataFrame({
        ID_COL: np.arange(1, n + 1),
        "name": [f"Customer_{i:04d}" for i in range(1, n + 1)],
        "email": [f"customer{i}@example.com" for i in range(1, n + 1)],
        "city": [cities[i % len(cities)] for i in range(n)],
        "signup_date": signup_dates,
        "tier": rng.choice(tiers, size=n, p=tier_weights).tolist(),
    })


def generate_products(n: int = 30, *, seed: int = 43) -> pd.DataFrame:
    """Generate a product entity DataFrame.

    Columns: __ID__, name, category, price, cost
    """
    rng = np.random.default_rng(seed)

    categories = ["Electronics", "Clothing", "Home", "Sports", "Books", "Food"]

    prices = np.round(rng.uniform(5.0, 500.0, size=n), 2)
    # Cost is 40–80% of price
    margins = rng.uniform(0.4, 0.8, size=n)
    costs = np.round(prices * margins, 2)

    return pd.DataFrame({
        ID_COL: np.arange(1_001, 1_001 + n),
        "name": [f"Product_{i:03d}" for i in range(1, n + 1)],
        "category": [categories[i % len(categories)] for i in range(n)],
        "price": prices,
        "cost": costs,
    })


def generate_orders(
    n: int = 200,
    n_customers: int = 50,
    n_products: int = 30,
    *,
    seed: int = 44,
) -> pd.DataFrame:
    """Generate an order relationship DataFrame (Customer → Product).

    Columns: __ID__, __SOURCE__, __TARGET__, quantity, order_date, status
    """
    rng = np.random.default_rng(seed)

    statuses = ["completed", "shipped", "processing", "returned"]
    status_weights = [0.6, 0.2, 0.15, 0.05]

    base_ts = np.datetime64("2024-01-01")
    day_offsets = rng.integers(0, 365, size=n)
    order_dates = [str(base_ts + np.timedelta64(int(d), "D")) for d in day_offsets]

    return pd.DataFrame({
        ID_COL: np.arange(10_001, 10_001 + n),
        SOURCE_COL: rng.integers(1, n_customers + 1, size=n),
        TARGET_COL: rng.integers(1_001, 1_001 + n_products, size=n),
        "quantity": rng.integers(1, 10, size=n),
        "order_date": order_dates,
        "status": rng.choice(statuses, size=n, p=status_weights).tolist(),
    })


def generate_reviews(
    n: int = 120,
    n_customers: int = 50,
    n_products: int = 30,
    *,
    seed: int = 45,
) -> pd.DataFrame:
    """Generate a review relationship DataFrame (Customer → Product).

    Columns: __ID__, __SOURCE__, __TARGET__, rating, review_date
    Used in Script 4 (multi-dataset integration).
    """
    rng = np.random.default_rng(seed)

    # Ratings skew positive (mean ~3.8)
    ratings = np.clip(rng.normal(3.8, 1.0, size=n).round().astype(int), 1, 5)

    base_ts = np.datetime64("2024-01-01")
    day_offsets = rng.integers(0, 365, size=n)
    review_dates = [str(base_ts + np.timedelta64(int(d), "D")) for d in day_offsets]

    return pd.DataFrame({
        ID_COL: np.arange(20_001, 20_001 + n),
        SOURCE_COL: rng.integers(1, n_customers + 1, size=n),
        TARGET_COL: rng.integers(1_001, 1_001 + n_products, size=n),
        "rating": ratings,
        "review_date": review_dates,
    })


def generate_ecommerce_dataset(
    *,
    n_customers: int = 50,
    n_products: int = 30,
    n_orders: int = 200,
    n_reviews: int = 120,
    seed: int = 42,
) -> dict[str, pd.DataFrame]:
    """Generate a complete e-commerce graph dataset.

    Returns a dict suitable for ``ContextBuilder.from_dict()``:
    ``{"Customer": ..., "Product": ..., "ORDERED": ..., "REVIEWED": ...}``
    """
    customers = generate_customers(n_customers, seed=seed)
    products = generate_products(n_products, seed=seed + 1)
    orders = generate_orders(n_orders, n_customers, n_products, seed=seed + 2)
    reviews = generate_reviews(n_reviews, n_customers, n_products, seed=seed + 3)

    return {
        "Customer": customers,
        "Product": products,
        "ORDERED": orders,
        "REVIEWED": reviews,
    }


# ---------------------------------------------------------------------------
# Scalable social graph  (Script 2 – backend performance)
# ---------------------------------------------------------------------------

def generate_persons(n: int, *, seed: int = 42) -> pd.DataFrame:
    """Generate a Person entity DataFrame at arbitrary scale.

    Columns: __ID__, name, age, city, department
    """
    rng = np.random.default_rng(seed)

    cities = [
        "Atlanta", "Boston", "Chicago", "Denver", "El Paso",
        "Fresno", "Grand Rapids", "Hartford", "Indianapolis", "Jacksonville",
    ]
    departments = ["eng", "marketing", "sales", "hr", "ops", "finance"]

    return pd.DataFrame({
        ID_COL: np.arange(1, n + 1),
        "name": [f"Person_{i}" for i in range(1, n + 1)],
        "age": rng.integers(18, 75, size=n),
        "city": [cities[i % len(cities)] for i in range(n)],
        "department": [departments[i % len(departments)] for i in range(n)],
    })


def generate_friendships(
    n: int,
    n_persons: int,
    *,
    seed: int = 44,
) -> pd.DataFrame:
    """Generate a KNOWS relationship DataFrame at arbitrary scale.

    Columns: __ID__, __SOURCE__, __TARGET__, since, strength
    """
    rng = np.random.default_rng(seed)

    return pd.DataFrame({
        ID_COL: np.arange(1, n + 1),
        SOURCE_COL: rng.integers(1, n_persons + 1, size=n),
        TARGET_COL: rng.integers(1, n_persons + 1, size=n),
        "since": rng.integers(2000, 2025, size=n),
        "strength": np.round(rng.uniform(0.1, 1.0, size=n), 3),
    })


def generate_social_graph(scale: Scale, *, seed: int = 42) -> dict[str, pd.DataFrame]:
    """Generate a social graph at the given scale tier.

    Returns a dict suitable for ``ContextBuilder.from_dict()``:
    ``{"Person": ..., "KNOWS": ...}``
    """
    persons = generate_persons(scale.entity_rows, seed=seed)
    friendships = generate_friendships(
        scale.relationship_rows, scale.entity_rows, seed=seed + 2,
    )
    return {"Person": persons, "KNOWS": friendships}
