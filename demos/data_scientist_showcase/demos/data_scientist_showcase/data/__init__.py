"""Data generation and loading utilities for the showcase demos.

Quick usage::

    from data.generate_sample_data import generate_ecommerce_dataset
    from data.generate_sample_data import generate_social_graph, SCALE_MEDIUM
    from data.load_georgia_contracts import load_georgia_contracts, contracts_as_graph
"""

from data.generate_sample_data import (
    ALL_SCALES,
    SCALE_LARGE,
    SCALE_MEDIUM,
    SCALE_SMALL,
    Scale,
    generate_customers,
    generate_ecommerce_dataset,
    generate_orders,
    generate_persons,
    generate_products,
    generate_reviews,
    generate_social_graph,
)
from data.load_georgia_contracts import (
    DEMO_COLUMNS,
    contracts_as_graph,
    load_georgia_contracts,
    load_georgia_sample,
)

__all__ = [
    # Synthetic data
    "generate_customers",
    "generate_products",
    "generate_orders",
    "generate_reviews",
    "generate_ecommerce_dataset",
    "generate_persons",
    "generate_social_graph",
    # Scale tiers
    "Scale",
    "SCALE_SMALL",
    "SCALE_MEDIUM",
    "SCALE_LARGE",
    "ALL_SCALES",
    # Georgia contract data
    "load_georgia_contracts",
    "load_georgia_sample",
    "contracts_as_graph",
    "DEMO_COLUMNS",
]
