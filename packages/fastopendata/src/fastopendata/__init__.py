"""FastOpenData — open data ingestion and processing for PyCypher.

This package provides utilities for loading, transforming, and ingesting
public open datasets into pycypher-compatible graph structures.
"""

__version__ = "0.0.1"

from fastopendata.config import Config, config
from fastopendata.pipeline import GraphPipeline

__all__: list[str] = [
    "Config",
    "GraphPipeline",
    "config",
]
