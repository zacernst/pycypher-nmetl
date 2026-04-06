"""Shared utilities for the data scientist showcase scripts.

Provides consistent output formatting, logging setup, and helper functions
used across all demonstration scripts.

This module is NOT part of the public PyCypher API — it exists solely to
support these demo scripts.
"""

from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager
from typing import Generator


def setup_demo(title: str) -> None:
    """Initialize a demo script with clean logging and a header.

    Suppresses internal PyCypher logging for clean demonstration output
    and prints a formatted header.

    Args:
        title: The demo script title to display.
    """
    # Suppress internal logging for clean output
    logging.disable(logging.CRITICAL)

    width = max(len(title) + 4, 60)
    print("=" * width)
    print(f"  {title}")
    print("=" * width)
    print()


def section(heading: str) -> None:
    """Print a section heading."""
    print(f"--- {heading} ---")


def show_result(result, *, label: str = "", max_rows: int = 20) -> None:
    """Display a query result DataFrame with optional label.

    Args:
        result: A pandas DataFrame returned by ``Star.execute_query()``.
        label: Optional label to print above the result.
        max_rows: Maximum rows to display before truncating.
    """
    if label:
        print(f"\n{label}:")
    if len(result) == 0:
        print("  (no results)")
    elif len(result) <= max_rows:
        print(result.to_string(index=False))
    else:
        print(result.head(max_rows).to_string(index=False))
        print(f"  ... and {len(result) - max_rows} more rows")
    print()


def show_count(result, label: str = "Result") -> None:
    """Print the row count of a result DataFrame."""
    print(f"{label}: {len(result)} rows")


@contextmanager
def timed(label: str) -> Generator[None, None, None]:
    """Context manager that prints elapsed wall-clock time.

    Usage::

        with timed("Query execution"):
            result = star.execute_query(...)

    Args:
        label: Description of the timed operation.
    """
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    if elapsed < 1.0:
        print(f"  [{label}: {elapsed * 1000:.1f}ms]")
    else:
        print(f"  [{label}: {elapsed:.2f}s]")


def done() -> None:
    """Print a completion message."""
    print("\nDone!")


def require_import(module_name: str, feature: str) -> bool:
    """Check if an optional module is available.

    Args:
        module_name: The module to check (e.g., ``"duckdb"``).
        feature: Human-readable feature name for the skip message.

    Returns:
        True if the module is available, False otherwise.
    """
    try:
        __import__(module_name)
        return True
    except ImportError:
        print(f"  Skipping {feature} ({module_name} not installed)")
        return False
