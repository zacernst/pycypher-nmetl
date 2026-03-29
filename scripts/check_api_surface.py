#!/usr/bin/env python3
"""check_api_surface.py — Detect breaking changes in the public API.

Compares the current ``pycypher.__all__`` against a frozen baseline stored
in ``scripts/api_surface_baseline.txt``.  Any *removed* name is flagged as a
breaking change (exit 1).  New additions are reported but allowed.

Usage (CI):
    uv run python scripts/check_api_surface.py

Regenerate baseline after intentional API changes:
    uv run python scripts/check_api_surface.py --update-baseline
"""

from __future__ import annotations

import sys
from pathlib import Path

BASELINE_PATH = Path(__file__).parent / "api_surface_baseline.txt"


def get_current_surface() -> set[str]:
    """Return the set of names in pycypher.__all__."""
    import pycypher

    return set(pycypher.__all__)


def load_baseline() -> set[str]:
    """Load the frozen baseline from disk."""
    if not BASELINE_PATH.exists():
        return set()
    return {
        line.strip()
        for line in BASELINE_PATH.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }


def save_baseline(names: set[str]) -> None:
    """Write the current API surface as the new baseline."""
    header = "# pycypher public API baseline (auto-generated)\n"
    BASELINE_PATH.write_text(
        header + "\n".join(sorted(names)) + "\n"
    )


def main() -> int:
    if "--update-baseline" in sys.argv:
        surface = get_current_surface()
        save_baseline(surface)
        print(f"Baseline updated: {len(surface)} names written to {BASELINE_PATH}")
        return 0

    current = get_current_surface()
    baseline = load_baseline()

    if not baseline:
        print("No baseline found. Generating initial baseline...")
        save_baseline(current)
        print(f"Baseline created: {len(current)} names written to {BASELINE_PATH}")
        return 0

    removed = baseline - current
    added = current - baseline

    if added:
        print(f"New API additions ({len(added)}):")
        for name in sorted(added):
            print(f"  + {name}")

    if removed:
        print(f"\nBREAKING: API removals detected ({len(removed)}):")
        for name in sorted(removed):
            print(f"  - {name}")
        print(
            "\nIf these removals are intentional, update the baseline:\n"
            "  uv run python scripts/check_api_surface.py --update-baseline"
        )
        return 1

    if not added:
        print("API surface unchanged.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
