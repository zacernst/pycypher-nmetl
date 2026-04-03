#!/usr/bin/env python3
"""check_orphan_modules.py — Detect .py modules with zero inbound imports.

Scans source packages for Python files that are never imported by any other
module in the workspace.  Orphan modules indicate dead code or missing
integration.

Usage (CI):
    uv run python scripts/check_orphan_modules.py

Exit codes:
    0  — No orphans found (or only allowlisted ones).
    1  — Orphan modules detected.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Directories to scan for source modules
SOURCE_DIRS = [
    REPO_ROOT / "packages" / "pycypher" / "src",
    REPO_ROOT / "packages" / "shared" / "src",
]

# Directories to scan for import references (includes tests)
IMPORT_SCAN_DIRS = [
    REPO_ROOT / "packages" / "pycypher" / "src",
    REPO_ROOT / "packages" / "shared" / "src",
    REPO_ROOT / "tests",
    REPO_ROOT / "examples",
]

# Modules that are legitimately standalone (entry points, CLI, etc.)
ALLOWLIST = {
    "__init__",
    "__main__",
    "conftest",
    "health_server",      # standalone HTTP server
    "cypher_lsp",         # LSP server entry point
    "nmetl_cli",          # CLI entry point
    "_cli_query",         # CLI entry point
}


def collect_module_names(source_dirs: list[Path]) -> dict[str, Path]:
    """Return a mapping of module stem -> file path for all .py files."""
    modules: dict[str, Path] = {}
    for src_dir in source_dirs:
        if not src_dir.exists():
            continue
        for py_file in src_dir.rglob("*.py"):
            stem = py_file.stem
            if stem not in ALLOWLIST:
                modules[stem] = py_file
    return modules


def collect_all_imports(scan_dirs: list[Path]) -> set[str]:
    """Scan all .py files and extract imported module names.

    Uses AST parsing for accuracy — handles multiline imports, parenthesized
    import lists, relative imports, etc.
    """
    import ast

    imported: set[str] = set()

    for scan_dir in scan_dirs:
        if not scan_dir.exists():
            continue
        for py_file in scan_dir.rglob("*.py"):
            try:
                text = py_file.read_text(errors="replace")
                tree = ast.parse(text, filename=str(py_file))
            except (OSError, SyntaxError):
                continue

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        # "import a.b.c" — record each component
                        for part in alias.name.split("."):
                            imported.add(part)
                elif isinstance(node, ast.ImportFrom):
                    # "from a.b import c, d" — record module parts + names
                    if node.module:
                        for part in node.module.split("."):
                            imported.add(part)
                    for alias in node.names:
                        imported.add(alias.name)
    return imported


def main() -> int:
    modules = collect_module_names(SOURCE_DIRS)
    imported = collect_all_imports(IMPORT_SCAN_DIRS)

    orphans = {
        name: path
        for name, path in sorted(modules.items())
        if name not in imported
    }

    if not orphans:
        print(f"OK: All {len(modules)} modules have at least one inbound import.")
        return 0

    print(f"ORPHAN MODULES DETECTED ({len(orphans)}):\n")
    for name, path in sorted(orphans.items()):
        rel = path.relative_to(REPO_ROOT)
        print(f"  {name}  ({rel})")

    print(
        f"\n{len(orphans)} module(s) have zero inbound imports."
        "\nIf intentional (entry points, etc.), add to ALLOWLIST in this script."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
