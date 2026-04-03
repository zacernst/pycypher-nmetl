#!/usr/bin/env python3
"""check_file_deps.py — Scan file dependencies before deletion.

Prevents build breaks by finding all consumers of a module before it is
removed.  Scans src/, tests/, and any other Python directories for import
statements, __init__.py re-exports, and string references.

Usage:
    uv run python scripts/check_file_deps.py src/pycypher_tui/widgets/help_system.py
    uv run python scripts/check_file_deps.py --package pycypher-tui src/pycypher_tui/modes/motions.py

Exit codes:
    0 — No consumers found, safe to delete.
    1 — Consumers found, deletion would break the build.
    2 — File is shared infrastructure, requires explicit --force.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Files that require extra coordination before modification/deletion.
SHARED_INFRASTRUCTURE = {
    "app.py",
    "base.py",
    "__init__.py",
    "conftest.py",
}

# Directories to scan (relative to package root).
SCAN_DIRS = ["src", "tests"]


def _module_names_from_path(file_path: Path, package_root: Path) -> list[str]:
    """Derive importable module names from a file path.

    For ``src/pycypher_tui/widgets/help_system.py`` returns:
      - "pycypher_tui.widgets.help_system"
      - "help_system"
    """
    names: list[str] = []
    # Bare module name (e.g. "help_system")
    stem = file_path.stem
    if stem != "__init__":
        names.append(stem)

    # Fully-qualified dotted name relative to src/
    for scan_dir in SCAN_DIRS:
        src_root = package_root / scan_dir
        try:
            rel = file_path.resolve().relative_to(src_root.resolve())
        except ValueError:
            continue
        parts = list(rel.with_suffix("").parts)
        if parts and parts[-1] == "__init__":
            parts = parts[:-1]
        if parts:
            names.append(".".join(parts))
    return names


def _find_consumers(
    module_names: list[str],
    file_path: Path,
    package_root: Path,
) -> list[tuple[Path, int, str]]:
    """Find all files that reference any of the given module names.

    Returns list of (file, line_number, line_text) tuples.
    """
    consumers: list[tuple[Path, int, str]] = []
    resolved_target = file_path.resolve()

    # Build regex patterns for each module name.
    # Two tiers: import patterns (high confidence) and bare name references (lower).
    import_patterns = []
    bare_patterns = []
    for name in module_names:
        escaped = re.escape(name)
        # High confidence: actual import statements
        import_patterns.append(re.compile(
            rf"(?:from\s+\S*{escaped}\s+import|import\s+\S*{escaped})"
        ))
        # Lower confidence: bare word boundary references (catches dynamic imports,
        # string references like "from pycypher_tui.X import Y" in comments, etc.)
        bare_patterns.append(re.compile(rf"\b{escaped}\b"))

    for scan_dir in SCAN_DIRS:
        root = package_root / scan_dir
        if not root.exists():
            continue
        for py_file in root.rglob("*.py"):
            if py_file.resolve() == resolved_target:
                continue
            try:
                lines = py_file.read_text(encoding="utf-8").splitlines()
            except (OSError, UnicodeDecodeError):
                continue
            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                # Check import patterns first (high confidence)
                matched = False
                for pat in import_patterns:
                    if pat.search(line):
                        consumers.append((py_file, i, stripped, "import"))
                        matched = True
                        break
                if not matched:
                    for pat in bare_patterns:
                        if pat.search(line):
                            consumers.append((py_file, i, stripped, "reference"))
                            break

    return consumers


def _is_shared_infrastructure(file_path: Path) -> bool:
    """Check if a file is designated shared infrastructure."""
    return file_path.name in SHARED_INFRASTRUCTURE


def _find_package_root(file_path: Path) -> Path:
    """Walk up from file_path to find the package root (contains src/ or pyproject.toml)."""
    current = file_path.resolve().parent
    for _ in range(20):
        if (current / "pyproject.toml").exists():
            return current
        if (current / "src").is_dir() and (current / "tests").is_dir():
            return current
        parent = current.parent
        if parent == current:
            break
        current = parent
    return file_path.resolve().parent


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check file dependencies before deletion.",
    )
    parser.add_argument(
        "file",
        type=Path,
        help="Python file to check for consumers.",
    )
    parser.add_argument(
        "--package",
        type=str,
        default=None,
        help="Package name (auto-detected from directory structure).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow checking shared infrastructure files.",
    )
    args = parser.parse_args()

    file_path = args.file.resolve()
    if not file_path.exists():
        print(f"WARNING: {args.file} does not exist (already deleted?).")
        print("Scanning for lingering references anyway...")

    package_root = _find_package_root(file_path)

    # Shared infrastructure check
    if _is_shared_infrastructure(file_path) and not args.force:
        print(f"BLOCKED: {file_path.name} is shared infrastructure.")
        print("  These files require explicit coordination before modification.")
        print(f"  Shared infrastructure files: {', '.join(sorted(SHARED_INFRASTRUCTURE))}")
        print("  Use --force to override (with team lead approval).")
        return 2

    module_names = _module_names_from_path(file_path, package_root)
    if not module_names:
        print(f"Could not derive module name from {args.file}")
        return 1

    print(f"Scanning for consumers of: {', '.join(module_names)}")
    print(f"Package root: {package_root}")
    print(f"Scanning: {', '.join(SCAN_DIRS)}")
    print()

    consumers = _find_consumers(module_names, file_path, package_root)

    if not consumers:
        print("SAFE: No consumers found. File can be deleted.")
        return 0

    imports = [(f, n, t, k) for f, n, t, k in consumers if k == "import"]
    refs = [(f, n, t, k) for f, n, t, k in consumers if k == "reference"]

    print(f"BLOCKED: {len(consumers)} reference(s) found:\n")

    if imports:
        print(f"  IMPORTS ({len(imports)}) — these WILL break the build:\n")
        for consumer_file, line_no, line_text, _ in imports:
            try:
                rel = consumer_file.relative_to(package_root)
            except ValueError:
                rel = consumer_file
            print(f"    {rel}:{line_no}")
            print(f"      {line_text}")
            print()

    if refs:
        print(f"  REFERENCES ({len(refs)}) — may be comments/strings, verify manually:\n")
        for consumer_file, line_no, line_text, _ in refs:
            try:
                rel = consumer_file.relative_to(package_root)
            except ValueError:
                rel = consumer_file
            print(f"    {rel}:{line_no}")
            print(f"      {line_text}")
            print()

    print("ACTION REQUIRED before deletion:")
    print("  1. Update or remove each IMPORT reference above")
    print("  2. Verify REFERENCE matches are not false positives")
    print("  3. Re-run this script to confirm zero consumers")
    print("  4. Then delete the file")
    return 1


if __name__ == "__main__":
    sys.exit(main())
