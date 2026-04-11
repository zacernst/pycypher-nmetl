#!/usr/bin/env python3
"""check_import_cycles.py — Detect circular import dependencies.

Builds an import graph from source packages and detects cycles using
Tarjan's algorithm (strongly connected components with >1 node).

Usage (CI):
    uv run python scripts/check_import_cycles.py

Exit codes:
    0  — No import cycles found.
    1  — Import cycles detected.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Packages to scan for import cycles
PACKAGES = {
    "pycypher": REPO_ROOT / "packages" / "pycypher" / "src" / "pycypher",
    "shared": REPO_ROOT / "packages" / "shared" / "src" / "shared",
}


def module_name_from_path(py_file: Path, src_root: Path) -> str:
    """Convert a file path to a dotted module name."""
    rel = py_file.relative_to(src_root)
    parts = list(rel.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def collect_modules(
    pkg_name: str, pkg_dir: Path,
) -> dict[str, Path]:
    """Collect all modules in a package."""
    src_root = pkg_dir.parent  # e.g. packages/pycypher/src
    modules: dict[str, Path] = {}
    if not pkg_dir.exists():
        return modules
    for py_file in pkg_dir.rglob("*.py"):
        mod_name = module_name_from_path(py_file, src_root)
        if mod_name:
            modules[mod_name] = py_file
    return modules


def extract_imports(py_file: Path, module_name: str) -> set[str]:
    """Extract import targets from a Python file using AST."""
    try:
        text = py_file.read_text(errors="replace")
        tree = ast.parse(text, filename=str(py_file))
    except (OSError, SyntaxError):
        return set()

    imports: set[str] = set()
    pkg_parts = module_name.split(".")

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level > 0:
                # Relative import — resolve against current module
                base_parts = pkg_parts[: max(0, len(pkg_parts) - node.level)]
                if node.module:
                    base_parts.extend(node.module.split("."))
                resolved = ".".join(base_parts)
                if resolved:
                    imports.add(resolved)
            elif node.module:
                imports.add(node.module)

    return imports


def build_graph(
    all_modules: dict[str, Path],
) -> dict[str, set[str]]:
    """Build a directed graph of module imports."""
    graph: dict[str, set[str]] = {mod: set() for mod in all_modules}

    for mod_name, py_file in all_modules.items():
        raw_imports = extract_imports(py_file, mod_name)
        for imp in raw_imports:
            # Only track edges within our known modules
            # Check exact match and parent package match
            if imp in all_modules:
                graph[mod_name].add(imp)
            else:
                # Check if it's a submodule reference
                for known in all_modules:
                    if known.startswith(imp + ".") or imp.startswith(known + "."):
                        graph[mod_name].add(known)

    return graph


def find_cycles_tarjan(graph: dict[str, set[str]]) -> list[list[str]]:
    """Find strongly connected components using Tarjan's algorithm."""
    index_counter = [0]
    stack: list[str] = []
    on_stack: set[str] = set()
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    sccs: list[list[str]] = []

    def strongconnect(v: str) -> None:
        indices[v] = index_counter[0]
        lowlinks[v] = index_counter[0]
        index_counter[0] += 1
        stack.append(v)
        on_stack.add(v)

        for w in graph.get(v, set()):
            if w not in indices:
                strongconnect(w)
                lowlinks[v] = min(lowlinks[v], lowlinks[w])
            elif w in on_stack:
                lowlinks[v] = min(lowlinks[v], indices[w])

        if lowlinks[v] == indices[v]:
            scc: list[str] = []
            while True:
                w = stack.pop()
                on_stack.discard(w)
                scc.append(w)
                if w == v:
                    break
            if len(scc) > 1:
                sccs.append(sorted(scc))

    for node in sorted(graph):
        if node not in indices:
            strongconnect(node)

    return sccs


def main() -> int:
    # Collect all modules across packages
    all_modules: dict[str, Path] = {}
    for pkg_name, pkg_dir in PACKAGES.items():
        all_modules.update(collect_modules(pkg_name, pkg_dir))

    if not all_modules:
        print("No modules found to scan.")
        return 0

    graph = build_graph(all_modules)
    cycles = find_cycles_tarjan(graph)

    print(f"Scanned {len(all_modules)} modules across {len(PACKAGES)} packages.")

    if not cycles:
        print("OK: No import cycles detected.")
        return 0

    print(f"\nIMPORT CYCLES DETECTED ({len(cycles)}):\n")
    for i, scc in enumerate(cycles, 1):
        print(f"  Cycle {i} ({len(scc)} modules):")
        for mod in scc:
            rel = all_modules[mod].relative_to(REPO_ROOT)
            print(f"    {mod}  ({rel})")
        print()

    print(
        f"{len(cycles)} import cycle(s) found."
        "\nCircular imports can cause ImportError at runtime and indicate"
        "\ntight coupling that should be refactored."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
