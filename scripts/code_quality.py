#!/usr/bin/env python3
"""Code quality analysis and developer health dashboard.

Provides a quick overview of project code quality metrics:
- Lint violations by category
- Code complexity hotspots (functions with high cyclomatic complexity)
- Type annotation coverage
- Test coverage summary
- Unused code detection

Usage:
    uv run python scripts/code_quality.py              # Full report
    uv run python scripts/code_quality.py --complexity  # Complexity only
    uv run python scripts/code_quality.py --lint        # Lint summary only
    uv run python scripts/code_quality.py --changed     # Only changed files
"""

from __future__ import annotations

import argparse
import ast
import subprocess
import sys
from pathlib import Path

# Production source directories
SRC_DIRS = [
    "packages/pycypher/src/pycypher",
    "packages/shared/src/shared",
]

# Complexity thresholds
COMPLEXITY_WARN = 10
COMPLEXITY_ERROR = 20


def _run(cmd: list[str], *, check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


# ── Complexity analysis ──────────────────────────────────────────────


def _cyclomatic_complexity(node: ast.AST) -> int:
    """Approximate cyclomatic complexity of a function/method AST node."""
    complexity = 1
    for child in ast.walk(node):
        if isinstance(child, (ast.If, ast.While, ast.For, ast.ExceptHandler)):
            complexity += 1
        elif isinstance(child, ast.BoolOp):
            complexity += len(child.values) - 1
        elif isinstance(child, (ast.Assert, ast.With)):
            complexity += 1
    return complexity


def analyze_complexity(paths: list[str] | None = None) -> list[dict]:
    """Find functions with high cyclomatic complexity."""
    targets = paths or SRC_DIRS
    results = []

    for target in targets:
        for py_file in Path(target).rglob("*.py"):
            try:
                tree = ast.parse(py_file.read_text())
            except SyntaxError:
                continue

            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    cc = _cyclomatic_complexity(node)
                    if cc >= COMPLEXITY_WARN:
                        # Try to find containing class
                        class_name = None
                        for parent in ast.walk(tree):
                            if isinstance(parent, ast.ClassDef):
                                for child in ast.iter_child_nodes(parent):
                                    if child is node:
                                        class_name = parent.name
                                        break

                        name = node.name
                        if class_name:
                            name = f"{class_name}.{name}"

                        results.append({
                            "file": str(py_file),
                            "line": node.lineno,
                            "function": name,
                            "complexity": cc,
                            "level": "ERROR" if cc >= COMPLEXITY_ERROR else "WARN",
                        })

    results.sort(key=lambda r: r["complexity"], reverse=True)
    return results


# ── Lint summary ─────────────────────────────────────────────────────


def lint_summary() -> dict:
    """Get ruff violation summary."""
    result = _run(["uv", "run", "ruff", "check", ".", "--statistics"])
    lines = result.stdout.strip().splitlines() if result.stdout else []

    total = 0
    categories: dict[str, int] = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if parts and parts[0].isdigit():
            count = int(parts[0])
            rule = parts[1] if len(parts) > 1 else "unknown"
            categories[rule] = count
            total += count

    # Also get the "Found N errors" line from stderr
    for line in (result.stderr or "").splitlines():
        if "Found" in line and "errors" in line:
            try:
                total = int(line.split()[1])
            except (ValueError, IndexError):
                pass

    return {"total": total, "top_categories": dict(list(categories.items())[:10])}


# ── Type annotation coverage ─────────────────────────────────────────


def type_annotation_coverage() -> dict:
    """Check type annotation coverage in production code."""
    result = _run(["uv", "run", "ruff", "check", *SRC_DIRS, "--select", "ANN", "--statistics"])
    lines = result.stdout.strip().splitlines() if result.stdout else []

    missing = 0
    for line in lines:
        parts = line.strip().split()
        if parts and parts[0].isdigit():
            missing += int(parts[0])

    return {"missing_annotations": missing}


# ── Changed files analysis ───────────────────────────────────────────


def get_changed_files() -> list[str]:
    """Get Python files changed vs main."""
    result = _run(["git", "diff", "--name-only", "--diff-filter=AMR", "HEAD", "--", "*.py"])
    if result.returncode != 0:
        result = _run(["git", "diff", "--name-only", "--diff-filter=AMR", "origin/main", "--", "*.py"])
    return [f for f in result.stdout.strip().splitlines() if f.endswith(".py")]


# ── Report formatting ────────────────────────────────────────────────


def print_section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def print_report(
    *,
    show_complexity: bool = True,
    show_lint: bool = True,
    changed_only: bool = False,
) -> int:
    """Print the full quality report. Returns exit code (1 if errors found)."""
    exit_code = 0
    paths = None

    if changed_only:
        changed = get_changed_files()
        if not changed:
            print("No changed Python files found.")
            return 0
        print(f"Analyzing {len(changed)} changed file(s)...")
        paths = changed

    if show_lint:
        print_section("LINT VIOLATIONS")
        lint = lint_summary()
        print(f"  Total violations: {lint['total']}")
        if lint["top_categories"]:
            print("  Top categories:")
            for rule, count in lint["top_categories"].items():
                print(f"    {rule:10s} {count:>5d}")

    if show_complexity:
        print_section("COMPLEXITY HOTSPOTS")
        hotspots = analyze_complexity(paths)
        if not hotspots:
            print("  No functions exceed complexity threshold.")
        else:
            errors = [h for h in hotspots if h["level"] == "ERROR"]
            warns = [h for h in hotspots if h["level"] == "WARN"]
            print(f"  {len(errors)} ERROR (>={COMPLEXITY_ERROR}), {len(warns)} WARN (>={COMPLEXITY_WARN})")
            print()
            for h in hotspots[:20]:
                marker = "!!" if h["level"] == "ERROR" else "  "
                print(
                    f"  {marker} CC={h['complexity']:>2d}  "
                    f"{h['file']}:{h['line']}  {h['function']}"
                )
            if errors:
                exit_code = 1

    if show_lint:
        print_section("TYPE ANNOTATION COVERAGE")
        ann = type_annotation_coverage()
        print(f"  Missing annotations in production code: {ann['missing_annotations']}")

    print()
    return exit_code


def main() -> None:
    parser = argparse.ArgumentParser(description="Code quality analysis dashboard")
    parser.add_argument("--complexity", action="store_true", help="Show complexity only")
    parser.add_argument("--lint", action="store_true", help="Show lint summary only")
    parser.add_argument("--changed", action="store_true", help="Analyze changed files only")
    args = parser.parse_args()

    if args.complexity:
        code = print_report(show_complexity=True, show_lint=False, changed_only=args.changed)
    elif args.lint:
        code = print_report(show_complexity=False, show_lint=True, changed_only=args.changed)
    else:
        code = print_report(changed_only=args.changed)

    sys.exit(code)


if __name__ == "__main__":
    main()
