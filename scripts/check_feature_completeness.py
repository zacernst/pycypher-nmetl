#!/usr/bin/env python3
"""check_feature_completeness.py — Track incomplete feature indicators.

Detects patterns that signal unfinished work in source and test code:

1. pytest.skip / @pytest.mark.skip — tests explicitly skipped
2. @pytest.mark.xfail — tests expected to fail
3. bare try/except pass — silently swallowed errors
4. TODO/FIXME/HACK/XXX comments — acknowledged tech debt
5. NotImplementedError raises — stub implementations

Usage (CI):
    uv run python scripts/check_feature_completeness.py

Exit codes:
    0  — Report generated (advisory; never blocks CI).
"""

from __future__ import annotations

import ast
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

SOURCE_DIRS = [
    REPO_ROOT / "packages" / "pycypher" / "src",
    REPO_ROOT / "packages" / "shared" / "src",
    REPO_ROOT / "packages" / "pycypher-tui" / "src",
]

TEST_DIRS = [
    REPO_ROOT / "tests",
]


@dataclass
class Indicator:
    file: Path
    line_no: int
    category: str
    detail: str


# Regex patterns for comment-based indicators
_COMMENT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("TODO", re.compile(r"#\s*TODO\b", re.IGNORECASE)),
    ("FIXME", re.compile(r"#\s*FIXME\b", re.IGNORECASE)),
    ("HACK", re.compile(r"#\s*HACK\b", re.IGNORECASE)),
    ("XXX", re.compile(r"#\s*XXX\b", re.IGNORECASE)),
]

# Regex patterns for code-based indicators
_CODE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("skip", re.compile(r"pytest\.skip\(")),
    ("skip_decorator", re.compile(r"@pytest\.mark\.skip\b")),
    ("xfail", re.compile(r"@pytest\.mark\.xfail\b")),
    ("not_implemented", re.compile(r"raise\s+NotImplementedError\b")),
]


def scan_file(py_file: Path) -> list[Indicator]:
    """Scan a single file for feature-completeness indicators."""
    indicators: list[Indicator] = []
    try:
        text = py_file.read_text(errors="replace")
        lines = text.splitlines()
    except OSError:
        return indicators

    for line_no, line in enumerate(lines, start=1):
        stripped = line.strip()

        # Comment-based patterns
        for category, pattern in _COMMENT_PATTERNS:
            if pattern.search(line):
                indicators.append(
                    Indicator(
                        file=py_file,
                        line_no=line_no,
                        category=category,
                        detail=stripped,
                    )
                )

        # Code-based patterns
        for category, pattern in _CODE_PATTERNS:
            if pattern.search(line):
                indicators.append(
                    Indicator(
                        file=py_file,
                        line_no=line_no,
                        category=category,
                        detail=stripped,
                    )
                )

    # AST-based: detect bare try/except pass
    try:
        tree = ast.parse(text, filename=str(py_file))
    except SyntaxError:
        return indicators

    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            for handler in node.handlers:
                # Bare except (no type) with only pass in body
                if (
                    handler.type is None
                    and len(handler.body) == 1
                    and isinstance(handler.body[0], ast.Pass)
                ):
                    indicators.append(
                        Indicator(
                            file=py_file,
                            line_no=handler.lineno,
                            category="bare_except_pass",
                            detail="except: pass (silently swallows all errors)",
                        )
                    )

    return indicators


def main() -> int:
    all_indicators: list[Indicator] = []

    for scan_dir in SOURCE_DIRS + TEST_DIRS:
        if not scan_dir.exists():
            continue
        for py_file in sorted(scan_dir.rglob("*.py")):
            all_indicators.extend(scan_file(py_file))

    # Summary
    counts: Counter[str] = Counter()
    for ind in all_indicators:
        counts[ind.category] += 1

    total = len(all_indicators)
    print("Feature Completeness Report")
    print("=" * 40)

    if not all_indicators:
        print("No incomplete-feature indicators found.")
        return 0

    print(f"Total indicators: {total}\n")
    print("By category:")
    for category, count in counts.most_common():
        print(f"  {category:25s} {count:4d}")

    print(f"\nTop locations (by file):")
    file_counts: Counter[str] = Counter()
    for ind in all_indicators:
        rel = str(ind.file.relative_to(REPO_ROOT))
        file_counts[rel] += 1

    for filepath, count in file_counts.most_common(10):
        print(f"  {filepath:60s} {count:4d}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
