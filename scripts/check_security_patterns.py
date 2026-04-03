#!/usr/bin/env python3
"""check_security_patterns.py — Grep-based security pattern detection.

Catches common security anti-patterns that bandit may miss or that are
project-specific:

1. SQL string formatting (f-strings, .format(), %) in query construction
2. Bare except clauses (swallow errors silently)
3. Use of eval/exec on untrusted input
4. Hardcoded secrets patterns

Usage (CI):
    uv run python scripts/check_security_patterns.py

Exit codes:
    0  — No violations found.
    1  — Security pattern violations detected.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

SOURCE_DIRS = [
    REPO_ROOT / "packages" / "pycypher" / "src",
    REPO_ROOT / "packages" / "shared" / "src",
]

# Files that are legitimately allowed to use certain patterns
ALLOWLIST: dict[str, set[str]] = {
    # input_validator.py validates SQL-like input, not constructing SQL
    "sql_format": {"input_validator.py", "check_security_patterns.py"},
    # Some modules need bare except for graceful degradation
    "bare_except": {"grammar_parser.py"},
}


@dataclass
class Violation:
    file: Path
    line_no: int
    line: str
    rule: str
    description: str


PATTERNS: list[tuple[str, re.Pattern[str], str]] = [
    (
        "sql_format",
        re.compile(
            r"""(?x)
            (?:execute|cursor\.execute|\.sql|query)\s*\(
            \s*f["\']              # f-string in SQL context
            """,
        ),
        "SQL query constructed with f-string — use parameterized queries",
    ),
    (
        "sql_format",
        re.compile(
            r"""(?x)
            (?:execute|cursor\.execute|\.sql|query)\s*\(
            .*\.format\(           # .format() in SQL context
            """,
        ),
        "SQL query constructed with .format() — use parameterized queries",
    ),
    (
        "sql_format",
        re.compile(
            r"""(?x)
            (?:execute|cursor\.execute|\.sql|query)\s*\(
            .*%\s                  # % formatting in SQL context
            """,
        ),
        "SQL query constructed with % formatting — use parameterized queries",
    ),
    (
        "bare_except",
        re.compile(r"^\s*except\s*:\s*$"),
        "Bare except clause — catch specific exceptions instead",
    ),
    (
        "eval_exec",
        re.compile(r"\beval\s*\("),
        "Use of eval() — avoid on untrusted input",
    ),
    (
        "hardcoded_secret",
        re.compile(
            r"""(?xi)
            (?:password|secret|api_key|token)\s*=\s*["\'][^"\']{8,}["\']
            """,
        ),
        "Possible hardcoded secret — use environment variables",
    ),
]


def scan_file(py_file: Path) -> list[Violation]:
    """Scan a single file for security anti-patterns."""
    violations: list[Violation] = []
    try:
        lines = py_file.read_text(errors="replace").splitlines()
    except OSError:
        return violations

    for line_no, line in enumerate(lines, start=1):
        # Skip comments
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue

        for rule, pattern, description in PATTERNS:
            if pattern.search(line):
                # Check allowlist
                if py_file.name in ALLOWLIST.get(rule, set()):
                    continue
                # Honor nosec / noqa inline suppressions
                if "# nosec" in line or "# noqa" in line:
                    continue
                violations.append(
                    Violation(
                        file=py_file,
                        line_no=line_no,
                        line=line.rstrip(),
                        rule=rule,
                        description=description,
                    )
                )
    return violations


def main() -> int:
    all_violations: list[Violation] = []

    for src_dir in SOURCE_DIRS:
        if not src_dir.exists():
            continue
        for py_file in sorted(src_dir.rglob("*.py")):
            all_violations.extend(scan_file(py_file))

    if not all_violations:
        print("OK: No security anti-patterns detected.")
        return 0

    print(f"SECURITY PATTERN VIOLATIONS ({len(all_violations)}):\n")
    for v in all_violations:
        rel = v.file.relative_to(REPO_ROOT)
        print(f"  {rel}:{v.line_no}  [{v.rule}]")
        print(f"    {v.description}")
        print(f"    > {v.line.strip()}")
        print()

    print(
        f"{len(all_violations)} violation(s) found."
        "\nIf intentional, add to ALLOWLIST in this script."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
