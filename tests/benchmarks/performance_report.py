"""Performance regression detection and reporting tool.

Compares benchmark JSON results against a baseline and generates a
human-readable Markdown report with per-test regression/improvement
classification, statistical summaries, and actionable recommendations.

Usage::

    # Compare a results file against baseline
    uv run python tests/benchmarks/performance_report.py \\
        --baseline .benchmarks/Darwin-CPython-3.14-64bit/0001_baseline.json \\
        --current benchmark-results.json \\
        --output performance-report.md

    # Use threshold (default 5%)
    uv run python tests/benchmarks/performance_report.py \\
        --baseline .benchmarks/Darwin-CPython-3.14-64bit/0001_baseline.json \\
        --current benchmark-results.json \\
        --threshold 10.0

    # JSON output for CI integration
    uv run python tests/benchmarks/performance_report.py \\
        --baseline .benchmarks/Darwin-CPython-3.14-64bit/0001_baseline.json \\
        --current benchmark-results.json \\
        --format json
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkComparison:
    """Comparison of a single benchmark between baseline and current."""

    name: str
    baseline_mean: float
    current_mean: float
    baseline_stddev: float
    current_stddev: float
    change_pct: float
    status: str  # "regression", "improvement", "stable", "new", "removed"
    baseline_rounds: int = 0
    current_rounds: int = 0


@dataclass
class PerformanceReport:
    """Full performance regression report."""

    baseline_commit: str = ""
    current_commit: str = ""
    baseline_file: str = ""
    current_file: str = ""
    threshold_pct: float = 5.0
    comparisons: list[BenchmarkComparison] = field(default_factory=list)
    regressions: list[BenchmarkComparison] = field(default_factory=list)
    improvements: list[BenchmarkComparison] = field(default_factory=list)
    stable: list[BenchmarkComparison] = field(default_factory=list)
    new_benchmarks: list[str] = field(default_factory=list)
    removed_benchmarks: list[str] = field(default_factory=list)
    overall_status: str = "pass"  # "pass" or "fail"


# ---------------------------------------------------------------------------
# Benchmark JSON parsing
# ---------------------------------------------------------------------------


def _load_benchmarks(path: Path) -> dict:
    """Load a pytest-benchmark JSON file."""
    with path.open() as f:
        return json.load(f)


def _extract_benchmark_map(data: dict) -> dict[str, dict]:
    """Extract name→stats mapping from benchmark JSON."""
    result: dict[str, dict] = {}
    for bench in data.get("benchmarks", []):
        name = bench.get("name", "")
        stats = bench.get("stats", {})
        result[name] = {
            "mean": stats.get("mean", 0.0),
            "stddev": stats.get("stddev", 0.0),
            "rounds": stats.get("rounds", 0),
            "min": stats.get("min", 0.0),
            "max": stats.get("max", 0.0),
            "median": stats.get("median", 0.0),
        }
    return result


def _extract_commit(data: dict) -> str:
    """Extract commit hash from benchmark JSON."""
    commit_info = data.get("commit_info", {})
    commit_id = commit_info.get("id", "unknown")
    if len(commit_id) > 8:
        return commit_id[:8]
    return commit_id


# ---------------------------------------------------------------------------
# Comparison logic
# ---------------------------------------------------------------------------


def compare_benchmarks(
    baseline_path: Path,
    current_path: Path,
    *,
    threshold_pct: float = 5.0,
) -> PerformanceReport:
    """Compare two benchmark JSON files and produce a report."""
    baseline_data = _load_benchmarks(baseline_path)
    current_data = _load_benchmarks(current_path)

    baseline_map = _extract_benchmark_map(baseline_data)
    current_map = _extract_benchmark_map(current_data)

    report = PerformanceReport(
        baseline_commit=_extract_commit(baseline_data),
        current_commit=_extract_commit(current_data),
        baseline_file=str(baseline_path),
        current_file=str(current_path),
        threshold_pct=threshold_pct,
    )

    all_names = sorted(set(baseline_map) | set(current_map))

    for name in all_names:
        if name not in baseline_map:
            report.new_benchmarks.append(name)
            continue
        if name not in current_map:
            report.removed_benchmarks.append(name)
            continue

        base = baseline_map[name]
        curr = current_map[name]

        base_mean = base["mean"]
        curr_mean = curr["mean"]

        if base_mean > 0:
            change_pct = ((curr_mean - base_mean) / base_mean) * 100.0
        else:
            change_pct = 0.0

        if change_pct > threshold_pct:
            status = "regression"
        elif change_pct < -threshold_pct:
            status = "improvement"
        else:
            status = "stable"

        comp = BenchmarkComparison(
            name=name,
            baseline_mean=base_mean,
            current_mean=curr_mean,
            baseline_stddev=base["stddev"],
            current_stddev=curr["stddev"],
            change_pct=round(change_pct, 2),
            status=status,
            baseline_rounds=base["rounds"],
            current_rounds=curr["rounds"],
        )
        report.comparisons.append(comp)

        if status == "regression":
            report.regressions.append(comp)
        elif status == "improvement":
            report.improvements.append(comp)
        else:
            report.stable.append(comp)

    if report.regressions:
        report.overall_status = "fail"

    return report


# ---------------------------------------------------------------------------
# Report formatters
# ---------------------------------------------------------------------------


def _format_time(seconds: float) -> str:
    """Format a time value with appropriate units."""
    if seconds < 1e-6:
        return f"{seconds * 1e9:.1f}ns"
    if seconds < 1e-3:
        return f"{seconds * 1e6:.1f}us"
    if seconds < 1.0:
        return f"{seconds * 1e3:.2f}ms"
    return f"{seconds:.3f}s"


def format_markdown(report: PerformanceReport) -> str:
    """Format the report as Markdown."""
    lines: list[str] = []
    status_emoji = "PASS" if report.overall_status == "pass" else "FAIL"

    lines.append("# Performance Regression Report")
    lines.append("")
    lines.append(f"**Status**: {status_emoji}")
    lines.append(
        f"**Baseline**: `{report.baseline_commit}` | "
        f"**Current**: `{report.current_commit}`"
    )
    lines.append(f"**Threshold**: {report.threshold_pct}%")
    lines.append("")

    # Summary
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **Total benchmarks**: {len(report.comparisons)}")
    lines.append(f"- **Regressions**: {len(report.regressions)}")
    lines.append(f"- **Improvements**: {len(report.improvements)}")
    lines.append(f"- **Stable**: {len(report.stable)}")
    if report.new_benchmarks:
        lines.append(f"- **New**: {len(report.new_benchmarks)}")
    if report.removed_benchmarks:
        lines.append(f"- **Removed**: {len(report.removed_benchmarks)}")
    lines.append("")

    # Regressions table
    if report.regressions:
        lines.append("## Regressions (action required)")
        lines.append("")
        lines.append("| Benchmark | Baseline | Current | Change | Stddev |")
        lines.append("|-----------|----------|---------|--------|--------|")
        for comp in sorted(report.regressions, key=lambda c: -c.change_pct):
            lines.append(
                f"| `{comp.name}` "
                f"| {_format_time(comp.baseline_mean)} "
                f"| {_format_time(comp.current_mean)} "
                f"| +{comp.change_pct:.1f}% "
                f"| {_format_time(comp.current_stddev)} |"
            )
        lines.append("")

    # Improvements table
    if report.improvements:
        lines.append("## Improvements")
        lines.append("")
        lines.append("| Benchmark | Baseline | Current | Change |")
        lines.append("|-----------|----------|---------|--------|")
        for comp in sorted(report.improvements, key=lambda c: c.change_pct):
            lines.append(
                f"| `{comp.name}` "
                f"| {_format_time(comp.baseline_mean)} "
                f"| {_format_time(comp.current_mean)} "
                f"| {comp.change_pct:.1f}% |"
            )
        lines.append("")

    # Stable benchmarks (collapsed)
    if report.stable:
        lines.append("## Stable Benchmarks")
        lines.append("")
        lines.append(
            "<details><summary>"
            f"{len(report.stable)} benchmarks within threshold"
            "</summary>"
        )
        lines.append("")
        lines.append("| Benchmark | Baseline | Current | Change |")
        lines.append("|-----------|----------|---------|--------|")
        for comp in sorted(report.stable, key=lambda c: c.name):
            lines.append(
                f"| `{comp.name}` "
                f"| {_format_time(comp.baseline_mean)} "
                f"| {_format_time(comp.current_mean)} "
                f"| {comp.change_pct:+.1f}% |"
            )
        lines.append("")
        lines.append("</details>")
        lines.append("")

    # New and removed
    if report.new_benchmarks:
        lines.append("## New Benchmarks")
        lines.append("")
        for name in report.new_benchmarks:
            lines.append(f"- `{name}`")
        lines.append("")

    if report.removed_benchmarks:
        lines.append("## Removed Benchmarks")
        lines.append("")
        for name in report.removed_benchmarks:
            lines.append(f"- `{name}`")
        lines.append("")

    return "\n".join(lines)


def format_json(report: PerformanceReport) -> str:
    """Format the report as JSON."""
    return json.dumps(asdict(report), indent=2, default=str)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Compare benchmark results and generate regression report",
    )
    parser.add_argument(
        "--baseline",
        type=str,
        required=True,
        help="Path to baseline benchmark JSON",
    )
    parser.add_argument(
        "--current",
        type=str,
        required=True,
        help="Path to current benchmark JSON",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        help="Regression threshold percentage (default: 5.0)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output file path (stdout if omitted)",
    )
    parser.add_argument(
        "--format",
        choices=["markdown", "json"],
        default="markdown",
        help="Output format (default: markdown)",
    )

    args = parser.parse_args()

    baseline_path = Path(args.baseline)
    current_path = Path(args.current)

    if not baseline_path.exists():
        print(
            f"Error: baseline file not found: {baseline_path}", file=sys.stderr
        )
        return 1
    if not current_path.exists():
        print(
            f"Error: current file not found: {current_path}", file=sys.stderr
        )
        return 1

    report = compare_benchmarks(
        baseline_path,
        current_path,
        threshold_pct=args.threshold,
    )

    if args.format == "json":
        output = format_json(report)
    else:
        output = format_markdown(report)

    if args.output:
        Path(args.output).write_text(output)
        print(f"Report written to {args.output}")
    else:
        print(output)

    # Exit with non-zero if regressions detected
    if report.overall_status == "fail":
        print(
            f"\nFAIL: {len(report.regressions)} regression(s) detected "
            f"above {report.threshold_pct}% threshold",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
