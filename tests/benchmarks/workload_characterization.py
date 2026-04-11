"""Query workload characterization framework.

Analyzes benchmark results to classify query patterns, identify performance
profiles by category, and track how different workload types scale across
dataset sizes.

Usage::

    # Analyze benchmark results from saved baselines
    uv run python tests/benchmarks/workload_characterization.py \\
        --benchmark-dir .benchmarks \\
        --output workload-report.md

    # JSON output for programmatic use
    uv run python tests/benchmarks/workload_characterization.py \\
        --benchmark-dir .benchmarks \\
        --format json \\
        --output workload-report.json

Workload categories:
    - parser:       Query parsing (no execution)
    - scan:         Full entity scans (MATCH ... RETURN)
    - filter:       Filtered scans (MATCH ... WHERE ... RETURN)
    - traversal:    Relationship traversals (single/multi hop)
    - aggregation:  GROUP BY / aggregate queries
    - optimizer:    Query optimization passes
    - scalar:       Scalar function execution
    - multi_type:   Cross-entity-type joins
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Workload classification
# ---------------------------------------------------------------------------

# Patterns to classify benchmark names into workload categories
_CATEGORY_PATTERNS: list[tuple[str, str]] = [
    (r"parse|parser", "parser"),
    (r"aggregat|count|avg|sum|collect", "aggregation"),
    (r"hop|travers|relationship|knows", "traversal"),
    (r"filter|where", "filter"),
    (r"scan|simple.*match|match.*return", "scan"),
    (r"optimi|rule|pushdown|reorder", "optimizer"),
    (r"scalar|registry|toupper|tolower|abs|trim|size", "scalar"),
    (r"multi.*type|company|location|works_at|lives_in", "multi_type"),
    (r"memory|mem_", "memory"),
]

# Patterns to extract dataset scale from benchmark names
_SCALE_PATTERNS: list[tuple[str, str]] = [
    (r"100[kK]|100_?000", "100K"),
    (r"10[kK]|10_?000", "10K"),
    (r"1[kK]|1_?000", "1K"),
    (r"tiny|small", "small"),
]


def classify_benchmark(name: str) -> str:
    """Classify a benchmark name into a workload category."""
    name_lower = name.lower()
    for pattern, category in _CATEGORY_PATTERNS:
        if re.search(pattern, name_lower):
            return category
    return "other"


def extract_scale(name: str) -> str:
    """Extract the dataset scale from a benchmark name."""
    for pattern, scale in _SCALE_PATTERNS:
        if re.search(pattern, name):
            return scale
    return "unknown"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkEntry:
    """Single benchmark result with classification."""

    name: str
    category: str
    scale: str
    mean_s: float
    stddev_s: float
    min_s: float
    max_s: float
    rounds: int


@dataclass
class CategorySummary:
    """Aggregate statistics for a workload category."""

    category: str
    count: int
    mean_s: float
    min_s: float
    max_s: float
    benchmarks: list[str] = field(default_factory=list)


@dataclass
class ScalingSummary:
    """Performance scaling across dataset sizes for a workload type."""

    category: str
    scales: dict[str, float] = field(default_factory=dict)  # scale → mean_s


@dataclass
class WorkloadReport:
    """Full workload characterization report."""

    source_file: str
    total_benchmarks: int
    categories: list[CategorySummary] = field(default_factory=list)
    scaling: list[ScalingSummary] = field(default_factory=list)
    entries: list[BenchmarkEntry] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------


def _load_benchmark_file(path: Path) -> list[dict]:
    """Load benchmarks from a pytest-benchmark JSON file."""
    with path.open() as f:
        data = json.load(f)
    return data.get("benchmarks", [])


def _find_latest_benchmark(benchmark_dir: Path) -> Path | None:
    """Find the most recent benchmark JSON in the directory tree."""
    json_files = sorted(benchmark_dir.rglob("*.json"), key=lambda p: p.stat().st_mtime)
    if not json_files:
        return None
    return json_files[-1]


def characterize_workload(benchmark_path: Path) -> WorkloadReport:
    """Analyze a benchmark results file and produce a workload report."""
    raw_benchmarks = _load_benchmark_file(benchmark_path)

    entries: list[BenchmarkEntry] = []
    for bench in raw_benchmarks:
        name = bench.get("name", "")
        stats = bench.get("stats", {})
        entries.append(
            BenchmarkEntry(
                name=name,
                category=classify_benchmark(name),
                scale=extract_scale(name),
                mean_s=stats.get("mean", 0.0),
                stddev_s=stats.get("stddev", 0.0),
                min_s=stats.get("min", 0.0),
                max_s=stats.get("max", 0.0),
                rounds=stats.get("rounds", 0),
            )
        )

    # Aggregate by category
    cat_map: dict[str, list[BenchmarkEntry]] = {}
    for entry in entries:
        cat_map.setdefault(entry.category, []).append(entry)

    categories: list[CategorySummary] = []
    for cat, cat_entries in sorted(cat_map.items()):
        means = [e.mean_s for e in cat_entries]
        categories.append(
            CategorySummary(
                category=cat,
                count=len(cat_entries),
                mean_s=sum(means) / len(means) if means else 0.0,
                min_s=min(means) if means else 0.0,
                max_s=max(means) if means else 0.0,
                benchmarks=[e.name for e in cat_entries],
            )
        )

    # Scaling analysis: group by (category, scale)
    scale_map: dict[str, dict[str, list[float]]] = {}
    for entry in entries:
        if entry.scale != "unknown":
            scale_map.setdefault(entry.category, {}).setdefault(
                entry.scale, []
            ).append(entry.mean_s)

    scaling: list[ScalingSummary] = []
    for cat, scales in sorted(scale_map.items()):
        scale_means = {
            scale: sum(vals) / len(vals) for scale, vals in sorted(scales.items())
        }
        if len(scale_means) > 1:
            scaling.append(ScalingSummary(category=cat, scales=scale_means))

    return WorkloadReport(
        source_file=str(benchmark_path),
        total_benchmarks=len(entries),
        categories=categories,
        scaling=scaling,
        entries=entries,
    )


# ---------------------------------------------------------------------------
# Formatters
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


def format_markdown(report: WorkloadReport) -> str:
    """Format workload report as Markdown."""
    lines: list[str] = []
    lines.append("# Workload Characterization Report")
    lines.append("")
    lines.append(f"**Source**: `{report.source_file}`")
    lines.append(f"**Total benchmarks**: {report.total_benchmarks}")
    lines.append("")

    # Category breakdown
    lines.append("## Workload Categories")
    lines.append("")
    lines.append("| Category | Count | Avg Time | Min Time | Max Time |")
    lines.append("|----------|-------|----------|----------|----------|")
    for cat in sorted(report.categories, key=lambda c: -c.mean_s):
        lines.append(
            f"| {cat.category} | {cat.count} "
            f"| {_format_time(cat.mean_s)} "
            f"| {_format_time(cat.min_s)} "
            f"| {_format_time(cat.max_s)} |"
        )
    lines.append("")

    # Scaling analysis
    if report.scaling:
        lines.append("## Scaling Analysis")
        lines.append("")
        lines.append(
            "Shows how performance scales across dataset sizes per category."
        )
        lines.append("")
        for sc in report.scaling:
            lines.append(f"### {sc.category}")
            lines.append("")
            lines.append("| Scale | Mean Time |")
            lines.append("|-------|-----------|")
            for scale, mean_s in sorted(
                sc.scales.items(),
                key=lambda x: {"small": 0, "1K": 1, "10K": 2, "100K": 3}.get(
                    x[0], 99
                ),
            ):
                lines.append(f"| {scale} | {_format_time(mean_s)} |")
            lines.append("")

    # Category distribution
    lines.append("## Category Distribution")
    lines.append("")
    total = report.total_benchmarks or 1
    for cat in sorted(report.categories, key=lambda c: -c.count):
        pct = (cat.count / total) * 100
        bar = "#" * int(pct / 2)
        lines.append(f"- **{cat.category}**: {cat.count} ({pct:.0f}%) {bar}")
    lines.append("")

    return "\n".join(lines)


def format_json(report: WorkloadReport) -> str:
    """Format workload report as JSON."""
    return json.dumps(asdict(report), indent=2, default=str)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Characterize query workloads from benchmark results",
    )
    parser.add_argument(
        "--benchmark-dir",
        default=".benchmarks",
        help="Directory containing benchmark JSON files (default: .benchmarks)",
    )
    parser.add_argument(
        "--benchmark-file",
        default=None,
        help="Specific benchmark JSON file to analyze (overrides --benchmark-dir)",
    )
    parser.add_argument(
        "--format",
        choices=["markdown", "json"],
        default="markdown",
        help="Output format (default: markdown)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output file path (stdout if omitted)",
    )

    args = parser.parse_args()

    if args.benchmark_file:
        benchmark_path = Path(args.benchmark_file)
    else:
        benchmark_path = _find_latest_benchmark(Path(args.benchmark_dir))

    if benchmark_path is None or not benchmark_path.exists():
        print(
            "Error: no benchmark files found. Run `make bench-save` first.",
            file=sys.stderr,
        )
        return 1

    report = characterize_workload(benchmark_path)

    if args.format == "json":
        output = format_json(report)
    else:
        output = format_markdown(report)

    if args.output:
        Path(args.output).write_text(output)
        print(f"Report written to {args.output}")
    else:
        print(output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
