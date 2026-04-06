"""cProfile evidence capture for performance claims.

Generates deterministic profiling evidence that can be attached to PRs
or saved alongside benchmark baselines to justify performance claims
with empirical data rather than estimates.

Usage::

    # Profile a specific benchmark test
    uv run python tests/benchmarks/profile_helper.py \\
        --target tests/benchmarks/bench_core_operations.py::TestQueryBenchmarks1K::test_simple_scan \\
        --output-dir .profiles

    # Profile all benchmarks in a file
    uv run python tests/benchmarks/profile_helper.py \\
        --target tests/benchmarks/bench_core_operations.py \\
        --output-dir .profiles

    # Profile with custom sort key
    uv run python tests/benchmarks/profile_helper.py \\
        --target tests/benchmarks/bench_core_operations.py \\
        --sort-key cumulative \\
        --output-dir .profiles

Output files:
    .profiles/<timestamp>_<target>.prof    — binary cProfile data (for snakeviz/pstats)
    .profiles/<timestamp>_<target>.txt     — human-readable top-N callers summary
    .profiles/<timestamp>_summary.json     — machine-readable summary for CI integration
"""

from __future__ import annotations

import argparse
import cProfile
import json
import pstats
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path


@dataclass
class ProfileEntry:
    """Single function profile entry."""

    function: str
    ncalls: int
    tottime: float
    cumtime: float
    filename: str
    lineno: int


@dataclass
class ProfileSummary:
    """Summary of a profiling run."""

    target: str
    timestamp: str
    total_time_s: float
    top_functions: list[ProfileEntry] = field(default_factory=list)
    prof_file: str = ""
    txt_file: str = ""


def _sanitize_target_name(target: str) -> str:
    """Convert a pytest target path to a safe filename component."""
    return (
        target.replace("/", "_")
        .replace("::", "__")
        .replace(".py", "")
        .replace(" ", "_")
    )


def profile_pytest_target(
    target: str,
    *,
    output_dir: Path,
    sort_key: str = "cumulative",
    top_n: int = 30,
) -> ProfileSummary:
    """Profile a pytest target using cProfile and save evidence files.

    Parameters
    ----------
    target:
        Pytest node ID, e.g. ``tests/benchmarks/bench_core.py::TestFoo::test_bar``
    output_dir:
        Directory to write .prof and .txt output files.
    sort_key:
        pstats sort key (cumulative, tottime, calls, etc.).
    top_n:
        Number of top functions to include in the summary.

    Returns
    -------
    ProfileSummary with paths to generated files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_name = _sanitize_target_name(target)
    prof_path = output_dir / f"{ts}_{safe_name}.prof"
    txt_path = output_dir / f"{ts}_{safe_name}.txt"

    # Run pytest with cProfile via subprocess so we capture the full
    # execution including fixtures and teardown.
    start = time.monotonic()
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "cProfile",
            "-o",
            str(prof_path),
            "-m",
            "pytest",
            target,
            "-v",
            "--benchmark-disable",
            "--timeout=120",
            "-x",
        ],
        capture_output=True,
        text=True,
    )
    elapsed = time.monotonic() - start

    if result.returncode != 0 and not prof_path.exists():
        print(f"Warning: pytest exited with code {result.returncode}", file=sys.stderr)
        if result.stderr:
            print(result.stderr[:500], file=sys.stderr)

    # Parse the profile data and write human-readable summary
    top_functions: list[ProfileEntry] = []
    if prof_path.exists():
        stats = pstats.Stats(str(prof_path))
        stats.sort_stats(sort_key)

        # Write human-readable text report
        stream = StringIO()
        stats_for_txt = pstats.Stats(str(prof_path), stream=stream)
        stats_for_txt.sort_stats(sort_key)
        stats_for_txt.print_stats(top_n)
        txt_path.write_text(stream.getvalue())

        # Extract top N entries for machine-readable summary
        for (filename, lineno, func_name), (
            _cc,
            ncalls,
            tottime,
            cumtime,
            _callers,
        ) in list(stats.stats.items())[:top_n]:
            top_functions.append(
                ProfileEntry(
                    function=func_name,
                    ncalls=ncalls,
                    tottime=round(tottime, 6),
                    cumtime=round(cumtime, 6),
                    filename=filename,
                    lineno=lineno,
                )
            )

    return ProfileSummary(
        target=target,
        timestamp=ts,
        total_time_s=round(elapsed, 3),
        top_functions=top_functions,
        prof_file=str(prof_path),
        txt_file=str(txt_path),
    )


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Profile pytest targets and save cProfile evidence",
    )
    parser.add_argument(
        "--target",
        required=True,
        help="Pytest target (file path or node ID)",
    )
    parser.add_argument(
        "--output-dir",
        default=".profiles",
        help="Output directory for profile data (default: .profiles)",
    )
    parser.add_argument(
        "--sort-key",
        default="cumulative",
        choices=["cumulative", "tottime", "calls", "filename", "ncalls"],
        help="Sort key for profile output (default: cumulative)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=30,
        help="Number of top functions to include (default: 30)",
    )

    args = parser.parse_args()
    output_dir = Path(args.output_dir)

    print(f"Profiling: {args.target}")
    print(f"Output:    {output_dir}/")

    summary = profile_pytest_target(
        args.target,
        output_dir=output_dir,
        sort_key=args.sort_key,
        top_n=args.top_n,
    )

    # Write machine-readable summary
    summary_path = output_dir / f"{summary.timestamp}_summary.json"
    summary_path.write_text(json.dumps(asdict(summary), indent=2, default=str))

    print(f"\nCompleted in {summary.total_time_s:.1f}s")
    print(f"  Profile:  {summary.prof_file}")
    print(f"  Summary:  {summary.txt_file}")
    print(f"  JSON:     {summary_path}")
    print(f"  Top {len(summary.top_functions)} functions by {args.sort_key}")

    if summary.top_functions:
        print(f"\n  Hottest function: {summary.top_functions[0].function} "
              f"({summary.top_functions[0].cumtime:.4f}s cumulative)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
