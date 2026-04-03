"""Benchmark utilities for large dataset performance testing.

Provides measurement tools, statistical analysis, and reporting for
systematic performance validation of PyCypher query execution.
"""

from __future__ import annotations

import gc
import statistics
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator


@dataclass(frozen=True)
class PerformanceMetrics:
    """Immutable record of a single benchmark measurement."""

    execution_time_s: float
    memory_before_mb: float
    memory_after_mb: float
    peak_memory_mb: float
    rows_processed: int
    query: str

    @property
    def memory_delta_mb(self) -> float:
        """Net memory change during execution."""
        return self.memory_after_mb - self.memory_before_mb

    @property
    def rows_per_second(self) -> float:
        """Throughput in rows per second."""
        if self.execution_time_s <= 0:
            return float("inf")
        return self.rows_processed / self.execution_time_s


@dataclass
class BenchmarkResult:
    """Aggregated result from multiple benchmark iterations."""

    query: str
    dataset_rows: int
    iterations: int
    times_s: list[float] = field(default_factory=list)
    memory_deltas_mb: list[float] = field(default_factory=list)
    peak_memories_mb: list[float] = field(default_factory=list)

    @property
    def median_time_s(self) -> float:
        """Median execution time across iterations."""
        return statistics.median(self.times_s)

    @property
    def mean_time_s(self) -> float:
        """Mean execution time across iterations."""
        return statistics.mean(self.times_s)

    @property
    def stdev_time_s(self) -> float:
        """Standard deviation of execution times."""
        if len(self.times_s) < 2:
            return 0.0
        return statistics.stdev(self.times_s)

    @property
    def max_peak_memory_mb(self) -> float:
        """Maximum peak memory observed across iterations."""
        return max(self.peak_memories_mb) if self.peak_memories_mb else 0.0

    @property
    def median_memory_delta_mb(self) -> float:
        """Median net memory change across iterations."""
        return (
            statistics.median(self.memory_deltas_mb)
            if self.memory_deltas_mb
            else 0.0
        )

    def assert_time_under(self, threshold_s: float) -> None:
        """Assert median execution time is under threshold."""
        assert self.median_time_s < threshold_s, (
            f"Query too slow: median={self.median_time_s:.2f}s "
            f"(threshold={threshold_s:.2f}s, "
            f"times={[f'{t:.2f}' for t in self.times_s]})"
        )

    def assert_memory_under(self, threshold_mb: float) -> None:
        """Assert peak memory usage stays under threshold."""
        assert self.max_peak_memory_mb < threshold_mb, (
            f"Memory too high: peak={self.max_peak_memory_mb:.1f}MB "
            f"(threshold={threshold_mb:.1f}MB)"
        )


def _get_process_memory_mb() -> float:
    """Get current process memory usage in MB via psutil."""
    try:
        import psutil

        process = psutil.Process()
        return process.memory_info().rss / (1024 * 1024)
    except ImportError:
        return 0.0


@contextmanager
def measure_performance(
    query: str = "",
    rows: int = 0,
) -> Generator[PerformanceMetrics | None]:
    """Context manager that measures execution time and memory usage.

    Usage::

        with measure_performance("MATCH ...", rows=10000) as get_metrics:
            result = star.execute_query(query)
        metrics = get_metrics()
    """
    gc.collect()
    mem_before = _get_process_memory_mb()
    peak_mem = mem_before
    t_start = time.perf_counter()

    # Mutable container for storing result
    result_holder: list[PerformanceMetrics] = []

    def get_metrics() -> PerformanceMetrics:
        if result_holder:
            return result_holder[0]
        msg = "Metrics not yet captured — use after exiting context"
        raise RuntimeError(msg)

    try:
        yield get_metrics  # type: ignore[misc]
    finally:
        t_end = time.perf_counter()
        mem_after = _get_process_memory_mb()
        peak_mem = max(peak_mem, mem_after)
        result_holder.append(
            PerformanceMetrics(
                execution_time_s=t_end - t_start,
                memory_before_mb=mem_before,
                memory_after_mb=mem_after,
                peak_memory_mb=peak_mem,
                rows_processed=rows,
                query=query,
            ),
        )


def run_benchmark(
    execute_fn: object,
    *,
    query: str = "",
    dataset_rows: int = 0,
    iterations: int = 5,
    warmup: int = 1,
) -> BenchmarkResult:
    """Run a benchmark with warmup iterations and statistical aggregation.

    Parameters
    ----------
    execute_fn
        Callable that executes the query under test.
    query
        The Cypher query string (for reporting).
    dataset_rows
        Number of rows in the dataset (for reporting).
    iterations
        Number of measured iterations.
    warmup
        Number of warmup iterations (not measured).

    Returns
    -------
    BenchmarkResult
        Aggregated performance metrics.

    """
    callable_fn = execute_fn  # type: ignore[assignment]

    # Warmup
    for _ in range(warmup):
        callable_fn()
        gc.collect()

    result = BenchmarkResult(
        query=query,
        dataset_rows=dataset_rows,
        iterations=iterations,
    )

    for _ in range(iterations):
        gc.collect()
        mem_before = _get_process_memory_mb()
        t_start = time.perf_counter()

        callable_fn()

        t_end = time.perf_counter()
        mem_after = _get_process_memory_mb()

        result.times_s.append(t_end - t_start)
        result.memory_deltas_mb.append(mem_after - mem_before)
        result.peak_memories_mb.append(max(mem_before, mem_after))

    return result
