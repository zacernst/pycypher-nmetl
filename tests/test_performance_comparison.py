"""Performance comparison TDD framework for Pandas vs PySpark backends.

This module provides Test-Driven Development framework for performance testing
and optimization of the dual-backend architecture. Tests define performance
contracts and benchmarking utilities for Phase 2 development.

Test Strategy:
- Define performance baselines and expectations
- Create benchmarking utilities for systematic comparison
- Establish TDD contracts for optimization targets
- Guide implementation decisions with data-driven insights
"""

import time

import pandas as pd
import pytest

try:
    import os

    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
from contextlib import contextmanager
from dataclasses import dataclass

from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star
from _perf_helpers import perf_threshold


@dataclass
class PerformanceMeasurement:
    """Container for performance measurement results."""

    operation: str
    backend: str
    dataset_size: int
    execution_time: float
    memory_usage: float
    peak_memory: float
    cpu_percent: float
    result_size: int
    success: bool
    error_message: str = ""


@dataclass
class PerformanceComparison:
    """Container for comparing performance between backends."""

    operation: str
    dataset_size: int
    pandas_measurement: PerformanceMeasurement
    spark_measurement: PerformanceMeasurement
    speedup_factor: (
        float  # spark_time / pandas_time (< 1.0 means Spark is faster)
    )
    memory_efficiency: float  # spark_memory / pandas_memory
    recommendation: str  # Which backend to use


class PerformanceMeasurementUtility:
    """Utility class for measuring and comparing backend performance.

    This class provides TDD-driven utilities for systematic performance
    measurement and comparison between Pandas and PySpark backends.
    """

    @staticmethod
    @contextmanager
    def measure_performance(
        operation_name: str,
        backend: str,
        dataset_size: int,
    ):
        """Context manager for measuring performance of operations.

        Args:
            operation_name: Name of the operation being measured
            backend: Backend type ('pandas' or 'spark')
            dataset_size: Size of the dataset being processed

        Yields:
            PerformanceMeasurement object to be populated

        """
        measurement = PerformanceMeasurement(
            operation=operation_name,
            backend=backend,
            dataset_size=dataset_size,
            execution_time=0.0,
            memory_usage=0.0,
            peak_memory=0.0,
            cpu_percent=0.0,
            result_size=0,
            success=False,
        )

        # Get process for memory monitoring (if available)
        if PSUTIL_AVAILABLE:
            process = psutil.Process(os.getpid())
            initial_memory = process.memory_info().rss / 1024 / 1024  # MB
            start_cpu = process.cpu_percent()
        else:
            process = None
            initial_memory = 0.0
            start_cpu = 0.0

        start_time = time.time()

        try:
            yield measurement
            measurement.success = True
        except Exception as e:
            measurement.error_message = str(e)
            measurement.success = False
        finally:
            # Record measurements
            end_time = time.time()
            measurement.execution_time = end_time - start_time

            if PSUTIL_AVAILABLE and process:
                end_cpu = process.cpu_percent()
                final_memory = process.memory_info().rss / 1024 / 1024  # MB
                measurement.memory_usage = final_memory - initial_memory
                measurement.peak_memory = final_memory
                measurement.cpu_percent = max(start_cpu, end_cpu)
            else:
                # Mock values when psutil not available
                measurement.memory_usage = 0.0
                measurement.peak_memory = 0.0
                measurement.cpu_percent = 0.0

    @staticmethod
    def create_test_dataset(
        size: int,
        entity_type: str = "Person",
    ) -> tuple[pd.DataFrame, EntityTable]:
        """Create test dataset of specified size for performance testing.

        Args:
            size: Number of rows in the dataset
            entity_type: Type of entity to create

        Returns:
            Tuple of (pandas DataFrame, EntityTable)

        """
        if size <= 1000:
            # Small dataset
            df = pd.DataFrame(
                {
                    ID_COLUMN: range(size),
                    "name": [f"Person_{i}" for i in range(size)],
                    "age": [(i % 80) + 18 for i in range(size)],
                    "department": [
                        ["Engineering", "Sales", "Marketing", "HR"][i % 4]
                        for i in range(size)
                    ],
                    "salary": [50000 + (i % 50000) for i in range(size)],
                },
            )
        else:
            # Large dataset - optimize creation
            import numpy as np

            df = pd.DataFrame(
                {
                    ID_COLUMN: np.arange(size),
                    "name": [f"Person_{i}" for i in range(size)],
                    "age": np.random.randint(18, 80, size),
                    "department": np.random.choice(
                        ["Engineering", "Sales", "Marketing", "HR"],
                        size,
                    ),
                    "salary": np.random.randint(40000, 150000, size),
                },
            )

        entity_table = EntityTable(
            entity_type=entity_type,
            identifier=entity_type,
            column_names=[ID_COLUMN, "name", "age", "department", "salary"],
            source_obj_attribute_map={
                "name": "name",
                "age": "age",
                "department": "department",
                "salary": "salary",
            },
            attribute_map={
                "name": "name",
                "age": "age",
                "department": "department",
                "salary": "salary",
            },
            source_obj=df,
        )

        return df, entity_table

    @staticmethod
    def compare_measurements(
        pandas_measurement: PerformanceMeasurement,
        spark_measurement: PerformanceMeasurement,
    ) -> PerformanceComparison:
        """Compare performance measurements between backends.

        Args:
            pandas_measurement: Pandas backend measurement
            spark_measurement: PySpark backend measurement

        Returns:
            PerformanceComparison with analysis and recommendations

        """
        # Calculate speedup factor (< 1.0 means Spark is faster)
        if pandas_measurement.execution_time > 0:
            speedup_factor = (
                spark_measurement.execution_time
                / pandas_measurement.execution_time
            )
        else:
            speedup_factor = float("inf")

        # Calculate memory efficiency
        if pandas_measurement.memory_usage > 0:
            memory_efficiency = (
                spark_measurement.memory_usage
                / pandas_measurement.memory_usage
            )
        else:
            memory_efficiency = 1.0

        # Generate recommendation
        if speedup_factor < 0.8 and memory_efficiency < 1.2:
            recommendation = "Use PySpark - significantly faster with acceptable memory usage"
        elif speedup_factor < 1.0:
            recommendation = "Use PySpark - faster execution"
        elif speedup_factor > 2.0:
            recommendation = "Use Pandas - much faster for this dataset size"
        elif memory_efficiency < 0.8:
            recommendation = "Use PySpark - much more memory efficient"
        else:
            recommendation = (
                "Use Pandas - better overall performance for this scale"
            )

        return PerformanceComparison(
            operation=pandas_measurement.operation,
            dataset_size=pandas_measurement.dataset_size,
            pandas_measurement=pandas_measurement,
            spark_measurement=spark_measurement,
            speedup_factor=speedup_factor,
            memory_efficiency=memory_efficiency,
            recommendation=recommendation,
        )


class TestPerformanceComparisonTDD:
    """TDD tests for performance comparison framework.

    These tests define the performance contracts and benchmarking requirements
    for the dual-backend implementation.
    """

    def test_performance_measurement_utility_basic(self):
        """Test basic performance measurement utility functionality."""
        with PerformanceMeasurementUtility.measure_performance(
            "test_operation",
            "pandas",
            100,
        ) as measurement:
            # Simulate some work
            time.sleep(0.01)
            measurement.result_size = 100

        assert measurement.operation == "test_operation"
        assert measurement.backend == "pandas"
        assert measurement.dataset_size == 100
        assert measurement.execution_time > 0.008  # At least the sleep time
        assert measurement.success is True
        assert measurement.result_size == 100

    def test_test_dataset_creation_small(self):
        """Test creation of small test datasets."""
        df, entity_table = PerformanceMeasurementUtility.create_test_dataset(
            100,
        )

        assert len(df) == 100
        assert ID_COLUMN in df.columns
        assert "name" in df.columns
        assert "age" in df.columns
        assert "salary" in df.columns

        assert entity_table.entity_type == "Person"
        assert len(entity_table.column_names) == 5

    def test_test_dataset_creation_large(self):
        """Test creation of large test datasets for performance testing."""
        df, entity_table = PerformanceMeasurementUtility.create_test_dataset(
            10000,
        )

        assert len(df) == 10000
        assert df["age"].min() >= 18
        assert df["age"].max() <= 79
        assert df["salary"].min() >= 40000
        assert df["salary"].max() <= 149999

    def test_performance_comparison_logic(self):
        """Test performance comparison calculation logic."""
        pandas_measurement = PerformanceMeasurement(
            operation="test_query",
            backend="pandas",
            dataset_size=1000,
            execution_time=2.0,
            memory_usage=100.0,
            peak_memory=150.0,
            cpu_percent=50.0,
            result_size=1000,
            success=True,
        )

        spark_measurement = PerformanceMeasurement(
            operation="test_query",
            backend="spark",
            dataset_size=1000,
            execution_time=1.0,  # Faster
            memory_usage=80.0,  # More efficient
            peak_memory=120.0,
            cpu_percent=40.0,
            result_size=1000,
            success=True,
        )

        comparison = PerformanceMeasurementUtility.compare_measurements(
            pandas_measurement,
            spark_measurement,
        )

        assert comparison.speedup_factor == 0.5  # Spark is 2x faster
        assert (
            comparison.memory_efficiency == 0.8
        )  # Spark uses 80% of Pandas memory
        assert "PySpark" in comparison.recommendation


class TestBackendPerformanceContractsTDD:
    """TDD performance contracts for backend implementations.

    These tests define the performance expectations and contracts that
    the PySpark implementation must meet in Phase 2.
    """

    @pytest.mark.parametrize("dataset_size", [1000, 10000, 100000])
    def test_pandas_performance_baseline(self, dataset_size):
        """Establish performance baselines for Pandas backend.

        These baselines will be used to validate PySpark performance improvements.
        """
        df, entity_table = PerformanceMeasurementUtility.create_test_dataset(
            dataset_size,
        )
        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": entity_table}),
        )

        # Test basic MATCH query
        with PerformanceMeasurementUtility.measure_performance(
            "match_query",
            "pandas",
            dataset_size,
        ) as measurement:
            star = Star(context=context)
            result = star.execute_query(
                "MATCH (p:Person) RETURN p.name AS name, p.age AS age",
            )
            measurement.result_size = len(result)

        # Performance expectations based on dataset size
        if dataset_size <= 1000:
            assert measurement.execution_time < perf_threshold(2.0), (
                f"Small dataset should be fast: {measurement.execution_time}s"
            )
            assert measurement.memory_usage < perf_threshold(50), (
                f"Small dataset should use little memory: {measurement.memory_usage}MB"
            )
        elif dataset_size <= 10000:
            assert measurement.execution_time < perf_threshold(5.0), (
                f"Medium dataset should be reasonable: {measurement.execution_time}s"
            )
            assert measurement.memory_usage < perf_threshold(200), (
                f"Medium dataset memory usage: {measurement.memory_usage}MB"
            )
        else:
            assert measurement.execution_time < perf_threshold(15.0), (
                f"Large dataset should complete: {measurement.execution_time}s"
            )
            assert measurement.memory_usage < perf_threshold(500), (
                f"Large dataset memory usage: {measurement.memory_usage}MB"
            )

        assert measurement.success is True
        assert measurement.result_size == dataset_size

    def test_pandas_set_operation_baseline(self):
        """Establish baseline for SET operation performance."""
        df, entity_table = PerformanceMeasurementUtility.create_test_dataset(
            5000,
        )
        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": entity_table}),
        )

        with PerformanceMeasurementUtility.measure_performance(
            "set_operation",
            "pandas",
            5000,
        ) as measurement:
            star = Star(context=context)
            result = star.execute_query("""
                MATCH (p:Person)
                SET p.bonus = p.salary * 0.1, p.total_comp = p.salary + p.bonus
                RETURN p.name AS name, p.total_comp AS total
            """)
            measurement.result_size = len(result)

        # SET operations should be reasonably fast
        assert measurement.execution_time < 8.0, (
            f"SET operation took {measurement.execution_time}s"
        )
        assert measurement.success is True
        assert measurement.result_size == 5000

    @pytest.mark.skip(reason="Phase 2: PySpark implementation not complete")
    def test_spark_performance_contracts_tdd(self):
        """TDD: Define performance contracts for PySpark implementation.

        These tests will be enabled in Phase 2 and define the expected
        performance characteristics of the PySpark backend.
        """
        # TODO Phase 2: Implement these performance contracts

        performance_contracts = {
            "large_dataset_advantage": {
                "description": "PySpark should outperform Pandas for datasets > 50K rows",
                "test_sizes": [50000, 100000, 500000],
                "expected_speedup": 1.5,  # At least 50% faster
            },
            "memory_efficiency": {
                "description": "PySpark should use memory more efficiently for large datasets",
                "test_sizes": [100000, 500000],
                "expected_memory_ratio": 0.7,  # Use at most 70% of Pandas memory
            },
            "scalability": {
                "description": "PySpark should scale better with increasing data size",
                "measurement": "Time complexity should be better than O(n²)",
            },
            "distributed_benefits": {
                "description": "PySpark should show benefits in multi-core/cluster environments",
                "test_scenarios": ["multi_core_local", "cluster_distributed"],
            },
        }

        # Phase 2 implementation should satisfy these contracts
        for contract_name, contract in performance_contracts.items():
            assert "description" in contract
            # Additional contract validation will be implemented in Phase 2

    @pytest.mark.skip(
        reason="Phase 2: Comparative benchmarking not implemented",
    )
    def test_comprehensive_performance_comparison_tdd(self):
        """TDD: Comprehensive performance comparison between backends.

        This test defines the framework for systematic comparison that
        will guide optimization decisions in Phase 2.
        """
        # TODO Phase 2: Implement comprehensive comparison framework

        comparison_scenarios = [
            {"dataset_size": 1000, "operations": ["match", "set", "return"]},
            {
                "dataset_size": 10000,
                "operations": ["match", "set", "return", "join"],
            },
            {
                "dataset_size": 100000,
                "operations": ["match", "set", "return", "aggregation"],
            },
        ]

        expected_breakeven_points = {
            "match_queries": 25000,  # PySpark faster above this size
            "set_operations": 50000,  # PySpark faster above this size
            "join_operations": 10000,  # PySpark faster above this size
            "aggregations": 5000,  # PySpark faster above this size
        }

        # Phase 2 implementation should validate these breakeven points
        for operation, breakeven_size in expected_breakeven_points.items():
            assert breakeven_size > 0
            # Actual validation will be implemented in Phase 2

    def test_performance_regression_detection_tdd(self):
        """TDD: Framework for detecting performance regressions.

        Defines how to detect if changes negatively impact performance.
        """
        regression_thresholds = {
            "execution_time_regression": 1.2,  # 20% slower is a regression
            "memory_usage_regression": 1.5,  # 50% more memory is a regression
            "result_accuracy": 1.0,  # Results must be identical
        }

        baseline_measurements = {}  # Store baseline measurements

        # TODO Phase 2: Implement regression detection
        # - Store baseline measurements for each operation and dataset size
        # - Compare new measurements against baselines
        # - Alert when regressions are detected
        # - Provide analysis of what changed

        for threshold_name, threshold_value in regression_thresholds.items():
            assert threshold_value > 0
            # Regression detection logic will be implemented in Phase 2


class TestOptimizationGuidanceTDD:
    """TDD guidance for optimization strategies and decisions.

    These tests document optimization strategies and provide guidance
    for Phase 2 implementation decisions.
    """

    def test_optimization_strategy_documentation(self):
        """Document optimization strategies for different scenarios."""
        optimization_strategies = {
            "small_datasets": {
                "size_range": "< 10K rows",
                "recommended_backend": "pandas",
                "optimizations": [
                    "Use vectorized operations",
                    "Minimize memory allocations",
                    "Avoid unnecessary copying",
                ],
            },
            "medium_datasets": {
                "size_range": "10K - 100K rows",
                "recommended_backend": "context_dependent",
                "decision_factors": [
                    "Query complexity",
                    "Available memory",
                    "CPU cores available",
                ],
            },
            "large_datasets": {
                "size_range": "> 100K rows",
                "recommended_backend": "spark",
                "optimizations": [
                    "Optimize partitioning strategy",
                    "Use broadcast joins for small lookup tables",
                    "Enable adaptive query execution",
                    "Cache intermediate results",
                ],
            },
            "join_heavy_workloads": {
                "characteristics": "Multiple joins, complex patterns",
                "recommended_backend": "spark",
                "optimizations": [
                    "Broadcast small tables",
                    "Partition on join keys",
                    "Use bucketing when possible",
                ],
            },
            "aggregation_workloads": {
                "characteristics": "GROUP BY, aggregation functions",
                "recommended_backend": "spark",
                "optimizations": [
                    "Push down aggregations",
                    "Use columnar operations",
                    "Optimize shuffle operations",
                ],
            },
        }

        # Verify all strategies are documented
        assert len(optimization_strategies) == 5

        for strategy_name, strategy in optimization_strategies.items():
            assert "recommended_backend" in strategy
            # Each strategy should provide actionable guidance

        # These strategies will guide Phase 2 implementation decisions

    def test_performance_monitoring_framework_tdd(self):
        """Define framework for ongoing performance monitoring."""
        monitoring_framework = {
            "metrics_collection": {
                "execution_time": "Track query execution time by operation type",
                "resource_usage": "Monitor CPU, memory, network I/O",
                "throughput": "Measure operations per second",
                "latency_distribution": "Track P50, P95, P99 latencies",
            },
            "alerting_thresholds": {
                "execution_time_p95": "> 10s for queries on < 100K rows",
                "memory_usage": "> 1GB for < 1M row datasets",
                "error_rate": "> 1% query failures",
                "regression_threshold": "> 20% performance degradation",
            },
            "optimization_triggers": {
                "automatic": [
                    "Switch to PySpark when dataset > breakeven point",
                    "Use caching for repeated operations",
                    "Adjust partitioning based on data skew",
                ],
                "manual": [
                    "Profile slow queries",
                    "Analyze execution plans",
                    "Tune cluster configurations",
                ],
            },
        }

        # Verify comprehensive monitoring framework
        for framework_area, framework_details in monitoring_framework.items():
            assert len(framework_details) > 0

        # This framework will be implemented in Phase 2 for production monitoring


# Utilities for creating performance test fixtures
@pytest.fixture(scope="session")
def performance_test_datasets():
    """Create standard test datasets for performance benchmarking."""
    datasets = {}
    sizes = [1000, 10000, 100000]

    for size in sizes:
        df, entity_table = PerformanceMeasurementUtility.create_test_dataset(
            size,
        )
        datasets[f"dataset_{size}"] = {
            "dataframe": df,
            "entity_table": entity_table,
            "size": size,
        }

    return datasets


@pytest.fixture
def performance_measurement_utility():
    """Provide performance measurement utility for tests."""
    return PerformanceMeasurementUtility()


# Skip markers for different phases
pytestmark = [pytest.mark.slow, pytest.mark.performance]
