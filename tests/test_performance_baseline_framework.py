"""Tests for the performance baseline framework components.

Covers:
- Profile helper: ProfileSummary data model, target name sanitization
- Workload characterization: benchmark classification, scale extraction,
  report generation from synthetic data
- Performance report: regression detection edge cases
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Profile helper tests
# ---------------------------------------------------------------------------


class TestProfileHelper:
    """Tests for tests/benchmarks/profile_helper.py."""

    def test_sanitize_target_name(self) -> None:
        from tests.benchmarks.profile_helper import _sanitize_target_name

        result = _sanitize_target_name(
            "tests/benchmarks/bench_core_operations.py::TestQueryBenchmarks1K::test_simple_scan"
        )
        assert "/" not in result
        assert "::" not in result
        assert ".py" not in result
        assert "test_simple_scan" in result

    def test_sanitize_simple_path(self) -> None:
        from tests.benchmarks.profile_helper import _sanitize_target_name

        result = _sanitize_target_name("tests/benchmarks/bench_core_operations.py")
        assert result == "tests_benchmarks_bench_core_operations"

    def test_profile_summary_dataclass(self) -> None:
        from tests.benchmarks.profile_helper import ProfileEntry, ProfileSummary

        entry = ProfileEntry(
            function="execute_query",
            ncalls=100,
            tottime=0.5,
            cumtime=1.2,
            filename="star.py",
            lineno=42,
        )
        summary = ProfileSummary(
            target="test_target",
            timestamp="20260406_120000",
            total_time_s=5.0,
            top_functions=[entry],
            prof_file="/tmp/test.prof",
            txt_file="/tmp/test.txt",
        )
        assert summary.total_time_s == 5.0
        assert len(summary.top_functions) == 1
        assert summary.top_functions[0].function == "execute_query"
        assert summary.top_functions[0].ncalls == 100


# ---------------------------------------------------------------------------
# Workload characterization tests
# ---------------------------------------------------------------------------


class TestWorkloadClassification:
    """Tests for workload category classification."""

    def test_classify_parser(self) -> None:
        from tests.benchmarks.workload_characterization import classify_benchmark

        assert classify_benchmark("test_parse_simple_match") == "parser"
        assert classify_benchmark("TestParserMicrobenchmarks::test_parse_complex") == "parser"

    def test_classify_scan(self) -> None:
        from tests.benchmarks.workload_characterization import classify_benchmark

        assert classify_benchmark("test_simple_scan") == "scan"

    def test_classify_filter(self) -> None:
        from tests.benchmarks.workload_characterization import classify_benchmark

        assert classify_benchmark("test_filtered_scan") == "filter"
        assert classify_benchmark("test_where_clause") == "filter"

    def test_classify_traversal(self) -> None:
        from tests.benchmarks.workload_characterization import classify_benchmark

        assert classify_benchmark("test_single_hop") == "traversal"
        assert classify_benchmark("test_relationship_join") == "traversal"

    def test_classify_aggregation(self) -> None:
        from tests.benchmarks.workload_characterization import classify_benchmark

        assert classify_benchmark("test_aggregation_count") == "aggregation"
        assert classify_benchmark("test_avg_salary") == "aggregation"

    def test_classify_optimizer(self) -> None:
        from tests.benchmarks.workload_characterization import classify_benchmark

        assert classify_benchmark("test_optimizer_pushdown") == "optimizer"
        assert classify_benchmark("test_pushdown_rule") == "optimizer"

    def test_classify_scalar(self) -> None:
        from tests.benchmarks.workload_characterization import classify_benchmark

        assert classify_benchmark("test_toupper_execution") == "scalar"
        assert classify_benchmark("test_scalar_registry") == "scalar"

    def test_classify_multi_type(self) -> None:
        from tests.benchmarks.workload_characterization import classify_benchmark

        assert classify_benchmark("test_multi_type_join") == "multi_type"
        assert classify_benchmark("test_company_location_join") == "multi_type"

    def test_classify_unknown(self) -> None:
        from tests.benchmarks.workload_characterization import classify_benchmark

        assert classify_benchmark("test_something_unique") == "other"


class TestScaleExtraction:
    """Tests for dataset scale extraction from benchmark names."""

    def test_extract_1k(self) -> None:
        from tests.benchmarks.workload_characterization import extract_scale

        assert extract_scale("TestQueryBenchmarks1K::test_scan") == "1K"
        assert extract_scale("test_1000_rows") == "1K"

    def test_extract_10k(self) -> None:
        from tests.benchmarks.workload_characterization import extract_scale

        assert extract_scale("TestQueryBenchmarks10K::test_scan") == "10K"
        assert extract_scale("test_10k_filtered") == "10K"

    def test_extract_100k(self) -> None:
        from tests.benchmarks.workload_characterization import extract_scale

        assert extract_scale("TestQueryBenchmarks100K::test_scan") == "100K"

    def test_extract_unknown(self) -> None:
        from tests.benchmarks.workload_characterization import extract_scale

        assert extract_scale("test_parse_simple") == "unknown"


class TestWorkloadReport:
    """Tests for workload report generation."""

    @pytest.fixture
    def synthetic_benchmark_file(self, tmp_path: Path) -> Path:
        """Create a synthetic pytest-benchmark JSON file."""
        data = {
            "machine_info": {"platform": "test"},
            "commit_info": {"id": "abc12345"},
            "benchmarks": [
                {
                    "name": "TestParserMicrobenchmarks::test_parse_simple_match",
                    "stats": {
                        "mean": 0.0001,
                        "stddev": 0.00001,
                        "min": 0.00008,
                        "max": 0.00015,
                        "rounds": 100,
                    },
                },
                {
                    "name": "TestQueryBenchmarks1K::test_simple_scan",
                    "stats": {
                        "mean": 0.01,
                        "stddev": 0.001,
                        "min": 0.008,
                        "max": 0.015,
                        "rounds": 50,
                    },
                },
                {
                    "name": "TestQueryBenchmarks10K::test_simple_scan",
                    "stats": {
                        "mean": 0.1,
                        "stddev": 0.01,
                        "min": 0.08,
                        "max": 0.15,
                        "rounds": 20,
                    },
                },
                {
                    "name": "TestQueryBenchmarks1K::test_filtered_scan",
                    "stats": {
                        "mean": 0.015,
                        "stddev": 0.002,
                        "min": 0.012,
                        "max": 0.02,
                        "rounds": 50,
                    },
                },
                {
                    "name": "TestQueryBenchmarks1K::test_single_hop",
                    "stats": {
                        "mean": 0.05,
                        "stddev": 0.005,
                        "min": 0.04,
                        "max": 0.07,
                        "rounds": 30,
                    },
                },
                {
                    "name": "TestQueryBenchmarks1K::test_aggregation_count",
                    "stats": {
                        "mean": 0.02,
                        "stddev": 0.003,
                        "min": 0.015,
                        "max": 0.03,
                        "rounds": 40,
                    },
                },
            ],
        }
        path = tmp_path / "benchmark.json"
        path.write_text(json.dumps(data))
        return path

    def test_characterize_workload(self, synthetic_benchmark_file: Path) -> None:
        from tests.benchmarks.workload_characterization import characterize_workload

        report = characterize_workload(synthetic_benchmark_file)
        assert report.total_benchmarks == 6
        assert len(report.categories) > 0

        # Verify categories are populated
        cat_names = {c.category for c in report.categories}
        assert "parser" in cat_names
        assert "scan" in cat_names

    def test_scaling_analysis(self, synthetic_benchmark_file: Path) -> None:
        from tests.benchmarks.workload_characterization import characterize_workload

        report = characterize_workload(synthetic_benchmark_file)
        # scan category has both 1K and 10K entries, so scaling should be detected
        scan_scaling = [s for s in report.scaling if s.category == "scan"]
        assert len(scan_scaling) == 1
        assert "1K" in scan_scaling[0].scales
        assert "10K" in scan_scaling[0].scales
        # 10K should be slower than 1K
        assert scan_scaling[0].scales["10K"] > scan_scaling[0].scales["1K"]

    def test_markdown_output(self, synthetic_benchmark_file: Path) -> None:
        from tests.benchmarks.workload_characterization import (
            characterize_workload,
            format_markdown,
        )

        report = characterize_workload(synthetic_benchmark_file)
        md = format_markdown(report)
        assert "# Workload Characterization Report" in md
        assert "Workload Categories" in md
        assert "Category Distribution" in md

    def test_json_output(self, synthetic_benchmark_file: Path) -> None:
        from tests.benchmarks.workload_characterization import (
            characterize_workload,
            format_json,
        )

        report = characterize_workload(synthetic_benchmark_file)
        j = format_json(report)
        parsed = json.loads(j)
        assert parsed["total_benchmarks"] == 6
        assert "categories" in parsed
        assert "entries" in parsed

    def test_empty_benchmark_file(self, tmp_path: Path) -> None:
        from tests.benchmarks.workload_characterization import characterize_workload

        path = tmp_path / "empty.json"
        path.write_text(json.dumps({"benchmarks": []}))
        report = characterize_workload(path)
        assert report.total_benchmarks == 0
        assert len(report.categories) == 0


# ---------------------------------------------------------------------------
# Performance report regression detection tests
# ---------------------------------------------------------------------------


class TestPerformanceReportEdgeCases:
    """Edge case tests for performance_report.py."""

    @pytest.fixture
    def baseline_file(self, tmp_path: Path) -> Path:
        data = {
            "commit_info": {"id": "baseline123"},
            "benchmarks": [
                {
                    "name": "test_scan_1k",
                    "stats": {"mean": 0.01, "stddev": 0.001, "rounds": 50, "min": 0.008, "max": 0.015, "median": 0.01},
                },
                {
                    "name": "test_hop_1k",
                    "stats": {"mean": 0.05, "stddev": 0.005, "rounds": 30, "min": 0.04, "max": 0.07, "median": 0.05},
                },
            ],
        }
        path = tmp_path / "baseline.json"
        path.write_text(json.dumps(data))
        return path

    def test_no_regression(self, baseline_file: Path, tmp_path: Path) -> None:
        from tests.benchmarks.performance_report import compare_benchmarks

        current = {
            "commit_info": {"id": "current456"},
            "benchmarks": [
                {"name": "test_scan_1k", "stats": {"mean": 0.0102, "stddev": 0.001, "rounds": 50, "min": 0.008, "max": 0.015, "median": 0.01}},
                {"name": "test_hop_1k", "stats": {"mean": 0.05, "stddev": 0.005, "rounds": 30, "min": 0.04, "max": 0.07, "median": 0.05}},
            ],
        }
        current_path = tmp_path / "current.json"
        current_path.write_text(json.dumps(current))

        report = compare_benchmarks(baseline_file, current_path, threshold_pct=5.0)
        assert report.overall_status == "pass"
        assert len(report.regressions) == 0

    def test_regression_detected(self, baseline_file: Path, tmp_path: Path) -> None:
        from tests.benchmarks.performance_report import compare_benchmarks

        current = {
            "commit_info": {"id": "regress789"},
            "benchmarks": [
                {"name": "test_scan_1k", "stats": {"mean": 0.02, "stddev": 0.002, "rounds": 50, "min": 0.015, "max": 0.03, "median": 0.02}},
                {"name": "test_hop_1k", "stats": {"mean": 0.05, "stddev": 0.005, "rounds": 30, "min": 0.04, "max": 0.07, "median": 0.05}},
            ],
        }
        current_path = tmp_path / "current.json"
        current_path.write_text(json.dumps(current))

        report = compare_benchmarks(baseline_file, current_path, threshold_pct=5.0)
        assert report.overall_status == "fail"
        assert len(report.regressions) == 1
        assert report.regressions[0].name == "test_scan_1k"
        assert report.regressions[0].change_pct > 5.0

    def test_new_benchmark_detected(self, baseline_file: Path, tmp_path: Path) -> None:
        from tests.benchmarks.performance_report import compare_benchmarks

        current = {
            "commit_info": {"id": "new_bench"},
            "benchmarks": [
                {"name": "test_scan_1k", "stats": {"mean": 0.01, "stddev": 0.001, "rounds": 50, "min": 0.008, "max": 0.015, "median": 0.01}},
                {"name": "test_hop_1k", "stats": {"mean": 0.05, "stddev": 0.005, "rounds": 30, "min": 0.04, "max": 0.07, "median": 0.05}},
                {"name": "test_new_feature", "stats": {"mean": 0.03, "stddev": 0.003, "rounds": 40, "min": 0.025, "max": 0.04, "median": 0.03}},
            ],
        }
        current_path = tmp_path / "current.json"
        current_path.write_text(json.dumps(current))

        report = compare_benchmarks(baseline_file, current_path, threshold_pct=5.0)
        assert "test_new_feature" in report.new_benchmarks

    def test_removed_benchmark_detected(self, baseline_file: Path, tmp_path: Path) -> None:
        from tests.benchmarks.performance_report import compare_benchmarks

        current = {
            "commit_info": {"id": "removed"},
            "benchmarks": [
                {"name": "test_scan_1k", "stats": {"mean": 0.01, "stddev": 0.001, "rounds": 50, "min": 0.008, "max": 0.015, "median": 0.01}},
            ],
        }
        current_path = tmp_path / "current.json"
        current_path.write_text(json.dumps(current))

        report = compare_benchmarks(baseline_file, current_path, threshold_pct=5.0)
        assert "test_hop_1k" in report.removed_benchmarks


# ---------------------------------------------------------------------------
# _perf_helpers tests
# ---------------------------------------------------------------------------


class TestPerfHelpers:
    """Tests for the CI-aware threshold helper."""

    def test_local_threshold_unchanged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """On local machines, threshold should pass through unchanged."""
        monkeypatch.delenv("CI", raising=False)
        monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)

        # Re-import to pick up env changes
        import importlib
        import _perf_helpers
        importlib.reload(_perf_helpers)

        assert _perf_helpers.perf_threshold(0.5) == 0.5
        assert _perf_helpers.perf_threshold(1.0) == 1.0

    def test_ci_threshold_scaled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """In CI, threshold should be multiplied by CI multiplier."""
        monkeypatch.setenv("CI", "true")
        monkeypatch.setenv("PYCYPHER_PERF_MULTIPLIER", "3.0")
        monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)

        import importlib
        import _perf_helpers
        importlib.reload(_perf_helpers)

        assert _perf_helpers.perf_threshold(0.5) == 1.5
        assert _perf_helpers.perf_threshold(1.0) == 3.0

    def test_xdist_threshold_scaled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Under xdist, threshold should use xdist multiplier."""
        monkeypatch.delenv("CI", raising=False)
        monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
        monkeypatch.setenv("PYCYPHER_XDIST_PERF_MULTIPLIER", "2.0")

        import importlib
        import _perf_helpers
        importlib.reload(_perf_helpers)

        assert _perf_helpers.perf_threshold(0.5) == 1.0
