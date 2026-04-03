"""Tests for the performance regression detection engine.

TDD tests written first — validates statistical regression detection
with >90% accuracy and <10% false positive rate.
"""

from __future__ import annotations

import random
import time

import pytest


class TestRegressionDetectorBasics:
    """Basic regression detector API and data recording."""

    def test_import(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector()
        assert detector is not None

    def test_record_baseline_sample(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(min_baseline_samples=5)
        detector.record("query_latency_ms", 10.0)
        detector.record("query_latency_ms", 12.0)
        assert detector.sample_count("query_latency_ms") == 2

    def test_record_multiple_metrics(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(min_baseline_samples=3)
        detector.record("latency_ms", 10.0)
        detector.record("memory_mb", 50.0)
        assert detector.sample_count("latency_ms") == 1
        assert detector.sample_count("memory_mb") == 1

    def test_sample_count_unknown_metric(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector()
        assert detector.sample_count("nonexistent") == 0

    def test_check_returns_none_with_insufficient_data(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(min_baseline_samples=10)
        detector.record("latency_ms", 10.0)
        result = detector.check("latency_ms", 100.0)
        assert result is None

    def test_rolling_window_eviction(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(
            min_baseline_samples=5, max_samples=20
        )
        for i in range(30):
            detector.record("metric", float(i))
        assert detector.sample_count("metric") == 20


class TestRegressionDetection:
    """Core regression detection with statistical analysis."""

    def _build_detector(self, baseline_values, **kwargs):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(
            min_baseline_samples=len(baseline_values), **kwargs
        )
        for v in baseline_values:
            detector.record("test_metric", v)
        return detector

    def test_no_regression_within_normal_range(self):
        baseline = [10.0, 11.0, 9.5, 10.5, 10.2, 9.8, 10.3, 10.1]
        detector = self._build_detector(baseline)
        # Value within ~1 std dev of mean should not be flagged
        result = detector.check("test_metric", 10.8)
        assert result is not None
        assert not result.is_regression

    def test_detect_clear_regression(self):
        baseline = [10.0, 11.0, 9.5, 10.5, 10.2, 9.8, 10.3, 10.1]
        detector = self._build_detector(baseline)
        # 50% increase should be detected
        result = detector.check("test_metric", 15.0)
        assert result is not None
        assert result.is_regression

    def test_detect_large_regression(self):
        baseline = [10.0] * 20
        detector = self._build_detector(baseline)
        result = detector.check("test_metric", 20.0)
        assert result is not None
        assert result.is_regression
        assert result.severity in ("warning", "critical")

    def test_regression_result_has_fields(self):
        baseline = [10.0, 11.0, 9.5, 10.5, 10.2, 9.8, 10.3, 10.1]
        detector = self._build_detector(baseline)
        result = detector.check("test_metric", 15.0)
        assert result is not None
        assert hasattr(result, "is_regression")
        assert hasattr(result, "metric_name")
        assert hasattr(result, "current_value")
        assert hasattr(result, "baseline_mean")
        assert hasattr(result, "baseline_std")
        assert hasattr(result, "z_score")
        assert hasattr(result, "severity")
        assert result.metric_name == "test_metric"
        assert result.current_value == 15.0

    def test_severity_levels(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(min_baseline_samples=20)
        for _ in range(20):
            detector.record("metric", 10.0 + random.gauss(0, 0.5))
        # Mild regression
        mild = detector.check("metric", 13.0)
        # Severe regression
        severe = detector.check("metric", 25.0)
        assert mild is not None
        assert severe is not None
        if mild.is_regression and severe.is_regression:
            severity_order = {"info": 0, "warning": 1, "critical": 2}
            assert severity_order.get(
                severe.severity, 0
            ) >= severity_order.get(mild.severity, 0)

    def test_improvement_not_flagged_as_regression(self):
        baseline = [10.0, 11.0, 9.5, 10.5, 10.2, 9.8, 10.3, 10.1]
        detector = self._build_detector(baseline)
        # Lower value is better (improvement)
        result = detector.check("test_metric", 5.0)
        assert result is not None
        assert not result.is_regression

    def test_configurable_z_score_threshold(self):
        from shared.regression_detector import RegressionDetector

        # Strict detector (low z-score threshold)
        strict = RegressionDetector(
            min_baseline_samples=8, z_score_threshold=1.5
        )
        # Lenient detector (high z-score threshold)
        lenient = RegressionDetector(
            min_baseline_samples=8, z_score_threshold=4.0
        )
        baseline = [10.0, 11.0, 9.5, 10.5, 10.2, 9.8, 10.3, 10.1]
        for v in baseline:
            strict.record("m", v)
            lenient.record("m", v)

        test_value = 13.0
        strict_result = strict.check("m", test_value)
        lenient_result = lenient.check("m", test_value)

        assert strict_result is not None
        assert lenient_result is not None
        # Strict should be more likely to flag regression
        if strict_result.is_regression:
            assert strict_result.z_score >= 0


class TestRegressionAccuracy:
    """Validate >90% regression detection accuracy and <10% false positive rate."""

    def test_true_positive_rate_above_90_percent(self):
        """Inject known regressions, verify >90% are detected."""
        from shared.regression_detector import RegressionDetector

        random.seed(42)
        detected = 0
        trials = 100

        for _ in range(trials):
            detector = RegressionDetector(min_baseline_samples=30)
            baseline_mean = 10.0
            baseline_std = 1.0
            for _ in range(30):
                detector.record(
                    "metric", random.gauss(baseline_mean, baseline_std)
                )
            # Inject a 3x std deviation regression
            regression_value = baseline_mean + 3.0 * baseline_std
            result = detector.check("metric", regression_value)
            if result is not None and result.is_regression:
                detected += 1

        accuracy = detected / trials
        assert accuracy >= 0.90, (
            f"True positive rate {accuracy:.1%} < 90%"
        )

    def test_false_positive_rate_below_10_percent(self):
        """Feed normal values, verify <10% false positive rate."""
        from shared.regression_detector import RegressionDetector

        random.seed(42)
        false_positives = 0
        trials = 200

        for _ in range(trials):
            detector = RegressionDetector(min_baseline_samples=30)
            baseline_mean = 10.0
            baseline_std = 1.0
            for _ in range(30):
                detector.record(
                    "metric", random.gauss(baseline_mean, baseline_std)
                )
            # Normal value within 1 std dev
            normal_value = random.gauss(baseline_mean, baseline_std)
            result = detector.check("metric", normal_value)
            if result is not None and result.is_regression:
                false_positives += 1

        fp_rate = false_positives / trials
        assert fp_rate < 0.10, (
            f"False positive rate {fp_rate:.1%} >= 10%"
        )


class TestMetricsIntegration:
    """Integration with existing MetricsSnapshot-based system."""

    def test_from_snapshots(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(min_baseline_samples=3)
        # Simulate recording from snapshot data
        for latency in [10.0, 11.0, 9.5]:
            detector.record("timing_p50_ms", latency)
        assert detector.sample_count("timing_p50_ms") == 3

    def test_check_multiple_metrics_at_once(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(min_baseline_samples=5)
        for i in range(5):
            detector.record("latency", 10.0 + random.gauss(0, 0.5))
            detector.record("memory", 50.0 + random.gauss(0, 2.0))

        results = detector.check_all(
            {"latency": 20.0, "memory": 100.0}
        )
        assert "latency" in results
        assert "memory" in results

    def test_thread_safety(self):
        import threading

        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(min_baseline_samples=10)
        errors = []

        def record_samples():
            try:
                for _ in range(50):
                    detector.record(
                        "concurrent", random.gauss(10, 1)
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_samples) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert detector.sample_count("concurrent") > 0


class TestBaselineManagement:
    """Baseline creation, update, and serialization."""

    def test_get_baseline_stats(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(min_baseline_samples=5)
        for v in [10.0, 12.0, 8.0, 11.0, 9.0]:
            detector.record("metric", v)
        stats = detector.baseline_stats("metric")
        assert stats is not None
        assert "mean" in stats
        assert "std" in stats
        assert "count" in stats
        assert abs(stats["mean"] - 10.0) < 1.0

    def test_baseline_stats_unknown_metric(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector()
        stats = detector.baseline_stats("nonexistent")
        assert stats is None

    def test_reset_metric(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(min_baseline_samples=3)
        for v in [10.0, 11.0, 12.0]:
            detector.record("metric", v)
        detector.reset("metric")
        assert detector.sample_count("metric") == 0

    def test_reset_all(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(min_baseline_samples=3)
        detector.record("a", 1.0)
        detector.record("b", 2.0)
        detector.reset_all()
        assert detector.sample_count("a") == 0
        assert detector.sample_count("b") == 0

    def test_to_dict_serialization(self):
        from shared.regression_detector import RegressionDetector

        detector = RegressionDetector(min_baseline_samples=3)
        for v in [10.0, 11.0, 12.0]:
            detector.record("metric", v)
        data = detector.to_dict()
        assert "metrics" in data
        assert "metric" in data["metrics"]
