"""Tests for fastopendata.analytics — metrics collection and analysis engine."""

from __future__ import annotations

import time

import pytest
from fastopendata.analytics.collector import (
    MetricsCollector,
    QueryMetric,
    QueryStatus,
)
from fastopendata.analytics.engine import AnalyticsEngine, LatencyBreakdown

# ── MetricsCollector ─────────────────────────────────────────────────


class TestMetricsCollector:
    def test_empty_collector(self) -> None:
        c = MetricsCollector()
        assert c.total_queries == 0
        assert c.total_errors == 0
        assert c.error_rate == 0.0
        assert c.recent() == []

    def test_record_success(self) -> None:
        c = MetricsCollector()
        m = c.record_success("MATCH (n) RETURN n", total_ms=42.5, row_count=10)
        assert c.total_queries == 1
        assert c.total_errors == 0
        assert m.status == QueryStatus.SUCCESS
        assert m.total_ms == 42.5
        assert m.row_count == 10

    def test_record_error(self) -> None:
        c = MetricsCollector()
        m = c.record_error(
            "BAD QUERY", total_ms=5.0, error_message="parse failure"
        )
        assert c.total_queries == 1
        assert c.total_errors == 1
        assert c.error_rate == 1.0
        assert m.status == QueryStatus.ERROR
        assert m.error_message == "parse failure"

    def test_record_timeout(self) -> None:
        c = MetricsCollector()
        m = c.record_error(
            "SLOW QUERY",
            total_ms=30000.0,
            error_message="timed out",
            status=QueryStatus.TIMEOUT,
        )
        assert m.status == QueryStatus.TIMEOUT
        assert c.total_errors == 1

    def test_recent_ordering(self) -> None:
        c = MetricsCollector()
        c.record_success("Q1", total_ms=10.0)
        c.record_success("Q2", total_ms=20.0)
        c.record_success("Q3", total_ms=30.0)
        recent = c.recent(2)
        assert len(recent) == 2
        # Newest first
        assert recent[0].query_text == "Q3"
        assert recent[1].query_text == "Q2"

    def test_max_history_ring_buffer(self) -> None:
        c = MetricsCollector(max_history=5)
        for i in range(10):
            c.record_success(f"Q{i}", total_ms=float(i))
        assert c.total_queries == 10  # Counter tracks all
        assert len(c.all_metrics()) == 5  # Ring buffer capped
        # Oldest retained should be Q5
        assert c.all_metrics()[0].query_text == "Q5"

    def test_clear(self) -> None:
        c = MetricsCollector()
        c.record_success("Q1", total_ms=10.0)
        c.clear()
        assert c.total_queries == 0
        assert c.total_errors == 0
        assert c.all_metrics() == []

    def test_queries_per_second(self) -> None:
        c = MetricsCollector()
        c.record_success("Q1", total_ms=1.0)
        # Should be > 0 since at least one query recorded
        assert c.queries_per_second > 0

    def test_error_rate_mixed(self) -> None:
        c = MetricsCollector()
        c.record_success("Q1", total_ms=10.0)
        c.record_success("Q2", total_ms=20.0)
        c.record_error("Q3", total_ms=5.0, error_message="fail")
        assert c.error_rate == pytest.approx(1 / 3, abs=0.01)

    def test_record_with_timing_breakdown(self) -> None:
        c = MetricsCollector()
        m = c.record_success(
            "MATCH (n) RETURN n",
            total_ms=100.0,
            parse_ms=10.0,
            plan_ms=20.0,
            exec_ms=70.0,
        )
        assert m.parse_ms == 10.0
        assert m.plan_ms == 20.0
        assert m.exec_ms == 70.0

    def test_record_with_metadata(self) -> None:
        c = MetricsCollector()
        m = c.record_success(
            "Q1",
            total_ms=10.0,
            metadata={"plan": "scan"},
        )
        assert m.metadata == {"plan": "scan"}

    def test_new_query_id(self) -> None:
        id1 = MetricsCollector.new_query_id()
        id2 = MetricsCollector.new_query_id()
        assert len(id1) == 12
        assert id1 != id2


# ── QueryMetric ──────────────────────────────────────────────────────


class TestQueryMetric:
    def test_to_dict(self) -> None:
        m = QueryMetric(
            query_id="abc123",
            query_text="MATCH (n) RETURN n",
            status=QueryStatus.SUCCESS,
            total_ms=42.567,
            parse_ms=5.123,
            row_count=3,
        )
        d = m.to_dict()
        assert d["query_id"] == "abc123"
        assert d["status"] == "success"
        assert d["total_ms"] == 42.567
        assert d["parse_ms"] == 5.123
        assert d["row_count"] == 3
        assert d["error_message"] is None

    def test_to_dict_error(self) -> None:
        m = QueryMetric(
            query_id="err1",
            query_text="BAD",
            status=QueryStatus.ERROR,
            total_ms=1.0,
            error_message="syntax error",
        )
        d = m.to_dict()
        assert d["status"] == "error"
        assert d["error_message"] == "syntax error"


# ── AnalyticsEngine ──────────────────────────────────────────────────


class TestAnalyticsEngine:
    def _populate(
        self,
        collector: MetricsCollector,
        n: int = 50,
        *,
        error_every: int = 0,
    ) -> None:
        """Helper to populate collector with test metrics."""
        for i in range(n):
            if error_every and i % error_every == 0:
                collector.record_error(
                    f"Q{i}",
                    total_ms=float(i * 2),
                    error_message="fail",
                )
            else:
                collector.record_success(
                    f"MATCH (n) RETURN n LIMIT {i}",
                    total_ms=float(i * 10 + 5),
                    row_count=i,
                    parse_ms=float(i),
                    exec_ms=float(i * 8),
                )

    def test_empty_summary(self) -> None:
        c = MetricsCollector()
        engine = AnalyticsEngine(c)
        s = engine.summary()
        assert s.total_queries == 0
        assert s.error_rate == 0.0
        assert s.latency.count == 0
        assert s.bottlenecks == []
        assert s.trends == []

    def test_summary_with_data(self) -> None:
        c = MetricsCollector()
        self._populate(c, n=20)
        engine = AnalyticsEngine(c)
        s = engine.summary()
        assert s.total_queries == 20
        assert s.latency.count == 20
        assert s.latency.min > 0
        assert s.latency.max >= s.latency.min
        assert s.latency.p50 > 0
        assert s.latency.p90 >= s.latency.p50
        assert s.latency.p95 >= s.latency.p90
        assert s.latency.p99 >= s.latency.p95

    def test_summary_last_n(self) -> None:
        c = MetricsCollector()
        self._populate(c, n=50)
        engine = AnalyticsEngine(c)
        s = engine.summary(last_n=10)
        assert s.total_queries == 10

    def test_bottleneck_slow_queries(self) -> None:
        c = MetricsCollector()
        # All queries exceed 1000ms threshold
        for i in range(10):
            c.record_success(f"Q{i}", total_ms=2000.0 + i)
        engine = AnalyticsEngine(c, slow_threshold_ms=1000.0)
        s = engine.summary()
        slow_bottleneck = [
            b for b in s.bottlenecks if b.category == "slow_queries"
        ]
        assert len(slow_bottleneck) == 1
        assert slow_bottleneck[0].affected_queries == 10

    def test_bottleneck_parse_heavy(self) -> None:
        c = MetricsCollector()
        for i in range(10):
            c.record_success(
                f"Q{i}",
                total_ms=100.0,
                parse_ms=50.0,
                exec_ms=50.0,
            )
        engine = AnalyticsEngine(c)
        s = engine.summary()
        parse_bottleneck = [
            b for b in s.bottlenecks if b.category == "parse_bottleneck"
        ]
        assert len(parse_bottleneck) == 1

    def test_bottleneck_high_error_rate(self) -> None:
        c = MetricsCollector()
        self._populate(c, n=20, error_every=2)
        engine = AnalyticsEngine(c)
        s = engine.summary()
        error_bottleneck = [
            b for b in s.bottlenecks if b.category == "high_error_rate"
        ]
        assert len(error_bottleneck) == 1

    def test_bottleneck_execution_heavy(self) -> None:
        c = MetricsCollector()
        for i in range(20):
            c.record_success(
                f"Q{i}",
                total_ms=100.0,
                parse_ms=5.0,
                exec_ms=90.0,
            )
        engine = AnalyticsEngine(c)
        s = engine.summary()
        exec_bottleneck = [
            b for b in s.bottlenecks if b.category == "execution_bottleneck"
        ]
        assert len(exec_bottleneck) == 1

    def test_trends(self) -> None:
        c = MetricsCollector()
        base_ts = time.time()
        # Create metrics across 3 time buckets (60s each)
        for i in range(30):
            m = QueryMetric(
                query_id=f"q{i}",
                query_text=f"Q{i}",
                status=QueryStatus.SUCCESS,
                total_ms=float(i * 10),
                timestamp=base_ts + i * 10,  # Spread over 300s
            )
            c.record(m)
        engine = AnalyticsEngine(c, trend_bucket_seconds=60.0)
        s = engine.summary()
        assert len(s.trends) >= 3

    def test_slowest_queries(self) -> None:
        c = MetricsCollector()
        for i in range(20):
            c.record_success(f"Q{i}", total_ms=float(i * 100), row_count=i)
        engine = AnalyticsEngine(c)
        s = engine.summary()
        assert len(s.slowest_queries) == 10  # Default top 10
        # First should be the slowest
        assert (
            s.slowest_queries[0]["total_ms"]
            >= s.slowest_queries[1]["total_ms"]
        )

    def test_recommendations_generated(self) -> None:
        c = MetricsCollector()
        # Create a mix of slow and error queries
        for i in range(10):
            c.record_success(f"Q{i}", total_ms=5000.0)
        for i in range(5):
            c.record_error(f"E{i}", total_ms=100.0, error_message="fail")
        engine = AnalyticsEngine(c, slow_threshold_ms=1000.0)
        s = engine.summary()
        assert len(s.recommendations) > 0

    def test_recommendations_timeout(self) -> None:
        c = MetricsCollector()
        c.record_error(
            "Q1",
            total_ms=30000.0,
            error_message="timeout",
            status=QueryStatus.TIMEOUT,
        )
        c.record_success("Q2", total_ms=10.0)
        engine = AnalyticsEngine(c)
        s = engine.summary()
        timeout_recs = [r for r in s.recommendations if "timeout" in r.lower()]
        assert len(timeout_recs) >= 1

    def test_summary_to_dict(self) -> None:
        c = MetricsCollector()
        self._populate(c, n=10)
        engine = AnalyticsEngine(c)
        s = engine.summary()
        d = s.to_dict()
        assert "total_queries" in d
        assert "latency" in d
        assert "bottlenecks" in d
        assert "trends" in d
        assert "recommendations" in d

    def test_recommendations_latency_ratio(self) -> None:
        c = MetricsCollector()
        # Most queries fast, a few very slow — creates large p99/p50 ratio
        for i in range(90):
            c.record_success(f"Q{i}", total_ms=10.0)
        for i in range(10):
            c.record_success(f"SLOW{i}", total_ms=500.0)
        engine = AnalyticsEngine(c)
        s = engine.summary()
        ratio_recs = [r for r in s.recommendations if "p99" in r.lower()]
        assert len(ratio_recs) >= 1


# ── LatencyBreakdown ─────────────────────────────────────────────────


class TestLatencyBreakdown:
    def test_default_values(self) -> None:
        lb = LatencyBreakdown()
        assert lb.count == 0
        assert lb.p50 == 0.0

    def test_to_dict(self) -> None:
        lb = LatencyBreakdown(
            p50=10.0,
            p90=50.0,
            p95=80.0,
            p99=100.0,
            mean=30.0,
            min=1.0,
            max=150.0,
            count=100,
        )
        d = lb.to_dict()
        assert d["p50_ms"] == 10.0
        assert d["count"] == 100


# ── Percentile edge cases ────────────────────────────────────────────


class TestPercentile:
    def test_empty(self) -> None:
        assert AnalyticsEngine._percentile([], 0.5) == 0.0

    def test_single_value(self) -> None:
        assert AnalyticsEngine._percentile([42.0], 0.5) == 42.0
        assert AnalyticsEngine._percentile([42.0], 0.99) == 42.0

    def test_two_values(self) -> None:
        result = AnalyticsEngine._percentile([10.0, 20.0], 0.5)
        assert result == 15.0  # Midpoint

    def test_interpolation(self) -> None:
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        p50 = AnalyticsEngine._percentile(values, 0.5)
        assert p50 == 30.0
        p0 = AnalyticsEngine._percentile(values, 0.0)
        assert p0 == 10.0
        p100 = AnalyticsEngine._percentile(values, 1.0)
        assert p100 == 50.0


# ── API integration ──────────────────────────────────────────────────


class TestAnalyticsAPI:
    """Test analytics endpoints via the FastAPI test client."""

    @pytest.fixture
    def client(self) -> TestClient:
        from fastapi.testclient import TestClient
        from fastopendata.api import app, get_metrics_collector

        get_metrics_collector().clear()
        return TestClient(app)

    def test_overview_empty(self, client: TestClient) -> None:
        r = client.get("/analytics/overview")
        assert r.status_code == 200
        body = r.json()
        assert body["total_queries"] == 0
        assert body["error_rate"] == 0.0

    def test_summary_empty(self, client: TestClient) -> None:
        r = client.get("/analytics/summary")
        assert r.status_code == 200
        body = r.json()
        assert body["total_queries"] == 0

    def test_recent_empty(self, client: TestClient) -> None:
        r = client.get("/analytics/recent")
        assert r.status_code == 200
        body = r.json()
        assert body["metrics"] == []

    def test_overview_after_queries(self, client: TestClient) -> None:
        from fastopendata.api import get_metrics_collector

        collector = get_metrics_collector()
        collector.record_success("Q1", total_ms=10.0)
        collector.record_success("Q2", total_ms=20.0)
        r = client.get("/analytics/overview")
        assert r.status_code == 200
        body = r.json()
        assert body["total_queries"] == 2
        assert body["total_errors"] == 0

    def test_summary_with_data(self, client: TestClient) -> None:
        from fastopendata.api import get_metrics_collector

        collector = get_metrics_collector()
        for i in range(10):
            collector.record_success(f"Q{i}", total_ms=float(i * 100 + 10))
        r = client.get("/analytics/summary")
        assert r.status_code == 200
        body = r.json()
        assert body["total_queries"] == 10
        assert body["latency"]["count"] == 10

    def test_summary_with_last_n(self, client: TestClient) -> None:
        from fastopendata.api import get_metrics_collector

        collector = get_metrics_collector()
        for i in range(20):
            collector.record_success(f"Q{i}", total_ms=float(i * 10))
        r = client.get("/analytics/summary?last_n=5")
        assert r.status_code == 200
        body = r.json()
        assert body["total_queries"] == 5

    def test_recent_returns_metrics(self, client: TestClient) -> None:
        from fastopendata.api import get_metrics_collector

        collector = get_metrics_collector()
        collector.record_success("Q1", total_ms=10.0, row_count=5)
        collector.record_success("Q2", total_ms=20.0, row_count=8)
        r = client.get("/analytics/recent?n=1")
        assert r.status_code == 200
        body = r.json()
        assert len(body["metrics"]) == 1
        assert body["metrics"][0]["query_text"] == "Q2"

    def test_query_endpoint_records_metrics(self) -> None:
        """Verify the /query endpoint instruments into the collector."""
        import pandas as pd
        from fastapi.testclient import TestClient
        from fastopendata.api import app, get_metrics_collector, set_star
        from pycypher.ingestion.context_builder import ContextBuilder
        from pycypher.star import Star

        get_metrics_collector().clear()

        people = pd.DataFrame(
            {
                "__ID__": [1, 2],
                "name": ["Alice", "Bob"],
            },
        )
        ctx = ContextBuilder().add_entity("Person", people).build()
        set_star(Star(ctx))
        try:
            client = TestClient(app)
            r = client.post(
                "/query",
                json={
                    "query": "MATCH (n:Person) RETURN n.name",
                },
            )
            assert r.status_code == 200
            collector = get_metrics_collector()
            assert collector.total_queries >= 1
            recent = collector.recent(1)
            assert recent[0].status == QueryStatus.SUCCESS
            assert recent[0].row_count == 2
            assert recent[0].parse_ms >= 0
            assert recent[0].total_ms > 0
        finally:
            set_star(Star())


# Import TestClient at module level for type hints
from fastapi.testclient import TestClient
