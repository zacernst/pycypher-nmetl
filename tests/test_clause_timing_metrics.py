"""TDD tests for per-clause timing breakdown in QueryMetrics.

Validates that QueryMetrics tracks per-clause-type timing distributions
and parse time separately from total execution time, so operators can
answer "where is time being spent?" from metrics alone.
"""

from __future__ import annotations

import pytest
from shared.metrics import QUERY_METRICS


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    """Reset metrics before each test."""
    QUERY_METRICS.reset()


class TestClauseTimingRecording:
    """Verify that record_query accepts and stores clause_timings."""

    def test_record_query_accepts_clause_timings(self) -> None:
        QUERY_METRICS.record_query(
            query_id="t1",
            elapsed_s=0.05,
            rows=10,
            clauses=["Match", "Return"],
            clause_timings_ms={"Match": 30.0, "Return": 20.0},
        )
        snap = QUERY_METRICS.snapshot()
        assert snap.total_queries == 1

    def test_clause_timings_appear_in_snapshot(self) -> None:
        QUERY_METRICS.record_query(
            query_id="t2",
            elapsed_s=0.1,
            rows=5,
            clauses=["Match", "With", "Return"],
            clause_timings_ms={"Match": 50.0, "With": 30.0, "Return": 20.0},
        )
        snap = QUERY_METRICS.snapshot()
        assert "Match" in snap.clause_timing_p50_ms
        assert "With" in snap.clause_timing_p50_ms
        assert "Return" in snap.clause_timing_p50_ms

    def test_clause_timing_percentile_ordering(self) -> None:
        for i in range(20):
            QUERY_METRICS.record_query(
                query_id=f"t{i}",
                elapsed_s=0.01 * (i + 1),
                rows=1,
                clauses=["Match"],
                clause_timings_ms={"Match": float(i + 1)},
            )
        snap = QUERY_METRICS.snapshot()
        assert snap.clause_timing_p50_ms["Match"] <= snap.clause_timing_p90_ms["Match"]
        assert snap.clause_timing_p90_ms["Match"] <= snap.clause_timing_max_ms["Match"]

    def test_multiple_clause_types_tracked_independently(self) -> None:
        QUERY_METRICS.record_query(
            query_id="t1",
            elapsed_s=0.1,
            rows=1,
            clauses=["Match", "Return"],
            clause_timings_ms={"Match": 80.0, "Return": 20.0},
        )
        QUERY_METRICS.record_query(
            query_id="t2",
            elapsed_s=0.05,
            rows=1,
            clauses=["Match", "With", "Return"],
            clause_timings_ms={"Match": 30.0, "With": 10.0, "Return": 10.0},
        )
        snap = QUERY_METRICS.snapshot()
        # Match was recorded twice, With once
        assert "Match" in snap.clause_timing_p50_ms
        assert "With" in snap.clause_timing_p50_ms
        assert "Return" in snap.clause_timing_p50_ms


class TestParseTimeTracking:
    """Verify that parse time is tracked separately from execution time."""

    def test_record_query_accepts_parse_time(self) -> None:
        QUERY_METRICS.record_query(
            query_id="p1",
            elapsed_s=0.1,
            rows=10,
            parse_time_ms=5.0,
        )
        snap = QUERY_METRICS.snapshot()
        assert snap.parse_time_p50_ms > 0.0

    def test_parse_time_percentile_ordering(self) -> None:
        for i in range(20):
            QUERY_METRICS.record_query(
                query_id=f"p{i}",
                elapsed_s=0.01,
                rows=1,
                parse_time_ms=float(i + 1),
            )
        snap = QUERY_METRICS.snapshot()
        assert snap.parse_time_p50_ms <= snap.parse_time_p90_ms
        assert snap.parse_time_p90_ms <= snap.parse_time_max_ms

    def test_parse_time_reset(self) -> None:
        QUERY_METRICS.record_query(
            query_id="p1",
            elapsed_s=0.01,
            rows=1,
            parse_time_ms=5.0,
        )
        QUERY_METRICS.reset()
        snap = QUERY_METRICS.snapshot()
        assert snap.parse_time_p50_ms == 0.0


class TestClauseTimingReset:
    """Verify reset clears clause timing data."""

    def test_reset_clears_clause_timings(self) -> None:
        QUERY_METRICS.record_query(
            query_id="r1",
            elapsed_s=0.01,
            rows=1,
            clauses=["Match"],
            clause_timings_ms={"Match": 10.0},
        )
        QUERY_METRICS.reset()
        snap = QUERY_METRICS.snapshot()
        assert snap.clause_timing_p50_ms == {}


class TestClauseTimingNoneHandling:
    """Verify backward compat when clause_timings is not provided."""

    def test_no_clause_timings_still_works(self) -> None:
        QUERY_METRICS.record_query(
            query_id="n1",
            elapsed_s=0.01,
            rows=1,
            clauses=["Match", "Return"],
        )
        snap = QUERY_METRICS.snapshot()
        assert snap.clause_timing_p50_ms == {}

    def test_no_parse_time_still_works(self) -> None:
        QUERY_METRICS.record_query(
            query_id="n2",
            elapsed_s=0.01,
            rows=1,
        )
        snap = QUERY_METRICS.snapshot()
        assert snap.parse_time_p50_ms == 0.0


class TestStarClauseTimingIntegration:
    """Verify end-to-end: Star.execute_query populates clause timings."""

    def test_clause_timings_populated_from_execute_query(self) -> None:
        import pandas as pd
        from pycypher.relational_models import EntityMapping, EntityTable
        from pycypher.star import Context, Star

        ID_COLUMN = "__ID__"
        df = pd.DataFrame(
            {ID_COLUMN: [1, 2, 3], "name": ["Alice", "Bob", "Carol"]},
        )
        table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name"],
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=df,
        )
        star = Star(
            context=Context(
                entity_mapping=EntityMapping(mapping={"Person": table}),
            ),
        )
        star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        snap = QUERY_METRICS.snapshot()
        # Should have at least Match and Return clause timings
        assert "Match" in snap.clause_timing_p50_ms
        assert "Return" in snap.clause_timing_p50_ms
        # Parse time should be recorded
        assert snap.parse_time_p50_ms > 0.0
