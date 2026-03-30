"""Tests for temporal windowing: tumbling, sliding, session windows and WindowManager."""

from __future__ import annotations

import pytest
from fastopendata.streaming.core import StreamRecord
from fastopendata.streaming.windows import (
    SessionWindow,
    SlidingWindow,
    TumblingWindow,
    WindowManager,
    WindowSpec,
)

# ---------------------------------------------------------------------------
# WindowSpec
# ---------------------------------------------------------------------------


class TestWindowSpec:
    def test_duration(self) -> None:
        w = WindowSpec(start=10.0, end=20.0)
        assert w.duration == 10.0

    def test_contains(self) -> None:
        w = WindowSpec(start=10.0, end=20.0)
        assert w.contains(10.0)
        assert w.contains(15.0)
        assert not w.contains(20.0)  # exclusive end
        assert not w.contains(9.9)

    def test_repr(self) -> None:
        w = WindowSpec(start=0.0, end=5.0)
        assert "0.0" in repr(w)
        assert "5.0" in repr(w)


# ---------------------------------------------------------------------------
# TumblingWindow
# ---------------------------------------------------------------------------


class TestTumblingWindow:
    def test_assigns_to_single_window(self) -> None:
        tw = TumblingWindow(size=10.0)
        r = StreamRecord(key="k", value={}, event_time=15.0)
        specs = tw.assign(r)
        assert len(specs) == 1
        assert specs[0] == WindowSpec(start=10.0, end=20.0)

    def test_boundary_alignment(self) -> None:
        tw = TumblingWindow(size=5.0)
        r = StreamRecord(key="k", value={}, event_time=10.0)
        specs = tw.assign(r)
        assert specs[0] == WindowSpec(start=10.0, end=15.0)

    def test_size_property(self) -> None:
        tw = TumblingWindow(size=7.5)
        assert tw.size == 7.5

    def test_merge_is_noop(self) -> None:
        tw = TumblingWindow(size=10.0)
        windows = [WindowSpec(0.0, 10.0), WindowSpec(10.0, 20.0)]
        assert tw.merge(windows) == windows


# ---------------------------------------------------------------------------
# SlidingWindow
# ---------------------------------------------------------------------------


class TestSlidingWindow:
    def test_slide_greater_than_size_raises(self) -> None:
        with pytest.raises(ValueError, match="slide"):
            SlidingWindow(size=5.0, slide=10.0)

    def test_slide_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="must be > 0"):
            SlidingWindow(size=10.0, slide=0.0)

    def test_slide_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="must be > 0"):
            SlidingWindow(size=10.0, slide=-1.0)

    def test_assigns_multiple_overlapping_windows(self) -> None:
        sw = SlidingWindow(size=10.0, slide=5.0)
        r = StreamRecord(key="k", value={}, event_time=12.0)
        specs = sw.assign(r)
        # event_time=12 should fall in [5,15) and [10,20)
        assert len(specs) == 2
        for spec in specs:
            assert spec.contains(12.0)

    def test_no_overlap_when_slide_equals_size(self) -> None:
        sw = SlidingWindow(size=10.0, slide=10.0)
        r = StreamRecord(key="k", value={}, event_time=5.0)
        specs = sw.assign(r)
        assert len(specs) == 1

    def test_properties(self) -> None:
        sw = SlidingWindow(size=10.0, slide=3.0)
        assert sw.size == 10.0
        assert sw.slide == 3.0


# ---------------------------------------------------------------------------
# SessionWindow
# ---------------------------------------------------------------------------


class TestSessionWindow:
    def test_assigns_micro_session(self) -> None:
        sw = SessionWindow(gap=5.0)
        r = StreamRecord(key="k", value={}, event_time=100.0)
        specs = sw.assign(r)
        assert len(specs) == 1
        assert specs[0] == WindowSpec(start=100.0, end=105.0)

    def test_gap_property(self) -> None:
        sw = SessionWindow(gap=3.0)
        assert sw.gap == 3.0

    def test_merge_overlapping(self) -> None:
        sw = SessionWindow(gap=5.0)
        windows = [
            WindowSpec(0.0, 5.0),
            WindowSpec(3.0, 8.0),
            WindowSpec(10.0, 15.0),
        ]
        merged = sw.merge(windows)
        assert len(merged) == 2
        assert merged[0] == WindowSpec(0.0, 8.0)
        assert merged[1] == WindowSpec(10.0, 15.0)

    def test_merge_empty(self) -> None:
        sw = SessionWindow(gap=5.0)
        assert sw.merge([]) == []

    def test_merge_no_overlap(self) -> None:
        sw = SessionWindow(gap=5.0)
        windows = [WindowSpec(0.0, 5.0), WindowSpec(10.0, 15.0)]
        merged = sw.merge(windows)
        assert len(merged) == 2


# ---------------------------------------------------------------------------
# WindowManager
# ---------------------------------------------------------------------------


class TestWindowManager:
    def test_add_and_fire_tumbling(self) -> None:
        wm = WindowManager(TumblingWindow(size=10.0))
        for i in range(5):
            r = StreamRecord(key="k", value={"i": i}, event_time=float(i))
            wm.add(r)
        # Window [0, 10) — fire at watermark=10
        fired = wm.fire(watermark=10.0)
        assert len(fired) == 1
        key, state = fired[0]
        assert key == "k"
        assert state.count == 5
        assert wm.total_fired == 1

    def test_fire_returns_empty_before_watermark(self) -> None:
        wm = WindowManager(TumblingWindow(size=10.0))
        r = StreamRecord(key="k", value={}, event_time=5.0)
        wm.add(r)
        fired = wm.fire(watermark=5.0)
        assert fired == []

    def test_multiple_keys(self) -> None:
        wm = WindowManager(TumblingWindow(size=10.0))
        wm.add(StreamRecord(key="a", value={}, event_time=1.0))
        wm.add(StreamRecord(key="b", value={}, event_time=2.0))
        fired = wm.fire(watermark=10.0)
        assert len(fired) == 2
        keys = {k for k, _ in fired}
        assert keys == {"a", "b"}

    def test_sliding_window_multiple_windows(self) -> None:
        wm = WindowManager(SlidingWindow(size=10.0, slide=5.0))
        wm.add(StreamRecord(key="k", value={}, event_time=7.0))
        # At watermark=10, window [0,10) should fire
        fired = wm.fire(watermark=10.0)
        assert len(fired) >= 1

    def test_session_merge(self) -> None:
        wm = WindowManager(SessionWindow(gap=5.0))
        wm.add(StreamRecord(key="k", value={}, event_time=1.0))
        wm.add(StreamRecord(key="k", value={}, event_time=3.0))
        wm.merge_sessions()
        # After merge, close events should be in one session
        fired = wm.fire(watermark=10.0)
        assert len(fired) >= 1
