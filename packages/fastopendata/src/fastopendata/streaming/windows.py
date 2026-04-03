"""Temporal windowing for the streaming query engine.

Supports tumbling (fixed-size, non-overlapping), sliding (fixed-size,
overlapping), and session (activity-gap-based) windows.  Each window
accumulates records and fires when triggered by the watermark.
"""

from __future__ import annotations

import bisect
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from fastopendata.streaming.core import StreamRecord


@dataclass(frozen=True, slots=True)
class WindowSpec:
    """Describes a closed time interval ``[start, end)``."""

    start: float
    end: float

    @property
    def duration(self) -> float:
        return self.end - self.start

    def contains(self, event_time: float) -> bool:
        return self.start <= event_time < self.end

    def __repr__(self) -> str:
        return f"Window({self.start:.1f}-{self.end:.1f})"


@dataclass
class WindowState:
    """Mutable accumulator for records assigned to a specific window."""

    spec: WindowSpec
    records: list[StreamRecord] = field(default_factory=list)
    fired: bool = False

    def add(self, record: StreamRecord) -> None:
        self.records.append(record)

    @property
    def count(self) -> int:
        return len(self.records)


class WindowAssigner(ABC):
    """Base class for window assignment strategies."""

    @abstractmethod
    def assign(self, record: StreamRecord) -> list[WindowSpec]:
        """Return the window(s) to which *record* should be assigned."""

    @abstractmethod
    def merge(self, windows: list[WindowSpec]) -> list[WindowSpec]:
        """Merge overlapping or adjacent windows (session semantics)."""


class TumblingWindow(WindowAssigner):
    """Fixed-size, non-overlapping windows.

    Parameters
    ----------
    size : float
        Window size in seconds.

    """

    def __init__(self, size: float) -> None:
        self._size = size

    @property
    def size(self) -> float:
        return self._size

    def assign(self, record: StreamRecord) -> list[WindowSpec]:
        start = (record.event_time // self._size) * self._size
        return [WindowSpec(start=start, end=start + self._size)]

    def merge(self, windows: list[WindowSpec]) -> list[WindowSpec]:
        # Tumbling windows never overlap — no merge needed.
        return windows


class SlidingWindow(WindowAssigner):
    """Fixed-size windows that advance by a configurable slide interval.

    Parameters
    ----------
    size : float
        Window size in seconds.
    slide : float
        Distance between the start of consecutive windows.

    """

    def __init__(self, size: float, slide: float) -> None:
        if slide <= 0:
            msg = f"slide ({slide}) must be > 0"
            raise ValueError(msg)
        if slide > size:
            msg = f"slide ({slide}) must be ≤ size ({size})"
            raise ValueError(msg)
        self._size = size
        self._slide = slide

    @property
    def size(self) -> float:
        return self._size

    @property
    def slide(self) -> float:
        return self._slide

    def assign(self, record: StreamRecord) -> list[WindowSpec]:
        # A record may belong to multiple overlapping windows.
        et = record.event_time
        # Earliest window whose end is > et
        last_start = (et // self._slide) * self._slide
        specs: list[WindowSpec] = []
        start = last_start - self._size + self._slide
        while start <= last_start:
            spec = WindowSpec(start=start, end=start + self._size)
            if spec.contains(et):
                specs.append(spec)
            start += self._slide
        return specs

    def merge(self, windows: list[WindowSpec]) -> list[WindowSpec]:
        return windows


class SessionWindow(WindowAssigner):
    """Activity-gap-based windows that close after inactivity exceeds the gap.

    Parameters
    ----------
    gap : float
        Maximum inactivity period (seconds) before starting a new session.

    """

    def __init__(self, gap: float) -> None:
        self._gap = gap

    @property
    def gap(self) -> float:
        return self._gap

    def assign(self, record: StreamRecord) -> list[WindowSpec]:
        # Each record initially starts its own micro-session.
        return [
            WindowSpec(
                start=record.event_time, end=record.event_time + self._gap
            )
        ]

    def merge(self, windows: list[WindowSpec]) -> list[WindowSpec]:
        if not windows:
            return []
        sorted_windows = sorted(windows, key=lambda w: w.start)
        merged: list[WindowSpec] = [sorted_windows[0]]
        for win in sorted_windows[1:]:
            last = merged[-1]
            if win.start <= last.end:
                merged[-1] = WindowSpec(
                    start=last.start, end=max(last.end, win.end)
                )
            else:
                merged.append(win)
        return merged


class WindowManager:
    """Manages window state for a keyed stream.

    Tracks open windows per key, assigns incoming records, and fires
    windows whose end time is at or before the watermark.

    Parameters
    ----------
    assigner : WindowAssigner
        Strategy for assigning records to windows.

    """

    def __init__(self, assigner: WindowAssigner) -> None:
        self._assigner = assigner
        # key → { WindowSpec → WindowState }
        self._state: dict[str, dict[WindowSpec, WindowState]] = {}
        self._total_fired: int = 0
        # Sorted index: list of (end_time, key, WindowSpec) sorted by end_time.
        # Enables O(log n) lookup of fireable windows via bisect.
        self._end_index: list[tuple[float, str, WindowSpec]] = []

    @property
    def total_fired(self) -> int:
        return self._total_fired

    def add(self, record: StreamRecord) -> None:
        """Assign a record to the appropriate window(s)."""
        specs = self._assigner.assign(record)
        key = record.key
        if key not in self._state:
            self._state[key] = {}
        key_windows = self._state[key]
        for spec in specs:
            if spec not in key_windows:
                key_windows[spec] = WindowState(spec=spec)
                # Insert into sorted index using bisect for O(log n) insertion.
                entry = (spec.end, key, spec)
                bisect.insort(self._end_index, entry)
            key_windows[spec].add(record)

    def fire(self, watermark: float) -> list[tuple[str, WindowState]]:
        """Fire and return all windows whose end <= watermark.

        Uses a sorted index on window end times for O(log n + k) performance
        where k is the number of windows that actually fire, instead of
        scanning all windows across all keys.
        """
        fired: list[tuple[str, WindowState]] = []
        # Binary search for the cutoff point: all entries with end <= watermark.
        cutoff = bisect.bisect_right(
            self._end_index, (watermark, "\xff", None)
        )
        if cutoff == 0:
            return fired
        # Process only the windows up to the cutoff.
        for i in range(cutoff):
            _end_time, key, spec = self._end_index[i]
            key_windows = self._state.get(key)
            if key_windows is None:
                continue
            state = key_windows.get(spec)
            if state is None or state.fired:
                continue
            state.fired = True
            fired.append((key, state))
            self._total_fired += 1
            del key_windows[spec]
            if not key_windows:
                del self._state[key]
        # Remove all processed entries from the sorted index (fired or not).
        self._end_index = self._end_index[cutoff:]
        return fired

    def merge_sessions(self) -> None:
        """Merge session windows for all keys (only relevant for SessionWindow)."""
        if not isinstance(self._assigner, SessionWindow):
            return
        rebuild_index = False
        for key, key_windows in self._state.items():
            specs = list(key_windows.keys())
            merged = self._assigner.merge(specs)
            if len(merged) < len(specs):
                rebuild_index = True
                new_states: dict[WindowSpec, WindowState] = {}
                for mspec in merged:
                    combined = WindowState(spec=mspec)
                    for old_spec, old_state in key_windows.items():
                        if mspec.contains(old_spec.start):
                            combined.records.extend(old_state.records)
                    new_states[mspec] = combined
                self._state[key] = new_states
        if rebuild_index:
            self._rebuild_end_index()

    def _rebuild_end_index(self) -> None:
        """Rebuild the sorted end-time index from current state."""
        entries: list[tuple[float, str, WindowSpec]] = []
        for key, key_windows in self._state.items():
            for spec in key_windows:
                if not key_windows[spec].fired:
                    entries.append((spec.end, key, spec))
        entries.sort()
        self._end_index = entries
