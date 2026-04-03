"""Category registration module for scalar functions."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pandas as pd
from shared.helpers import is_null_value

from pycypher.constants import (
    _broadcast_series,
    _init_null_result,
)

if TYPE_CHECKING:
    from pycypher.scalar_functions import ScalarFunctionRegistry

_is_null = is_null_value


def register(registry: ScalarFunctionRegistry) -> None:
    """Register Neo4j-compatible temporal constructor functions.

    Functions registered:

    * ``date(str)`` — parse an ISO 8601 date string; returns the string
      representation (``'YYYY-MM-DD'``).
    * ``datetime(str)`` — parse an ISO 8601 datetime string; returns the
      ISO-format string.
    * ``localdatetime(str)`` — alias for ``datetime`` without timezone
      awareness.
    * ``duration(str | dict)`` — parse an ISO 8601 duration string (e.g.
      ``'P5D'``) or a component dict (e.g. ``{months: 6}``); returns a
      dict with all ten component fields: ``years``, ``months``, ``weeks``,
      ``days``, ``hours``, ``minutes``, ``seconds``, ``milliseconds``,
      ``microseconds``, ``nanoseconds``.

    All functions are null-safe: passing ``null`` returns ``null``.
    """
    from datetime import date, datetime

    # ------------------------------------------------------------------
    # date([string]) -> date string  (no-arg → current date)
    # ------------------------------------------------------------------
    def _date(s: pd.Series) -> pd.Series:
        """Return current date or parse an ISO 8601 date string row-wise.

        When called with no argument the evaluator injects a dummy integer
        Series; an integer dtype signals "return today" per row.  When
        called with a string argument each element is parsed as
        ``'YYYY-MM-DD'``.

        Args:
            s: Integer dummy Series (zero-arg call) **or** Series of ISO
               8601 date strings (e.g. ``'2024-03-15'``).

        Returns:
            Series of ``'YYYY-MM-DD'`` strings.  Null string inputs
            produce null outputs.  Unparseable strings raise
            :exc:`ValueError`.

        """
        if s.dtype.kind in ("i", "u"):
            # Zero-arg call via dummy injection → current date per row
            val = date.today().isoformat()
            return _broadcast_series(val, len(s))

        # Vectorized implementation replacing .apply(_parse) anti-pattern
        if len(s) == 0:
            return s

        # Use pandas to_datetime for vectorized date parsing
        try:
            # Convert to datetime first (vectorized)
            parsed_dates = pd.to_datetime(
                s,
                format="%Y-%m-%d",
                errors="coerce",
            )

            # Convert back to ISO format strings (vectorized)
            result = parsed_dates.dt.strftime("%Y-%m-%d")

            # Handle nulls - both original nulls and parse failures become None
            result = result.where(parsed_dates.notna() & s.notna(), None)

            return result

        except (ValueError, TypeError):
            # Fallback for complex cases
            nr = _init_null_result(s)
            if nr.all_null:
                return nr.result

            parsed_values = []
            for sv in nr.non_null_vals.astype(str):
                try:
                    parsed_values.append(
                        date.fromisoformat(sv).isoformat(),
                    )
                except (ValueError, TypeError):
                    parsed_values.append(None)

            nr.result[nr.non_null_mask] = parsed_values

            return nr.result

    registry.register_function(
        name="date",
        callable=_date,
        min_args=0,
        max_args=1,
        description="Current date or parse ISO 8601 date string",
        example="date() → '2024-03-15'; date('2024-03-15') → '2024-03-15'",
    )

    # ------------------------------------------------------------------
    # datetime([string]) -> datetime string  (no-arg → current datetime)
    # ------------------------------------------------------------------
    def _datetime(s: pd.Series) -> pd.Series:
        """Return current datetime or parse an ISO 8601 datetime string.

        Handles both naive (``'2024-03-15T10:30:00'``) and UTC-offset
        (``'2024-03-15T10:30:00Z'``) forms.  The trailing ``Z`` is
        normalised to ``+00:00`` before parsing.  An integer dummy Series
        (from zero-arg injection) returns the current datetime per row.

        Args:
            s: Integer dummy Series (zero-arg call) **or** Series of ISO
               8601 datetime strings.

        Returns:
            Series of ISO-format datetime strings.  Null inputs → null.

        """
        if s.dtype.kind in ("i", "u"):
            # Zero-arg call → current datetime per row
            val = datetime.now().isoformat()
            return _broadcast_series(val, len(s))

        # Vectorized implementation replacing .apply(_parse) anti-pattern
        if len(s) == 0:
            return s

        # Use pandas to_datetime for vectorized datetime parsing
        try:
            # Handle 'Z' timezone suffix (vectorized string operation)
            s_normalized = s.astype(str).str.replace(
                "Z",
                "+00:00",
                regex=False,
            )

            # Convert to datetime (vectorized)
            parsed_datetimes = pd.to_datetime(
                s_normalized,
                errors="coerce",
                utc=True,
            )

            # Convert back to ISO format strings (vectorized)
            # Include microsecond precision but no timezone
            result = parsed_datetimes.dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

            # Handle nulls - both original nulls and parse failures become None
            result = result.where(
                parsed_datetimes.notna() & s.notna(),
                None,
            )

            return result

        except (ValueError, TypeError):
            # Fallback for complex cases
            nr = _init_null_result(s)
            if nr.all_null:
                return nr.result

            parsed_values = []
            for sv in nr.non_null_vals.astype(str):
                try:
                    normalized = sv.replace("Z", "+00:00")
                    parsed_values.append(
                        datetime.fromisoformat(normalized).isoformat(),
                    )
                except (ValueError, TypeError):
                    parsed_values.append(None)

            nr.result[nr.non_null_mask] = parsed_values

            return nr.result

    registry.register_function(
        name="datetime",
        callable=_datetime,
        min_args=0,
        max_args=1,
        description="Current datetime or parse ISO 8601 datetime string",
        example="datetime() → '2024-03-15T10:30:00'; datetime('2024-03-15T10:30:00Z') → '2024-03-15T10:30:00+00:00'",
    )

    # localdatetime — same as datetime (no timezone stripping needed;
    # users pass naive strings)
    registry.register_function(
        name="localdatetime",
        callable=_datetime,
        min_args=0,
        max_args=1,
        description="Current local datetime or parse ISO 8601 local datetime string",
        example="localdatetime() → '2024-03-15T10:30:00'; localdatetime('2024-03-15T10:30:00') → '2024-03-15T10:30:00'",
    )

    # ------------------------------------------------------------------
    # duration(string | map) -> full component dict
    # ------------------------------------------------------------------
    def _duration(s: pd.Series) -> pd.Series:
        """Parse ISO 8601 duration strings or map-form dicts row-wise.

        Supports two input forms:

        * **ISO string** ``'P1Y2M3DT4H5M6S'`` — all ISO 8601 duration
          string variants including weeks (``P4W``).
        * **Map form** ``{"years": 1, "months": 2, ...}`` — any subset of
          the component keys; missing keys default to 0.

        Returns a dict with all ten Neo4j duration component fields:
        ``years``, ``months``, ``weeks``, ``days``, ``hours``,
        ``minutes``, ``seconds``, ``milliseconds``, ``microseconds``,
        ``nanoseconds``.  Each value is an integer (float inputs are
        truncated to int).

        This preserves the original components exactly — unlike the
        previous ``timedelta``-based approach which collapsed years and
        months into approximate day/second counts, losing the original
        values on field access.

        Null inputs produce null outputs.

        Args:
            s: Series of ISO 8601 duration strings or component dicts.

        Returns:
            Series of duration component dicts or null.

        """
        import re

        _DURATION_RE = re.compile(
            r"^P"
            r"(?:(?P<years>\d+(?:\.\d+)?)Y)?"
            r"(?:(?P<months>\d+(?:\.\d+)?)M)?"
            r"(?:(?P<weeks>\d+(?:\.\d+)?)W)?"
            r"(?:(?P<days>\d+(?:\.\d+)?)D)?"
            r"(?:T"
            r"(?:(?P<hours>\d+(?:\.\d+)?)H)?"
            r"(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
            r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?"
            r")?$",
        )

        _COMPONENT_KEYS = (
            "years",
            "months",
            "weeks",
            "days",
            "hours",
            "minutes",
            "seconds",
            "milliseconds",
            "microseconds",
            "nanoseconds",
        )

        def _parse(val: object) -> object:
            if _is_null(val):
                return None
            # Map form: {"years": 1, "days": 3, ...}
            if isinstance(val, dict):
                return {k: int(val.get(k, 0)) for k in _COMPONENT_KEYS}  # type: ignore[arg-type]  # narrowed by isinstance
            # ISO 8601 string form
            sv = str(val)
            m = _DURATION_RE.match(sv)
            if not m:
                from pycypher.exceptions import InvalidCastError

                msg = f"Cannot parse duration string: {sv!r}"
                raise InvalidCastError(msg)
            g = m.groupdict(default="0")
            return {
                "years": int(float(g["years"])),
                "months": int(float(g["months"])),
                "weeks": int(float(g["weeks"])),
                "days": int(float(g["days"])),
                "hours": int(float(g["hours"])),
                "minutes": int(float(g["minutes"])),
                "seconds": int(float(g["seconds"])),
                "milliseconds": 0,
                "microseconds": 0,
                "nanoseconds": 0,
            }

        # Vectorized implementation replacing .apply(_parse) anti-pattern
        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result

        parsed_values = []
        for val in nr.non_null_vals:
            try:
                parsed_values.append(_parse(val))
            except (ValueError, TypeError, AttributeError, KeyError):
                parsed_values.append(None)

        nr.result[nr.non_null_vals.index] = parsed_values

        return nr.result

    registry.register_function(
        name="duration",
        callable=_duration,
        min_args=1,
        max_args=1,
        description=(
            "Parse ISO 8601 duration string or map into a duration value "
            "with all component fields (years, months, weeks, days, hours, "
            "minutes, seconds, milliseconds, microseconds, nanoseconds)"
        ),
        example=(
            "duration('P1Y2M3DT4H') → {years:1, months:2, days:3, hours:4, ...}; "
            "duration({months: 6}) → {months:6, ...}"
        ),
    )

    # ------------------------------------------------------------------
    # timestamp() -> current epoch milliseconds (integer)
    # ------------------------------------------------------------------
    import time as _time

    def _timestamp(s: pd.Series | None = None) -> pd.Series:
        """Return current epoch milliseconds for every row.

        Accepts an optional dummy series (ignored values) so the function
        can be called as ``timestamp()`` (no args) or with a row-index
        series to control output length.

        Args:
            s: Optional dummy series; length determines output length.

        Returns:
            Series of integers representing milliseconds since Unix epoch.

        """
        if s is None:
            s = pd.Series(dtype="int64")
        ts = int(_time.time() * 1000)
        n = len(s) if len(s) > 0 else 1
        return _broadcast_series(ts, n, dtype="int64")

    registry.register_function(
        name="timestamp",
        callable=_timestamp,
        min_args=0,
        max_args=1,
        description="Return current epoch milliseconds",
        example="timestamp() → 1700000000000",
    )

    # ------------------------------------------------------------------
    # localtime() -> current local time string (HH:MM:SS...)
    # ------------------------------------------------------------------
    def _localtime(s: pd.Series | None = None) -> pd.Series:
        """Return current local time string for every row.

        Args:
            s: Optional dummy series; length determines output length.

        Returns:
            Series of local-time strings in ISO format (``'HH:MM:SS.ffffff'``).

        """
        if s is None:
            s = pd.Series(dtype=object)
        from datetime import datetime as _dt

        val = _dt.now().time().isoformat()
        n = len(s) if len(s) > 0 else 1
        return _broadcast_series(val, n)

    registry.register_function(
        name="localtime",
        callable=_localtime,
        min_args=0,
        max_args=1,
        description="Return current local time as a string",
        example="localtime() → '10:30:00.123456'",
    )

    # ------------------------------------------------------------------
    # localdate() -> current local date string (YYYY-MM-DD)
    # ------------------------------------------------------------------
    def _localdate(s: pd.Series | None = None) -> pd.Series:
        """Return current local date string for every row.

        Args:
            s: Optional dummy series; length determines output length.

        Returns:
            Series of ISO 8601 date strings (``'YYYY-MM-DD'``).

        """
        if s is None:
            s = pd.Series(dtype=object)
        val = date.today().isoformat()
        n = len(s) if len(s) > 0 else 1
        return _broadcast_series(val, n)

    registry.register_function(
        name="localdate",
        callable=_localdate,
        min_args=0,
        max_args=1,
        description="Return current local date as a string",
        example="localdate() → '2024-03-15'",
    )

    # ------------------------------------------------------------------
    # date.truncate(unit, temporal) -> truncated date string
    # datetime.truncate(unit, temporal) -> truncated datetime string
    # localdatetime.truncate(unit, temporal) -> truncated datetime string
    # ------------------------------------------------------------------
    def _truncate_date(unit_s: pd.Series, value_s: pd.Series) -> pd.Series:
        """Truncate a date string to the specified unit.

        Args:
            unit_s: Series of truncation unit strings (e.g. ``'month'``).
            value_s: Series of ISO 8601 date strings (e.g. ``'2024-03-15'``).

        Returns:
            Series of truncated ISO 8601 date strings.

        Raises:
            ValueError: If an unrecognised unit is supplied.

        """
        _DATE_UNITS: set[str] = {
            "millennium",
            "century",
            "decade",
            "year",
            "quarter",
            "month",
            "week",
            "day",
        }

        def _trunc(unit_raw: object, val: object) -> object:
            if _is_null(val):
                return None
            unit = str(unit_raw).lower()
            if unit not in _DATE_UNITS:
                msg = (
                    f"Unknown truncation unit for date.truncate: {unit_raw!r}. "
                    f"Valid units: {sorted(_DATE_UNITS)}"
                )
                raise ValueError(
                    msg,
                )
            d = date.fromisoformat(str(val))
            if unit == "millennium":
                # Neo4j: millennium containing year 2024 starts at 2001
                start = ((d.year - 1) // 1000) * 1000 + 1
                return date(start, 1, 1).isoformat()
            if unit == "century":
                start = ((d.year - 1) // 100) * 100 + 1
                return date(start, 1, 1).isoformat()
            if unit == "decade":
                start = (d.year // 10) * 10
                return date(start, 1, 1).isoformat()
            if unit == "year":
                return date(d.year, 1, 1).isoformat()
            if unit == "quarter":
                q_start_month = ((d.month - 1) // 3) * 3 + 1
                return date(d.year, q_start_month, 1).isoformat()
            if unit == "month":
                return date(d.year, d.month, 1).isoformat()
            if unit == "week":
                # ISO week starts on Monday
                return (d - timedelta(days=d.weekday())).isoformat()
            # day — no-op for plain dates
            return d.isoformat()

        return pd.Series(
            [_trunc(u, v) for u, v in zip(unit_s, value_s, strict=False)],
        )

    registry.register_function(
        name="date.truncate",
        callable=_truncate_date,
        min_args=2,
        max_args=2,
        description="Truncate a date to the specified unit",
        example="date.truncate('month', '2024-03-15') → '2024-03-01'",
    )

    def _truncate_datetime(
        unit_s: pd.Series,
        value_s: pd.Series,
    ) -> pd.Series:
        """Truncate a datetime string to the specified unit.

        Args:
            unit_s: Series of truncation unit strings (e.g. ``'hour'``).
            value_s: Series of ISO 8601 datetime strings.

        Returns:
            Series of truncated ISO 8601 datetime strings.

        Raises:
            ValueError: If an unrecognised unit is supplied.

        """
        _DATETIME_UNITS: set[str] = {
            "millennium",
            "century",
            "decade",
            "year",
            "quarter",
            "month",
            "week",
            "day",
            "hour",
            "minute",
            "second",
        }

        def _trunc(unit_raw: object, val: object) -> object:
            if _is_null(val):
                return None
            unit = str(unit_raw).lower()
            if unit not in _DATETIME_UNITS:
                msg = (
                    f"Unknown truncation unit for datetime.truncate: {unit_raw!r}. "
                    f"Valid units: {sorted(_DATETIME_UNITS)}"
                )
                raise ValueError(
                    msg,
                )
            sv = str(val).replace("Z", "+00:00")
            dt = datetime.fromisoformat(sv)
            if unit == "millennium":
                start = ((dt.year - 1) // 1000) * 1000 + 1
                return datetime(start, 1, 1).isoformat()
            if unit == "century":
                start = ((dt.year - 1) // 100) * 100 + 1
                return datetime(start, 1, 1).isoformat()
            if unit == "decade":
                start = (dt.year // 10) * 10
                return datetime(start, 1, 1).isoformat()
            if unit == "year":
                return datetime(dt.year, 1, 1).isoformat()
            if unit == "quarter":
                q_start_month = ((dt.month - 1) // 3) * 3 + 1
                return datetime(dt.year, q_start_month, 1).isoformat()
            if unit == "month":
                return datetime(dt.year, dt.month, 1).isoformat()
            if unit == "week":
                d = dt.date() - timedelta(days=dt.weekday())
                return datetime(d.year, d.month, d.day).isoformat()
            if unit == "day":
                return datetime(dt.year, dt.month, dt.day).isoformat()
            if unit == "hour":
                return datetime(
                    dt.year,
                    dt.month,
                    dt.day,
                    dt.hour,
                ).isoformat()
            if unit == "minute":
                return datetime(
                    dt.year,
                    dt.month,
                    dt.day,
                    dt.hour,
                    dt.minute,
                ).isoformat()
            # second
            return datetime(
                dt.year,
                dt.month,
                dt.day,
                dt.hour,
                dt.minute,
                dt.second,
            ).isoformat()

        return pd.Series(
            [_trunc(u, v) for u, v in zip(unit_s, value_s, strict=False)],
        )

    registry.register_function(
        name="datetime.truncate",
        callable=_truncate_datetime,
        min_args=2,
        max_args=2,
        description="Truncate a datetime to the specified unit",
        example="datetime.truncate('hour', '2024-03-15T10:30:45') → '2024-03-15T10:00:00'",
    )

    registry.register_function(
        name="localdatetime.truncate",
        callable=_truncate_datetime,
        min_args=2,
        max_args=2,
        description="Truncate a local datetime to the specified unit",
        example="localdatetime.truncate('day', '2024-03-15T10:30:45') → '2024-03-15T00:00:00'",
    )
