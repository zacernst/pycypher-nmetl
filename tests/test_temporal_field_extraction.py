"""TDD tests for temporal field extraction helper function.

This test documents the expected behavior of _extract_temporal_field
function before implementing the comprehensive fix.
"""

from __future__ import annotations

import datetime as _dt

from pycypher.collection_evaluator import _extract_temporal_field


class TestTemporalFieldExtraction:
    """Test _extract_temporal_field function behavior."""

    def test_date_fields_basic(self) -> None:
        """Test basic date field extraction (year, month, day)."""
        date_str = "2024-03-15"
        assert _extract_temporal_field(date_str, "year") == 2024
        assert _extract_temporal_field(date_str, "month") == 3
        assert _extract_temporal_field(date_str, "day") == 15

    def test_date_fields_iso_calendar(self) -> None:
        """Test ISO calendar date fields (week, dayOfWeek, dayOfYear)."""
        date_str = "2024-03-15"  # Friday, ISO week 11, day 75
        expected_week = _dt.date(2024, 3, 15).isocalendar()[1]
        expected_day_of_week = _dt.date(2024, 3, 15).isoweekday()
        expected_day_of_year = _dt.date(2024, 3, 15).timetuple().tm_yday

        assert _extract_temporal_field(date_str, "week") == expected_week
        assert _extract_temporal_field(date_str, "dayOfWeek") == expected_day_of_week
        assert _extract_temporal_field(date_str, "dayOfYear") == expected_day_of_year

    def test_date_fields_quarter(self) -> None:
        """Test quarter field extraction."""
        # Q1: Jan, Feb, Mar
        assert _extract_temporal_field("2024-01-15", "quarter") == 1
        assert _extract_temporal_field("2024-02-15", "quarter") == 1
        assert _extract_temporal_field("2024-03-15", "quarter") == 1
        # Q2: Apr, May, Jun
        assert _extract_temporal_field("2024-04-15", "quarter") == 2
        assert _extract_temporal_field("2024-05-15", "quarter") == 2
        assert _extract_temporal_field("2024-06-15", "quarter") == 2
        # Q3: Jul, Aug, Sep
        assert _extract_temporal_field("2024-07-15", "quarter") == 3
        assert _extract_temporal_field("2024-08-15", "quarter") == 3
        assert _extract_temporal_field("2024-09-15", "quarter") == 3
        # Q4: Oct, Nov, Dec
        assert _extract_temporal_field("2024-10-15", "quarter") == 4
        assert _extract_temporal_field("2024-11-15", "quarter") == 4
        assert _extract_temporal_field("2024-12-15", "quarter") == 4

    def test_datetime_fields_basic(self) -> None:
        """Test datetime field extraction (includes date fields)."""
        datetime_str = "2024-03-15T10:30:45"
        # Date fields should work on datetime too
        assert _extract_temporal_field(datetime_str, "year") == 2024
        assert _extract_temporal_field(datetime_str, "month") == 3
        assert _extract_temporal_field(datetime_str, "day") == 15

    def test_datetime_fields_time(self) -> None:
        """Test time-specific fields (hour, minute, second)."""
        datetime_str = "2024-03-15T10:30:45"
        assert _extract_temporal_field(datetime_str, "hour") == 10
        assert _extract_temporal_field(datetime_str, "minute") == 30
        assert _extract_temporal_field(datetime_str, "second") == 45

    def test_datetime_fields_microsecond(self) -> None:
        """Test microsecond and millisecond extraction."""
        # Test with microseconds
        datetime_str = "2024-03-15T10:30:45.123456"
        assert _extract_temporal_field(datetime_str, "microsecond") == 123456
        assert (
            _extract_temporal_field(datetime_str, "millisecond") == 123
        )  # First 3 digits of microsecond

        # Test with only milliseconds
        datetime_str = "2024-03-15T10:30:45.789"
        assert _extract_temporal_field(datetime_str, "microsecond") == 789000
        assert _extract_temporal_field(datetime_str, "millisecond") == 789

    def test_invalid_inputs(self) -> None:
        """Test error handling for invalid inputs."""
        # Non-string input
        assert _extract_temporal_field(None, "year") is None
        assert _extract_temporal_field(123, "year") is None
        assert _extract_temporal_field([], "year") is None

        # Invalid date strings
        assert _extract_temporal_field("invalid-date", "year") is None
        assert _extract_temporal_field("2024-13-01", "year") is None  # Invalid month
        assert _extract_temporal_field("2024-02-30", "year") is None  # Invalid day

        # Invalid field names
        assert _extract_temporal_field("2024-03-15", "invalid_field") is None
        assert _extract_temporal_field("2024-03-15", "") is None

    def test_timezone_handling(self) -> None:
        """Test timezone offset handling."""
        # UTC offset
        datetime_str = "2024-03-15T10:30:45+05:00"
        assert _extract_temporal_field(datetime_str, "year") == 2024
        assert (
            _extract_temporal_field(datetime_str, "hour") == 10
        )  # Should be local hour, not UTC

        # Z suffix (UTC)
        datetime_str = "2024-03-15T10:30:45Z"
        assert _extract_temporal_field(datetime_str, "year") == 2024
        assert _extract_temporal_field(datetime_str, "hour") == 10
