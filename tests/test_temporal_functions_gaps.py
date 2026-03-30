"""Tests for uncovered lines in temporal_functions.py.

Targets specific coverage gaps:
- Empty series handling in _date() and _datetime()
- Date/datetime parsing fallback paths (via monkeypatch)
- Null and invalid duration inputs
- Week truncation for date and datetime
- Invalid truncation units for datetime.truncate()
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def registry() -> ScalarFunctionRegistry:
    """Return the shared scalar function registry."""
    return ScalarFunctionRegistry.get_instance()


@pytest.fixture
def date_fn(registry: ScalarFunctionRegistry):
    """Return the raw _date callable from the registry."""
    return registry._functions["date"].callable


@pytest.fixture
def datetime_fn(registry: ScalarFunctionRegistry):
    """Return the raw _datetime callable from the registry."""
    return registry._functions["datetime"].callable


@pytest.fixture
def duration_fn(registry: ScalarFunctionRegistry):
    """Return the raw _duration callable from the registry."""
    return registry._functions["duration"].callable


@pytest.fixture
def truncate_date_fn(registry: ScalarFunctionRegistry):
    """Return the raw _truncate_date callable."""
    return registry._functions["date.truncate"].callable


@pytest.fixture
def truncate_datetime_fn(registry: ScalarFunctionRegistry):
    """Return the raw _truncate_datetime callable."""
    return registry._functions["datetime.truncate"].callable


@pytest.fixture
def minimal_context() -> Context:
    """Minimal context with a single entity for standalone RETURN queries."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1],
            "name": ["Alice"],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


# ---------------------------------------------------------------------------
# 1. Empty series in _date() (line 66)
# ---------------------------------------------------------------------------


class TestDateEmptySeries:
    """Calling _date with an empty string Series returns an empty copy."""

    def test_empty_series_returns_empty(self, date_fn) -> None:
        empty = pd.Series(dtype=object)
        result = date_fn(empty)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# 2. Date parsing fallback (lines 85-110)
# ---------------------------------------------------------------------------


class TestDateParsingFallback:
    """Force the except (ValueError, TypeError) handler in _date by patching
    pd.to_datetime to raise.
    """

    def test_fallback_parses_valid_dates(self, date_fn) -> None:
        """When pd.to_datetime raises, the fallback uses date.fromisoformat
        row-by-row and returns valid ISO strings for parseable inputs.
        """
        s = pd.Series(["2024-01-15", "2024-06-01"], dtype=object)
        with patch("pandas.to_datetime", side_effect=TypeError("forced")):
            result = date_fn(s)
        assert result.iloc[0] == "2024-01-15"
        assert result.iloc[1] == "2024-06-01"

    def test_fallback_handles_nulls(self, date_fn) -> None:
        """Fallback path preserves None for null inputs."""
        s = pd.Series([None, "2024-03-15"], dtype=object)
        with patch("pandas.to_datetime", side_effect=ValueError("forced")):
            result = date_fn(s)
        assert result.iloc[0] is None
        assert result.iloc[1] == "2024-03-15"

    def test_fallback_handles_unparseable(self, date_fn) -> None:
        """Fallback path returns None for strings that fromisoformat cannot parse."""
        s = pd.Series(["not-a-date", "2024-01-15"], dtype=object)
        with patch("pandas.to_datetime", side_effect=TypeError("forced")):
            result = date_fn(s)
        assert result.iloc[0] is None
        assert result.iloc[1] == "2024-01-15"

    def test_fallback_all_null(self, date_fn) -> None:
        """Fallback path with all-null Series returns all None."""
        s = pd.Series([None, None], dtype=object)
        with patch("pandas.to_datetime", side_effect=TypeError("forced")):
            result = date_fn(s)
        assert result.iloc[0] is None
        assert result.iloc[1] is None


# ---------------------------------------------------------------------------
# 3. Empty series in _datetime() (line 147)
# ---------------------------------------------------------------------------


class TestDatetimeEmptySeries:
    """Calling _datetime with an empty string Series returns an empty copy."""

    def test_empty_series_returns_empty(self, datetime_fn) -> None:
        empty = pd.Series(dtype=object)
        result = datetime_fn(empty)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# 4. Datetime parsing fallback (lines 177-203)
# ---------------------------------------------------------------------------


class TestDatetimeParsingFallback:
    """Force the except (ValueError, TypeError) handler in _datetime."""

    def test_fallback_parses_valid_datetimes(self, datetime_fn) -> None:
        """Fallback uses datetime.fromisoformat row-by-row."""
        s = pd.Series(
            ["2024-01-15T10:30:00", "2024-06-01T09:00:00"],
            dtype=object,
        )
        with patch("pandas.to_datetime", side_effect=TypeError("forced")):
            result = datetime_fn(s)
        assert "2024-01-15" in str(result.iloc[0])
        assert "2024-06-01" in str(result.iloc[1])

    def test_fallback_handles_z_suffix(self, datetime_fn) -> None:
        """Fallback normalises 'Z' to '+00:00' before fromisoformat."""
        s = pd.Series(["2024-03-15T10:30:00Z"], dtype=object)
        with patch("pandas.to_datetime", side_effect=ValueError("forced")):
            result = datetime_fn(s)
        assert "2024-03-15" in str(result.iloc[0])
        assert "10:30:00" in str(result.iloc[0])

    def test_fallback_handles_nulls(self, datetime_fn) -> None:
        """Fallback path preserves None for null inputs."""
        s = pd.Series([None, "2024-03-15T10:30:00"], dtype=object)
        with patch("pandas.to_datetime", side_effect=TypeError("forced")):
            result = datetime_fn(s)
        assert result.iloc[0] is None
        assert "2024-03-15" in str(result.iloc[1])

    def test_fallback_handles_unparseable(self, datetime_fn) -> None:
        """Fallback returns None for unparseable datetime strings."""
        s = pd.Series(["not-a-datetime", "2024-01-15T10:30:00"], dtype=object)
        with patch("pandas.to_datetime", side_effect=TypeError("forced")):
            result = datetime_fn(s)
        assert result.iloc[0] is None
        assert "2024-01-15" in str(result.iloc[1])

    def test_fallback_all_null(self, datetime_fn) -> None:
        """Fallback with all-null Series returns all None."""
        s = pd.Series([None, None], dtype=object)
        with patch("pandas.to_datetime", side_effect=TypeError("forced")):
            result = datetime_fn(s)
        assert result.iloc[0] is None
        assert result.iloc[1] is None


# ---------------------------------------------------------------------------
# 5. Null duration input (line 288)
# ---------------------------------------------------------------------------


class TestDurationNullInput:
    """Pass null value to duration() -- should produce null output."""

    def test_null_duration_returns_null(self, duration_fn) -> None:
        s = pd.Series([None], dtype=object)
        result = duration_fn(s)
        assert result.iloc[0] is None

    def test_null_among_valid(self, duration_fn) -> None:
        s = pd.Series([None, "P1Y"], dtype=object)
        result = duration_fn(s)
        assert result.iloc[0] is None
        assert isinstance(result.iloc[1], dict)
        assert result.iloc[1]["years"] == 1


# ---------------------------------------------------------------------------
# 6. Invalid duration string (lines 296-299) -- triggers InvalidCastError
# ---------------------------------------------------------------------------


class TestDurationInvalidString:
    """Pass non-ISO-8601 strings to duration(). The _parse helper raises
    InvalidCastError (a ValueError subclass), which is caught by the loop's
    except clause and stored as None.
    """

    def test_invalid_duration_becomes_none(self, duration_fn) -> None:
        s = pd.Series(["not-a-duration"], dtype=object)
        result = duration_fn(s)
        assert result.iloc[0] is None

    def test_various_invalid_strings(self, duration_fn) -> None:
        """Multiple non-matching strings all produce None."""
        s = pd.Series(["hello", "12345", ""], dtype=object)
        result = duration_fn(s)
        for i in range(len(s)):
            assert result.iloc[i] is None


# ---------------------------------------------------------------------------
# 7. Duration parsing exception path (lines 332-333)
# ---------------------------------------------------------------------------


class TestDurationParsingException:
    """Values that trigger the except clause in the duration processing loop."""

    def test_mixed_valid_and_invalid(self, duration_fn) -> None:
        """Valid durations parse; invalid strings become None."""
        s = pd.Series(["P1Y2M", "garbage", "P5D"], dtype=object)
        result = duration_fn(s)
        assert isinstance(result.iloc[0], dict)
        assert result.iloc[0]["years"] == 1
        assert result.iloc[0]["months"] == 2
        assert result.iloc[1] is None
        assert isinstance(result.iloc[2], dict)
        assert result.iloc[2]["days"] == 5

    def test_map_form_duration(self, duration_fn) -> None:
        """Dict inputs are handled via the map-form branch."""
        s = pd.Series([{"years": 2, "months": 6}], dtype=object)
        result = duration_fn(s)
        d = result.iloc[0]
        assert isinstance(d, dict)
        assert d["years"] == 2
        assert d["months"] == 6
        assert d["days"] == 0


# ---------------------------------------------------------------------------
# 8. Week truncation for date (line 504)
# ---------------------------------------------------------------------------


class TestDateTruncateWeek:
    """date.truncate('week', ...) truncates to ISO week start (Monday)."""

    def test_week_truncation_monday(self, truncate_date_fn) -> None:
        """2024-01-15 is a Monday -- stays unchanged."""
        unit_s = pd.Series(["week"])
        value_s = pd.Series(["2024-01-15"])
        result = truncate_date_fn(unit_s, value_s)
        assert result.iloc[0] == "2024-01-15"

    def test_week_truncation_thursday(self, truncate_date_fn) -> None:
        """2024-01-18 is a Thursday -- truncates to Monday 2024-01-15."""
        unit_s = pd.Series(["week"])
        value_s = pd.Series(["2024-01-18"])
        result = truncate_date_fn(unit_s, value_s)
        assert result.iloc[0] == "2024-01-15"

    def test_week_truncation_sunday(self, truncate_date_fn) -> None:
        """2024-01-21 is a Sunday -- truncates to Monday 2024-01-15."""
        unit_s = pd.Series(["week"])
        value_s = pd.Series(["2024-01-21"])
        result = truncate_date_fn(unit_s, value_s)
        assert result.iloc[0] == "2024-01-15"

    def test_week_truncation_via_star(self, minimal_context: Context) -> None:
        """End-to-end test through Star.execute_query."""
        star = Star(context=minimal_context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN date.truncate('week', date('2024-01-18')) AS d",
        )
        assert result["d"].iloc[0] == "2024-01-15"


# ---------------------------------------------------------------------------
# 9. Invalid datetime truncation unit (lines 557-561)
# ---------------------------------------------------------------------------


class TestDatetimeTruncateInvalidUnit:
    """Pass invalid unit to datetime.truncate() -- should raise ValueError."""

    def test_invalid_unit_nanosecond(self, truncate_datetime_fn) -> None:
        unit_s = pd.Series(["nanosecond"])
        value_s = pd.Series(["2024-01-15T10:30:00"])
        with pytest.raises(ValueError, match="Unknown truncation unit"):
            truncate_datetime_fn(unit_s, value_s)

    def test_invalid_unit_fortnight(self, truncate_datetime_fn) -> None:
        unit_s = pd.Series(["fortnight"])
        value_s = pd.Series(["2024-01-15T10:30:00"])
        with pytest.raises(ValueError, match="Unknown truncation unit"):
            truncate_datetime_fn(unit_s, value_s)


# ---------------------------------------------------------------------------
# 10. Week truncation for datetime (lines 583-584)
# ---------------------------------------------------------------------------


class TestDatetimeTruncateWeek:
    """datetime.truncate('week', ...) truncates to Monday midnight."""

    def test_week_truncation_thursday(self, truncate_datetime_fn) -> None:
        """2024-01-18T14:30:00 (Thursday) -> 2024-01-15T00:00:00."""
        unit_s = pd.Series(["week"])
        value_s = pd.Series(["2024-01-18T14:30:00"])
        result = truncate_datetime_fn(unit_s, value_s)
        assert "2024-01-15" in str(result.iloc[0])
        assert "00:00:00" in str(result.iloc[0])

    def test_week_truncation_monday_morning(
        self,
        truncate_datetime_fn,
    ) -> None:
        """2024-01-15T10:00:00 (Monday) -> 2024-01-15T00:00:00."""
        unit_s = pd.Series(["week"])
        value_s = pd.Series(["2024-01-15T10:00:00"])
        result = truncate_datetime_fn(unit_s, value_s)
        assert "2024-01-15" in str(result.iloc[0])
        assert "00:00:00" in str(result.iloc[0])

    def test_week_truncation_via_star(self, minimal_context: Context) -> None:
        """End-to-end test through Star.execute_query."""
        star = Star(context=minimal_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN datetime.truncate('week', datetime('2024-01-18T14:30:00')) AS dt",
        )
        val = str(result["dt"].iloc[0])
        assert "2024-01-15" in val
        assert "00:00:00" in val
