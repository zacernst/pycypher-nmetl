"""TDD tests for temporal field accessors on date/datetime values.

In Neo4j/openCypher, temporal values expose named fields via property
access syntax:

    date('2024-03-15').year      → 2024
    date('2024-03-15').month     → 3
    date('2024-03-15').day       → 15
    datetime('2024-03-15T10:30:45').hour   → 10
    datetime('2024-03-15T10:30:45').minute → 30
    datetime('2024-03-15T10:30:45').second → 45

    date().year   → current year
    datetime().month → current month

All tests written before implementation (TDD step 1).
"""

from __future__ import annotations

import datetime as _dt

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star


@pytest.fixture
def empty_star() -> Star:
    return Star(context=Context(entity_mapping=EntityMapping(mapping={})))


@pytest.fixture
def event_star() -> Star:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "date_str": ["2024-03-15", "2023-12-25"],
            "dt_str": ["2024-03-15T10:30:45", "2023-12-25T23:59:59"],
        },
    )
    table = EntityTable(
        entity_type="Event",
        identifier="Event",
        column_names=[ID_COLUMN, "date_str", "dt_str"],
        source_obj_attribute_map={"date_str": "date_str", "dt_str": "dt_str"},
        attribute_map={"date_str": "date_str", "dt_str": "dt_str"},
        source_obj=df,
    )
    return Star(
        context=Context(entity_mapping=EntityMapping(mapping={"Event": table})),
    )


# ---------------------------------------------------------------------------
# date literal field accessors (standalone RETURN)
# ---------------------------------------------------------------------------


class TestDateLiteralFieldAccess:
    """date('YYYY-MM-DD').field extracts the correct component."""

    def test_date_year(self, empty_star: Star) -> None:
        """date('2024-03-15').year == 2024."""
        r = empty_star.execute_query("RETURN date('2024-03-15').year AS y")
        assert int(r["y"].iloc[0]) == 2024

    def test_date_month(self, empty_star: Star) -> None:
        """date('2024-03-15').month == 3."""
        r = empty_star.execute_query("RETURN date('2024-03-15').month AS m")
        assert int(r["m"].iloc[0]) == 3

    def test_date_day(self, empty_star: Star) -> None:
        """date('2024-03-15').day == 15."""
        r = empty_star.execute_query("RETURN date('2024-03-15').day AS d")
        assert int(r["d"].iloc[0]) == 15

    def test_date_week(self, empty_star: Star) -> None:
        """date('2024-03-15').week returns the ISO week number."""
        r = empty_star.execute_query("RETURN date('2024-03-15').week AS w")
        expected = _dt.date(2024, 3, 15).isocalendar()[1]
        assert int(r["w"].iloc[0]) == expected

    def test_date_day_of_week(self, empty_star: Star) -> None:
        """date('2024-03-15').dayOfWeek == 5 (Friday, ISO weekday)."""
        r = empty_star.execute_query(
            "RETURN date('2024-03-15').dayOfWeek AS dow",
        )
        # ISO: Mon=1 … Sun=7; 2024-03-15 is a Friday = 5
        assert int(r["dow"].iloc[0]) == 5

    def test_date_day_of_year(self, empty_star: Star) -> None:
        """date('2024-03-15').dayOfYear is correct."""
        r = empty_star.execute_query(
            "RETURN date('2024-03-15').dayOfYear AS doy",
        )
        expected = _dt.date(2024, 3, 15).timetuple().tm_yday
        assert int(r["doy"].iloc[0]) == expected

    def test_date_quarter(self, empty_star: Star) -> None:
        """date('2024-03-15').quarter == 1 (Jan-Mar)."""
        r = empty_star.execute_query("RETURN date('2024-03-15').quarter AS q")
        assert int(r["q"].iloc[0]) == 1

    def test_date_quarter_q4(self, empty_star: Star) -> None:
        """date('2024-12-25').quarter == 4."""
        r = empty_star.execute_query("RETURN date('2024-12-25').quarter AS q")
        assert int(r["q"].iloc[0]) == 4


# ---------------------------------------------------------------------------
# datetime literal field accessors
# ---------------------------------------------------------------------------


class TestDatetimeLiteralFieldAccess:
    """datetime('...').field extracts the correct component."""

    def test_datetime_year(self, empty_star: Star) -> None:
        """datetime('2024-03-15T10:30:45').year == 2024."""
        r = empty_star.execute_query(
            "RETURN datetime('2024-03-15T10:30:45').year AS y",
        )
        assert int(r["y"].iloc[0]) == 2024

    def test_datetime_month(self, empty_star: Star) -> None:
        """datetime('2024-03-15T10:30:45').month == 3."""
        r = empty_star.execute_query(
            "RETURN datetime('2024-03-15T10:30:45').month AS m",
        )
        assert int(r["m"].iloc[0]) == 3

    def test_datetime_day(self, empty_star: Star) -> None:
        """datetime('2024-03-15T10:30:45').day == 15."""
        r = empty_star.execute_query(
            "RETURN datetime('2024-03-15T10:30:45').day AS d",
        )
        assert int(r["d"].iloc[0]) == 15

    def test_datetime_hour(self, empty_star: Star) -> None:
        """datetime('2024-03-15T10:30:45').hour == 10."""
        r = empty_star.execute_query(
            "RETURN datetime('2024-03-15T10:30:45').hour AS h",
        )
        assert int(r["h"].iloc[0]) == 10

    def test_datetime_minute(self, empty_star: Star) -> None:
        """datetime('2024-03-15T10:30:45').minute == 30."""
        r = empty_star.execute_query(
            "RETURN datetime('2024-03-15T10:30:45').minute AS mi",
        )
        assert int(r["mi"].iloc[0]) == 30

    def test_datetime_second(self, empty_star: Star) -> None:
        """datetime('2024-03-15T10:30:45').second == 45."""
        r = empty_star.execute_query(
            "RETURN datetime('2024-03-15T10:30:45').second AS s",
        )
        assert int(r["s"].iloc[0]) == 45

    def test_datetime_millisecond(self, empty_star: Star) -> None:
        """datetime('2024-03-15T10:30:45.123').millisecond == 123."""
        r = empty_star.execute_query(
            "RETURN datetime('2024-03-15T10:30:45.123000').millisecond AS ms",
        )
        assert int(r["ms"].iloc[0]) == 123

    def test_datetime_microsecond(self, empty_star: Star) -> None:
        """datetime('2024-03-15T10:30:45.123456').microsecond == 123456."""
        r = empty_star.execute_query(
            "RETURN datetime('2024-03-15T10:30:45.123456').microsecond AS us",
        )
        assert int(r["us"].iloc[0]) == 123456


# ---------------------------------------------------------------------------
# Zero-arg date()/datetime() field access
# ---------------------------------------------------------------------------


class TestZeroArgDateFieldAccess:
    """date().year / datetime().month etc. use current date/time."""

    def test_date_zero_arg_year(self, empty_star: Star) -> None:
        """date().year returns the current year."""
        r = empty_star.execute_query("RETURN date().year AS y")
        assert int(r["y"].iloc[0]) == _dt.date.today().year

    def test_date_zero_arg_month(self, empty_star: Star) -> None:
        """date().month is in 1..12."""
        r = empty_star.execute_query("RETURN date().month AS m")
        assert 1 <= int(r["m"].iloc[0]) <= 12

    def test_datetime_zero_arg_hour(self, empty_star: Star) -> None:
        """datetime().hour is in 0..23."""
        r = empty_star.execute_query("RETURN datetime().hour AS h")
        assert 0 <= int(r["h"].iloc[0]) <= 23


# ---------------------------------------------------------------------------
# Field access on entity property containing a date string
# ---------------------------------------------------------------------------


class TestEntityDatePropertyFieldAccess:
    """date(e.date_str).year works when property holds a date string."""

    def test_field_via_date_function_on_property(
        self,
        event_star: Star,
    ) -> None:
        """date(e.date_str).year returns correct years for each row."""
        r = event_star.execute_query(
            "MATCH (e:Event) RETURN date(e.date_str).year AS y ORDER BY y",
        )
        years = sorted(int(v) for v in r["y"])
        assert years == [2023, 2024]

    def test_field_via_datetime_function_on_property(
        self,
        event_star: Star,
    ) -> None:
        """datetime(e.dt_str).hour returns correct hours for each row."""
        r = event_star.execute_query(
            "MATCH (e:Event) RETURN datetime(e.dt_str).hour AS h ORDER BY h",
        )
        hours = sorted(int(v) for v in r["h"])
        assert hours == [10, 23]
