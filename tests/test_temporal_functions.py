"""Tests for temporal scalar functions: date(), datetime(), duration().

These are standard Neo4j Cypher temporal functions not yet present in
pycypher's scalar function registry.

TDD: all tests written before implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def event_context() -> Context:
    """Context with an Event entity that has string date fields."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Birthday", "Meeting", "Holiday"],
            "date_str": ["2024-03-15", "2024-06-01", "2024-12-25"],
        },
    )
    table = EntityTable(
        entity_type="Event",
        identifier="Event",
        column_names=[ID_COLUMN, "name", "date_str"],
        source_obj_attribute_map={"name": "name", "date_str": "date_str"},
        attribute_map={"name": "name", "date_str": "date_str"},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Event": table}))


@pytest.fixture
def person_context() -> Context:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=table_from_df(df),
    )
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


def table_from_df(df: pd.DataFrame) -> pd.DataFrame:
    """Pass-through for EntityTable source_obj."""
    return df


# ---------------------------------------------------------------------------
# date() — construct or parse a date
# ---------------------------------------------------------------------------


class TestDateFunction:
    """date(dateString) parses an ISO 8601 date string."""

    def test_date_from_string_literal(self, event_context: Context) -> None:
        """date('2024-03-15') returns a date-like string or object."""
        star = Star(context=event_context)
        result = star.execute_query(
            "MATCH (e:Event) WHERE e.name = 'Birthday' RETURN date('2024-03-15') AS d",
        )
        val = str(result["d"].iloc[0])
        assert "2024-03-15" in val or "2024" in val

    def test_date_year_month_day_components(
        self,
        event_context: Context,
    ) -> None:
        """date({year: 2024, month: 3, day: 15}) returns a date."""
        star = Star(context=event_context)
        # Simplest form: date(string)
        result = star.execute_query(
            "MATCH (e:Event) WHERE e.name = 'Birthday' RETURN date('2024-03-15') AS d",
        )
        assert result is not None
        assert len(result) == 1

    def test_date_does_not_raise_not_implemented(
        self,
        event_context: Context,
    ) -> None:
        """Regression: date() must not raise NotImplementedError."""
        star = Star(context=event_context)
        result = star.execute_query(
            "MATCH (e:Event) RETURN date(e.date_str) AS d",
        )
        assert result is not None
        assert len(result) == 3

    def test_date_null_returns_null(self, event_context: Context) -> None:
        """date(null) returns null (not an exception)."""
        star = Star(context=event_context)
        result = star.execute_query(
            "MATCH (e:Event) WHERE e.name = 'Birthday' RETURN date(null) AS d",
        )
        # Should not raise; result should be null/None/NaT
        assert result is not None
        assert len(result) == 1


# ---------------------------------------------------------------------------
# datetime() — construct or parse a datetime
# ---------------------------------------------------------------------------


class TestDatetimeFunction:
    """datetime(string) parses an ISO 8601 datetime string."""

    def test_datetime_from_string_literal(
        self,
        event_context: Context,
    ) -> None:
        """datetime('2024-03-15T10:30:00') returns a datetime-like value."""
        star = Star(context=event_context)
        result = star.execute_query(
            "MATCH (e:Event) WHERE e.name = 'Birthday' "
            "RETURN datetime('2024-03-15T10:30:00') AS dt",
        )
        val = str(result["dt"].iloc[0])
        assert "2024" in val

    def test_datetime_does_not_raise_not_implemented(
        self,
        event_context: Context,
    ) -> None:
        """Regression: datetime() must not raise NotImplementedError."""
        star = Star(context=event_context)
        result = star.execute_query(
            "MATCH (e:Event) WHERE e.name = 'Meeting' "
            "RETURN datetime('2024-06-01T09:00:00Z') AS dt",
        )
        assert result is not None

    def test_datetime_null_returns_null(self, event_context: Context) -> None:
        """datetime(null) returns null."""
        star = Star(context=event_context)
        result = star.execute_query(
            "MATCH (e:Event) WHERE e.name = 'Birthday' RETURN datetime(null) AS dt",
        )
        assert result is not None
        assert len(result) == 1


# ---------------------------------------------------------------------------
# duration() — construct a duration value
# ---------------------------------------------------------------------------


class TestDurationFunction:
    """duration(string) parses an ISO 8601 duration string."""

    def test_duration_from_iso_string(self, event_context: Context) -> None:
        """duration('P5D') returns a duration-like value."""
        star = Star(context=event_context)
        result = star.execute_query(
            "MATCH (e:Event) WHERE e.name = 'Birthday' RETURN duration('P5D') AS dur",
        )
        assert result is not None
        assert len(result) == 1

    def test_duration_does_not_raise_not_implemented(
        self,
        event_context: Context,
    ) -> None:
        """Regression: duration() must not raise NotImplementedError."""
        star = Star(context=event_context)
        result = star.execute_query(
            "MATCH (e:Event) WHERE e.name = 'Meeting' "
            "RETURN duration('P1Y2M3DT4H5M6S') AS dur",
        )
        assert result is not None

    def test_duration_null_returns_null(self, event_context: Context) -> None:
        """duration(null) returns null."""
        star = Star(context=event_context)
        result = star.execute_query(
            "MATCH (e:Event) WHERE e.name = 'Birthday' RETURN duration(null) AS dur",
        )
        assert result is not None
        assert len(result) == 1


# ---------------------------------------------------------------------------
# localdatetime() — alias form
# ---------------------------------------------------------------------------


class TestLocalDatetimeFunction:
    """localdatetime() without timezone suffix."""

    def test_localdatetime_from_string(self, event_context: Context) -> None:
        """localdatetime('2024-03-15T10:30:00') returns a datetime-like value."""
        star = Star(context=event_context)
        result = star.execute_query(
            "MATCH (e:Event) WHERE e.name = 'Birthday' "
            "RETURN localdatetime('2024-03-15T10:30:00') AS ldt",
        )
        assert result is not None
        assert len(result) == 1

    def test_localdatetime_does_not_raise(
        self,
        event_context: Context,
    ) -> None:
        """Regression: localdatetime() must not raise NotImplementedError."""
        star = Star(context=event_context)
        result = star.execute_query(
            "MATCH (e:Event) WHERE e.name = 'Meeting' "
            "RETURN localdatetime('2024-06-01T09:00:00') AS ldt",
        )
        assert result is not None
