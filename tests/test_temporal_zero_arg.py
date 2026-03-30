"""TDD tests for zero-argument temporal functions.

In Neo4j/openCypher, date(), datetime(), and localdatetime() may be called
with no arguments to return the current date/time:

    RETURN date() AS d          -- current date as 'YYYY-MM-DD'
    RETURN datetime() AS dt     -- current datetime as ISO string
    RETURN localdatetime() AS ldt

All tests written before implementation (TDD step 1).
"""

from __future__ import annotations

import re

import pytest
from pycypher.relational_models import Context, EntityMapping
from pycypher.star import Star


@pytest.fixture
def empty_star() -> Star:
    return Star(context=Context(entity_mapping=EntityMapping(mapping={})))


# ---------------------------------------------------------------------------
# date() with no arguments
# ---------------------------------------------------------------------------


class TestDateNoArgs:
    """date() with no arguments must return the current date."""

    def test_date_no_args_does_not_raise(self, empty_star: Star) -> None:
        """RETURN date() AS d must not raise."""
        empty_star.execute_query("RETURN date() AS d")

    def test_date_no_args_returns_one_row(self, empty_star: Star) -> None:
        """date() returns exactly one row when used in RETURN."""
        result = empty_star.execute_query("RETURN date() AS d")
        assert len(result) == 1

    def test_date_no_args_returns_string(self, empty_star: Star) -> None:
        """date() returns a string value."""
        result = empty_star.execute_query("RETURN date() AS d")
        val = result["d"].iloc[0]
        assert isinstance(str(val), str)
        assert len(str(val)) > 0

    def test_date_no_args_matches_iso_format(self, empty_star: Star) -> None:
        """date() returns a 'YYYY-MM-DD' formatted string."""
        result = empty_star.execute_query("RETURN date() AS d")
        val = str(result["d"].iloc[0])
        assert re.match(r"\d{4}-\d{2}-\d{2}", val), f"Not ISO date: {val!r}"

    def test_date_no_args_year_is_reasonable(self, empty_star: Star) -> None:
        """date() returns a date in 2020–2099 (sanity check)."""
        result = empty_star.execute_query("RETURN date() AS d")
        val = str(result["d"].iloc[0])
        year = int(val[:4])
        assert 2020 <= year <= 2099, f"Unreasonable year: {year}"

    def test_date_with_arg_still_parses_string(self, empty_star: Star) -> None:
        """date('2024-03-15') must still return '2024-03-15'."""
        result = empty_star.execute_query("RETURN date('2024-03-15') AS d")
        val = str(result["d"].iloc[0])
        assert val == "2024-03-15"


# ---------------------------------------------------------------------------
# datetime() with no arguments
# ---------------------------------------------------------------------------


class TestDatetimeNoArgs:
    """datetime() with no arguments must return the current datetime."""

    def test_datetime_no_args_does_not_raise(self, empty_star: Star) -> None:
        """RETURN datetime() AS dt must not raise."""
        empty_star.execute_query("RETURN datetime() AS dt")

    def test_datetime_no_args_returns_one_row(self, empty_star: Star) -> None:
        """datetime() returns exactly one row."""
        result = empty_star.execute_query("RETURN datetime() AS dt")
        assert len(result) == 1

    def test_datetime_no_args_returns_string(self, empty_star: Star) -> None:
        """datetime() returns a non-empty string."""
        result = empty_star.execute_query("RETURN datetime() AS dt")
        val = str(result["dt"].iloc[0])
        assert len(val) > 0

    def test_datetime_no_args_contains_time_component(
        self,
        empty_star: Star,
    ) -> None:
        """datetime() result contains 'T' separator (ISO 8601)."""
        result = empty_star.execute_query("RETURN datetime() AS dt")
        val = str(result["dt"].iloc[0])
        assert "T" in val, f"No time component in: {val!r}"

    def test_datetime_with_arg_still_parses_string(
        self,
        empty_star: Star,
    ) -> None:
        """datetime('2024-03-15T10:30:00') must still parse correctly."""
        result = empty_star.execute_query(
            "RETURN datetime('2024-03-15T10:30:00') AS dt",
        )
        val = str(result["dt"].iloc[0])
        assert "2024-03-15" in val


# ---------------------------------------------------------------------------
# localdatetime() with no arguments
# ---------------------------------------------------------------------------


class TestLocaldatetimeNoArgs:
    """localdatetime() with no arguments must return the current local datetime."""

    def test_localdatetime_no_args_does_not_raise(
        self,
        empty_star: Star,
    ) -> None:
        """RETURN localdatetime() AS ldt must not raise."""
        empty_star.execute_query("RETURN localdatetime() AS ldt")

    def test_localdatetime_no_args_returns_one_row(
        self,
        empty_star: Star,
    ) -> None:
        """localdatetime() returns exactly one row."""
        result = empty_star.execute_query("RETURN localdatetime() AS ldt")
        assert len(result) == 1

    def test_localdatetime_no_args_contains_date(
        self,
        empty_star: Star,
    ) -> None:
        """localdatetime() result contains a date portion."""
        result = empty_star.execute_query("RETURN localdatetime() AS ldt")
        val = str(result["ldt"].iloc[0])
        assert re.match(r"\d{4}-\d{2}-\d{2}", val), f"No date in: {val!r}"
