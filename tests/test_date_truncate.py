"""TDD tests for date.truncate(), datetime.truncate(), and localdatetime.truncate().

Neo4j 5.x supports temporal truncation:
  date.truncate(unit, temporal)
  datetime.truncate(unit, temporal)
  localdatetime.truncate(unit, temporal)

Supported units (coarsest to finest):
  millennium, century, decade, year, quarter, month, week, day
  hour (datetime/localdatetime only)
  minute (datetime/localdatetime only)
  second (datetime/localdatetime only)
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star


@pytest.fixture
def reg() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


@pytest.fixture
def ctx():
    return ContextBuilder().from_dict(
        {
            "Event": pd.DataFrame(
                {
                    "__ID__": ["e1"],
                    "dt": ["2024-03-15T10:30:45"],
                    "d": ["2024-03-15"],
                }
            )
        }
    )


def _exec_date_truncate(
    reg: ScalarFunctionRegistry, unit: str, value: str
) -> str:
    result = reg.execute(
        "date.truncate",
        [pd.Series([unit]), pd.Series([value])],
    )
    return result.iloc[0]


def _exec_datetime_truncate(
    reg: ScalarFunctionRegistry, unit: str, value: str
) -> str:
    result = reg.execute(
        "datetime.truncate",
        [pd.Series([unit]), pd.Series([value])],
    )
    return result.iloc[0]


# ---------------------------------------------------------------------------
# Registry: functions are registered
# ---------------------------------------------------------------------------


class TestTruncateFunctionsRegistered:
    def test_date_truncate_registered(
        self, reg: ScalarFunctionRegistry
    ) -> None:
        assert "date.truncate" in reg._functions

    def test_datetime_truncate_registered(
        self, reg: ScalarFunctionRegistry
    ) -> None:
        assert "datetime.truncate" in reg._functions

    def test_localdatetime_truncate_registered(
        self, reg: ScalarFunctionRegistry
    ) -> None:
        assert "localdatetime.truncate" in reg._functions


# ---------------------------------------------------------------------------
# date.truncate — truncate a date string to a coarser granularity
# ---------------------------------------------------------------------------


class TestDateTruncate:
    def test_truncate_to_year(self, reg: ScalarFunctionRegistry) -> None:
        assert _exec_date_truncate(reg, "year", "2024-03-15") == "2024-01-01"

    def test_truncate_to_month(self, reg: ScalarFunctionRegistry) -> None:
        assert _exec_date_truncate(reg, "month", "2024-03-15") == "2024-03-01"

    def test_truncate_to_day(self, reg: ScalarFunctionRegistry) -> None:
        """Day truncation is a no-op for a plain date."""
        assert _exec_date_truncate(reg, "day", "2024-03-15") == "2024-03-15"

    def test_truncate_to_decade(self, reg: ScalarFunctionRegistry) -> None:
        assert _exec_date_truncate(reg, "decade", "2024-07-04") == "2020-01-01"

    def test_truncate_to_century(self, reg: ScalarFunctionRegistry) -> None:
        assert (
            _exec_date_truncate(reg, "century", "2024-07-04") == "2001-01-01"
        )

    def test_truncate_to_millennium(self, reg: ScalarFunctionRegistry) -> None:
        assert (
            _exec_date_truncate(reg, "millennium", "2024-07-04")
            == "2001-01-01"
        )

    def test_truncate_to_quarter(self, reg: ScalarFunctionRegistry) -> None:
        # Q1: Jan-Mar, Q2: Apr-Jun, Q3: Jul-Sep, Q4: Oct-Dec
        assert (
            _exec_date_truncate(reg, "quarter", "2024-03-15") == "2024-01-01"
        )
        assert (
            _exec_date_truncate(reg, "quarter", "2024-05-20") == "2024-04-01"
        )
        assert (
            _exec_date_truncate(reg, "quarter", "2024-08-10") == "2024-07-01"
        )
        assert (
            _exec_date_truncate(reg, "quarter", "2024-11-30") == "2024-10-01"
        )

    def test_truncate_unit_case_insensitive(
        self, reg: ScalarFunctionRegistry
    ) -> None:
        assert _exec_date_truncate(reg, "MONTH", "2024-03-15") == "2024-03-01"
        assert _exec_date_truncate(reg, "Year", "2024-03-15") == "2024-01-01"

    def test_null_propagates(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute(
            "date.truncate",
            [pd.Series(["month"]), pd.Series([None])],
        )
        assert result.iloc[0] is None or (result.iloc[0] != result.iloc[0])

    def test_invalid_unit_raises(self, reg: ScalarFunctionRegistry) -> None:
        with pytest.raises((ValueError, KeyError)):
            _exec_date_truncate(reg, "nanosecond", "2024-03-15")


# ---------------------------------------------------------------------------
# datetime.truncate — truncate a datetime string
# ---------------------------------------------------------------------------


class TestDatetimeTruncate:
    def test_truncate_to_year(self, reg: ScalarFunctionRegistry) -> None:
        assert (
            _exec_datetime_truncate(reg, "year", "2024-03-15T10:30:45")
            == "2024-01-01T00:00:00"
        )

    def test_truncate_to_month(self, reg: ScalarFunctionRegistry) -> None:
        assert (
            _exec_datetime_truncate(reg, "month", "2024-03-15T10:30:45")
            == "2024-03-01T00:00:00"
        )

    def test_truncate_to_day(self, reg: ScalarFunctionRegistry) -> None:
        assert (
            _exec_datetime_truncate(reg, "day", "2024-03-15T10:30:45")
            == "2024-03-15T00:00:00"
        )

    def test_truncate_to_hour(self, reg: ScalarFunctionRegistry) -> None:
        assert (
            _exec_datetime_truncate(reg, "hour", "2024-03-15T10:30:45")
            == "2024-03-15T10:00:00"
        )

    def test_truncate_to_minute(self, reg: ScalarFunctionRegistry) -> None:
        assert (
            _exec_datetime_truncate(reg, "minute", "2024-03-15T10:30:45")
            == "2024-03-15T10:30:00"
        )

    def test_truncate_to_second(self, reg: ScalarFunctionRegistry) -> None:
        assert (
            _exec_datetime_truncate(reg, "second", "2024-03-15T10:30:45")
            == "2024-03-15T10:30:45"
        )

    def test_truncate_to_decade(self, reg: ScalarFunctionRegistry) -> None:
        assert (
            _exec_datetime_truncate(reg, "decade", "2024-03-15T10:30:45")
            == "2020-01-01T00:00:00"
        )

    def test_truncate_to_century(self, reg: ScalarFunctionRegistry) -> None:
        assert (
            _exec_datetime_truncate(reg, "century", "2024-03-15T10:30:45")
            == "2001-01-01T00:00:00"
        )

    def test_truncate_to_millennium(self, reg: ScalarFunctionRegistry) -> None:
        assert (
            _exec_datetime_truncate(reg, "millennium", "2024-03-15T10:30:45")
            == "2001-01-01T00:00:00"
        )

    def test_truncate_to_quarter(self, reg: ScalarFunctionRegistry) -> None:
        assert (
            _exec_datetime_truncate(reg, "quarter", "2024-05-20T10:30:45")
            == "2024-04-01T00:00:00"
        )

    def test_null_propagates(self, reg: ScalarFunctionRegistry) -> None:
        result = reg.execute(
            "datetime.truncate",
            [pd.Series(["month"]), pd.Series([None])],
        )
        assert result.iloc[0] is None or (result.iloc[0] != result.iloc[0])


# ---------------------------------------------------------------------------
# Cypher integration: round-trip through query engine
# ---------------------------------------------------------------------------


class TestCypherIntegration:
    def test_date_truncate_month_in_return(self, ctx: ContextBuilder) -> None:
        s = Star(context=ctx)
        result = s.execute_query(
            'MATCH (n:Event) RETURN date.truncate("month", n.d) AS r'
        )
        assert result["r"].iloc[0] == "2024-03-01"

    def test_date_truncate_year_in_return(self, ctx: ContextBuilder) -> None:
        s = Star(context=ctx)
        result = s.execute_query(
            'MATCH (n:Event) RETURN date.truncate("year", n.d) AS r'
        )
        assert result["r"].iloc[0] == "2024-01-01"

    def test_datetime_truncate_day_in_return(
        self, ctx: ContextBuilder
    ) -> None:
        s = Star(context=ctx)
        result = s.execute_query(
            'MATCH (n:Event) RETURN datetime.truncate("day", n.dt) AS r'
        )
        assert result["r"].iloc[0] == "2024-03-15T00:00:00"

    def test_datetime_truncate_hour_in_return(
        self, ctx: ContextBuilder
    ) -> None:
        s = Star(context=ctx)
        result = s.execute_query(
            'MATCH (n:Event) RETURN datetime.truncate("hour", n.dt) AS r'
        )
        assert result["r"].iloc[0] == "2024-03-15T10:00:00"

    def test_date_truncate_in_where(self, ctx: ContextBuilder) -> None:
        s = Star(context=ctx)
        result = s.execute_query(
            'MATCH (n:Event) WHERE date.truncate("month", n.d) = "2024-03-01" RETURN n.d'
        )
        assert len(result) == 1
