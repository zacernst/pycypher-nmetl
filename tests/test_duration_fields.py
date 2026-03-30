"""TDD tests for duration() full field support.

In Neo4j/openCypher, duration values expose all components:
    duration('P1Y').years   → 1
    duration('P2M').months  → 2
    duration('PT5H').hours  → 5
    duration({days: 3}).days → 3

Currently the implementation converts to timedelta and loses components.

Written before the fix (TDD red phase).

Run with:
    uv run pytest tests/test_duration_fields.py -v
"""

from __future__ import annotations

import pandas as pd
from pycypher import Star
from pycypher.ingestion import ContextBuilder
from pycypher.scalar_functions import ScalarFunctionRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reg() -> ScalarFunctionRegistry:
    ScalarFunctionRegistry._instance = None
    return ScalarFunctionRegistry.get_instance()


def _dur(iso_or_map: object) -> dict:
    """Call duration() on a single value and return the dict result."""
    reg = _reg()
    result = reg.execute("duration", [pd.Series([iso_or_map])])
    return result.iloc[0]


def _empty_star() -> Star:
    import pandas as pd

    people = pd.DataFrame({"__ID__": ["p1"], "name": ["Alice"]})
    return Star(context=ContextBuilder.from_dict({"Person": people}))


# ---------------------------------------------------------------------------
# ISO string form — individual component fields
# ---------------------------------------------------------------------------


class TestDurationISO:
    def test_years_component(self) -> None:
        """duration('P1Y').years → 1"""
        assert _dur("P1Y")["years"] == 1

    def test_months_component(self) -> None:
        """duration('P2M').months → 2"""
        assert _dur("P2M")["months"] == 2

    def test_days_component(self) -> None:
        """duration('P3D').days → 3"""
        assert _dur("P3D")["days"] == 3

    def test_weeks_component(self) -> None:
        """duration('P4W').weeks → 4"""
        assert _dur("P4W")["weeks"] == 4

    def test_hours_component(self) -> None:
        """duration('PT5H').hours → 5"""
        assert _dur("PT5H")["hours"] == 5

    def test_minutes_component(self) -> None:
        """duration('PT6M').minutes → 6"""
        assert _dur("PT6M")["minutes"] == 6

    def test_seconds_component(self) -> None:
        """duration('PT7S').seconds → 7"""
        assert _dur("PT7S")["seconds"] == 7

    def test_full_iso_all_components(self) -> None:
        """duration('P1Y2M3DT4H5M6S') exposes all components correctly."""
        d = _dur("P1Y2M3DT4H5M6S")
        assert d["years"] == 1
        assert d["months"] == 2
        assert d["days"] == 3
        assert d["hours"] == 4
        assert d["minutes"] == 5
        assert d["seconds"] == 6

    def test_zero_adjacent_field(self) -> None:
        """duration('P1Y').months → 0 (not contaminating adjacent fields)."""
        d = _dur("P1Y")
        assert d["months"] == 0
        assert d["days"] == 0

    def test_null_propagates(self) -> None:
        """duration(null) → null."""
        reg = _reg()
        result = reg.execute("duration", [pd.Series([None])])
        assert result.iloc[0] is None

    def test_weeks_with_days(self) -> None:
        """duration('P1W3D') has weeks=1 and days=3 (not merged)."""
        d = _dur("P1W3D")
        assert d["weeks"] == 1
        assert d["days"] == 3


# ---------------------------------------------------------------------------
# Map form — duration({years: 1, months: 2, ...})
# ---------------------------------------------------------------------------


class TestDurationMapForm:
    def test_map_months_only(self) -> None:
        """duration({months: 6}) → dict with months=6, other fields=0."""
        d = _dur({"months": 6})
        assert d["months"] == 6
        assert d["days"] == 0
        assert d["years"] == 0

    def test_map_years_and_hours(self) -> None:
        """duration({years: 1, hours: 12}) → correct separate fields."""
        d = _dur({"years": 1, "hours": 12})
        assert d["years"] == 1
        assert d["hours"] == 12
        assert d["months"] == 0

    def test_map_all_fields(self) -> None:
        """duration({years:1, months:2, days:3, hours:4, minutes:5, seconds:6})."""
        d = _dur(
            {
                "years": 1,
                "months": 2,
                "days": 3,
                "hours": 4,
                "minutes": 5,
                "seconds": 6,
            },
        )
        assert d["years"] == 1
        assert d["months"] == 2
        assert d["days"] == 3
        assert d["hours"] == 4
        assert d["minutes"] == 5
        assert d["seconds"] == 6

    def test_map_empty_defaults_to_zero(self) -> None:
        """duration({}) → all zero fields."""
        d = _dur({})
        assert d["years"] == 0
        assert d["days"] == 0
        assert d["hours"] == 0


# ---------------------------------------------------------------------------
# Cypher integration
# ---------------------------------------------------------------------------


class TestDurationCypherIntegration:
    def test_days_field_in_return(self) -> None:
        """RETURN duration('P5D').days AS d → 5."""
        star = _empty_star()
        result = star.execute_query("RETURN duration('P5D').days AS d")
        assert result["d"].iloc[0] == 5

    def test_hours_field_in_return(self) -> None:
        """RETURN duration('PT3H').hours AS h → 3."""
        star = _empty_star()
        result = star.execute_query("RETURN duration('PT3H').hours AS h")
        assert result["h"].iloc[0] == 3

    def test_years_field_in_return(self) -> None:
        """RETURN duration('P2Y').years AS y → 2."""
        star = _empty_star()
        result = star.execute_query("RETURN duration('P2Y').years AS y")
        assert result["y"].iloc[0] == 2

    def test_map_form_in_return(self) -> None:
        """RETURN duration({months: 2}).months AS m → 2."""
        star = _empty_star()
        result = star.execute_query("RETURN duration({months: 2}).months AS m")
        assert result["m"].iloc[0] == 2

    def test_duration_in_where_clause(self) -> None:
        """MATCH (n:Person) WHERE duration('PT1H').hours > 0 RETURN n.name."""
        star = _empty_star()
        result = star.execute_query(
            "MATCH (n:Person) WHERE duration('PT1H').hours > 0 RETURN n.name AS name",
        )
        assert len(result) == 1
