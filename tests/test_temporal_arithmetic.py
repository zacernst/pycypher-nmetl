"""TDD tests for temporal arithmetic (Loop 180).

Neo4j 5.x temporal arithmetic operations currently absent from pycypher:

    date + duration      → date
    date - duration      → date
    date - date          → duration
    datetime + duration  → datetime
    datetime - duration  → datetime
    datetime - datetime  → duration
    duration + duration  → duration
    duration - duration  → duration
    duration * number    → duration  (scaling)
    number * duration    → duration

Use cases that are currently blocked without these:
  - Time-window filters: WHERE r.created_at > date() - duration({days: 30})
  - Expiry calculation: RETURN order.placed_at + duration({days: 14}) AS due
  - Age computation:    RETURN date() - person.born AS age
  - Batch scheduling:   RETURN start_date + duration({weeks: n}) AS next_batch

All tests are written before the implementation (TDD red phase).
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Helpers — reuse the Star fixture pattern to run standalone RETURN queries
# ---------------------------------------------------------------------------


def _star_empty():
    """Return a Star instance with an empty context (for expression-only queries)."""
    from pycypher.relational_models import (
        Context,
        EntityMapping,
        RelationshipMapping,
    )
    from pycypher.star import Star

    ctx = Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )
    return Star(context=ctx)


def _q(cypher: str):
    """Execute a standalone RETURN query and return the first row as a dict."""
    star = _star_empty()
    result = star.execute_query(cypher)
    return result.iloc[0].to_dict()


# ===========================================================================
# Category 1 — date + duration
# ===========================================================================


class TestDatePlusDuration:
    """date('YYYY-MM-DD') + duration({...}) must return a new ISO date string."""

    def test_add_days(self) -> None:
        row = _q("RETURN date('2024-01-15') + duration({days: 5}) AS d")
        assert row["d"] == "2024-01-20"

    def test_add_days_crossing_month_boundary(self) -> None:
        row = _q("RETURN date('2024-01-28') + duration({days: 5}) AS d")
        assert row["d"] == "2024-02-02"

    def test_add_one_month(self) -> None:
        row = _q("RETURN date('2024-01-15') + duration({months: 1}) AS d")
        assert row["d"] == "2024-02-15"

    def test_add_months_crossing_year_boundary(self) -> None:
        row = _q("RETURN date('2024-11-15') + duration({months: 3}) AS d")
        assert row["d"] == "2025-02-15"

    def test_add_one_year(self) -> None:
        row = _q("RETURN date('2024-01-15') + duration({years: 1}) AS d")
        assert row["d"] == "2025-01-15"

    def test_add_month_clamps_at_end_of_month(self) -> None:
        """Jan 31 + 1 month must clamp to Feb 29 in a leap year."""
        row = _q("RETURN date('2024-01-31') + duration({months: 1}) AS d")
        assert row["d"] == "2024-02-29"

    def test_add_month_clamps_non_leap(self) -> None:
        """Jan 31 + 1 month must clamp to Feb 28 in a non-leap year."""
        row = _q("RETURN date('2023-01-31') + duration({months: 1}) AS d")
        assert row["d"] == "2023-02-28"

    def test_add_combined_years_months_days(self) -> None:
        row = _q(
            "RETURN date('2024-01-01') + duration({years: 1, months: 2, days: 3}) AS d",
        )
        assert row["d"] == "2025-03-04"

    def test_add_weeks(self) -> None:
        row = _q("RETURN date('2024-01-01') + duration({weeks: 2}) AS d")
        assert row["d"] == "2024-01-15"

    def test_duration_plus_date_is_commutative(self) -> None:
        """Duration + date must equal date + duration."""
        row = _q("RETURN duration({days: 5}) + date('2024-01-15') AS d")
        assert row["d"] == "2024-01-20"


# ===========================================================================
# Category 2 — date - duration
# ===========================================================================


class TestDateMinusDuration:
    """date('YYYY-MM-DD') - duration({...}) must return a new ISO date string."""

    def test_subtract_days(self) -> None:
        row = _q("RETURN date('2024-01-20') - duration({days: 5}) AS d")
        assert row["d"] == "2024-01-15"

    def test_subtract_days_crossing_month_boundary(self) -> None:
        row = _q("RETURN date('2024-02-02') - duration({days: 5}) AS d")
        assert row["d"] == "2024-01-28"

    def test_subtract_one_month(self) -> None:
        row = _q("RETURN date('2024-02-15') - duration({months: 1}) AS d")
        assert row["d"] == "2024-01-15"

    def test_subtract_months_crossing_year_boundary(self) -> None:
        row = _q("RETURN date('2025-02-15') - duration({months: 3}) AS d")
        assert row["d"] == "2024-11-15"

    def test_subtract_one_year(self) -> None:
        row = _q("RETURN date('2025-01-15') - duration({years: 1}) AS d")
        assert row["d"] == "2024-01-15"

    def test_subtract_clamps_at_end_of_month(self) -> None:
        """Mar 31 - 1 month must clamp to Feb 29 (2024 is leap year)."""
        row = _q("RETURN date('2024-03-31') - duration({months: 1}) AS d")
        assert row["d"] == "2024-02-29"

    def test_subtract_weeks(self) -> None:
        row = _q("RETURN date('2024-01-15') - duration({weeks: 2}) AS d")
        assert row["d"] == "2024-01-01"


# ===========================================================================
# Category 3 — date - date → duration dict
# ===========================================================================


class TestDateMinusDate:
    """date1 - date2 must return a duration dict with a 'days' component."""

    def test_positive_difference(self) -> None:
        row = _q("RETURN date('2024-01-20') - date('2024-01-15') AS dur")
        d = row["dur"]
        assert isinstance(d, dict)
        assert d["days"] == 5

    def test_zero_difference(self) -> None:
        row = _q("RETURN date('2024-01-15') - date('2024-01-15') AS dur")
        d = row["dur"]
        assert d["days"] == 0

    def test_negative_difference(self) -> None:
        row = _q("RETURN date('2024-01-15') - date('2024-01-20') AS dur")
        d = row["dur"]
        assert d["days"] == -5

    def test_crossing_year_boundary(self) -> None:
        row = _q("RETURN date('2025-01-01') - date('2024-01-01') AS dur")
        d = row["dur"]
        assert d["days"] == 366  # 2024 is a leap year, so 366 days

    def test_result_is_duration_dict_with_required_keys(self) -> None:
        """The result must have all standard duration component keys."""
        row = _q("RETURN date('2024-01-20') - date('2024-01-15') AS dur")
        d = row["dur"]
        for key in (
            "years",
            "months",
            "weeks",
            "days",
            "hours",
            "minutes",
            "seconds",
        ):
            assert key in d, f"Missing key: {key}"


# ===========================================================================
# Category 4 — datetime + duration
# ===========================================================================


class TestDatetimePlusDuration:
    """datetime('...') + duration({...}) must return a new ISO datetime string."""

    def test_add_hours(self) -> None:
        row = _q(
            "RETURN datetime('2024-01-15T10:00:00') + duration({hours: 3}) AS dt",
        )
        assert row["dt"] == "2024-01-15T13:00:00"

    def test_add_hours_crossing_midnight(self) -> None:
        row = _q(
            "RETURN datetime('2024-01-15T23:00:00') + duration({hours: 3}) AS dt",
        )
        assert row["dt"] == "2024-01-16T02:00:00"

    def test_add_days_to_datetime(self) -> None:
        row = _q(
            "RETURN datetime('2024-01-15T12:00:00') + duration({days: 2}) AS dt",
        )
        assert row["dt"] == "2024-01-17T12:00:00"

    def test_add_minutes(self) -> None:
        row = _q(
            "RETURN datetime('2024-01-15T10:00:00') + duration({minutes: 90}) AS dt",
        )
        assert row["dt"] == "2024-01-15T11:30:00"

    def test_add_months_to_datetime(self) -> None:
        row = _q(
            "RETURN datetime('2024-01-15T10:00:00') + duration({months: 2}) AS dt",
        )
        assert row["dt"] == "2024-03-15T10:00:00"


# ===========================================================================
# Category 5 — datetime - duration
# ===========================================================================


class TestDatetimeMinusDuration:
    """datetime('...') - duration({...}) must return a new ISO datetime string."""

    def test_subtract_hours(self) -> None:
        row = _q(
            "RETURN datetime('2024-01-15T13:00:00') - duration({hours: 3}) AS dt",
        )
        assert row["dt"] == "2024-01-15T10:00:00"

    def test_subtract_days(self) -> None:
        row = _q(
            "RETURN datetime('2024-01-17T12:00:00') - duration({days: 2}) AS dt",
        )
        assert row["dt"] == "2024-01-15T12:00:00"


# ===========================================================================
# Category 6 — datetime - datetime → duration
# ===========================================================================


class TestDatetimeMinusDatetime:
    """datetime1 - datetime2 must return a duration dict."""

    def test_same_datetime(self) -> None:
        row = _q(
            "RETURN datetime('2024-01-15T10:00:00') - datetime('2024-01-15T10:00:00') AS dur",
        )
        d = row["dur"]
        assert d["days"] == 0
        assert d["seconds"] == 0

    def test_two_hours_difference(self) -> None:
        row = _q(
            "RETURN datetime('2024-01-15T12:00:00') - datetime('2024-01-15T10:00:00') AS dur",
        )
        d = row["dur"]
        # 2 hours = 7200 seconds
        assert d["days"] == 0
        assert d["seconds"] == 7200

    def test_one_day_difference(self) -> None:
        row = _q(
            "RETURN datetime('2024-01-16T10:00:00') - datetime('2024-01-15T10:00:00') AS dur",
        )
        d = row["dur"]
        assert d["days"] == 1
        assert d["seconds"] == 0


# ===========================================================================
# Category 7 — duration + duration and duration - duration
# ===========================================================================


class TestDurationArithmetic:
    """duration + duration and duration - duration must combine components."""

    def test_add_durations(self) -> None:
        row = _q("RETURN duration({days: 3}) + duration({days: 4}) AS dur")
        d = row["dur"]
        assert d["days"] == 7

    def test_add_durations_mixed_components(self) -> None:
        row = _q(
            "RETURN duration({months: 1, days: 5}) + duration({months: 2, days: 3}) AS dur",
        )
        d = row["dur"]
        assert d["months"] == 3
        assert d["days"] == 8

    def test_subtract_durations(self) -> None:
        row = _q("RETURN duration({days: 10}) - duration({days: 4}) AS dur")
        d = row["dur"]
        assert d["days"] == 6

    def test_subtract_durations_produces_negative_component(self) -> None:
        row = _q("RETURN duration({days: 3}) - duration({days: 7}) AS dur")
        d = row["dur"]
        assert d["days"] == -4


# ===========================================================================
# Category 8 — Null propagation
# ===========================================================================


class TestTemporalArithmeticNullPropagation:
    """When either operand is null, temporal arithmetic must return null."""

    def test_null_date_plus_duration(self) -> None:
        """date(null) + duration({days:1}) → null."""
        import pandas as _pd
        import pandas as pd
        from pycypher.relational_models import (
            ID_COLUMN,
            Context,
            EntityMapping,
            EntityTable,
            RelationshipMapping,
        )
        from pycypher.star import Star

        df = _pd.DataFrame({ID_COLUMN: [1], "birth": [None]})
        table = EntityTable(
            entity_type="P",
            identifier="P",
            column_names=[ID_COLUMN, "birth"],
            source_obj_attribute_map={"birth": "birth"},
            attribute_map={"birth": "birth"},
            source_obj=df,
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"P": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:P) RETURN p.birth + duration({days: 1}) AS d",
        )
        assert pd.isna(result["d"].iloc[0])

    def test_date_minus_null_duration_is_null(self) -> None:
        """Date + null → null (when duration value is null)."""
        import pandas as _pd
        import pandas as pd
        from pycypher.relational_models import (
            ID_COLUMN,
            Context,
            EntityMapping,
            EntityTable,
            RelationshipMapping,
        )
        from pycypher.star import Star

        df = _pd.DataFrame({ID_COLUMN: [1], "n": [None]})
        table = EntityTable(
            entity_type="P",
            identifier="P",
            column_names=[ID_COLUMN, "n"],
            source_obj_attribute_map={"n": "n"},
            attribute_map={"n": "n"},
            source_obj=df,
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"P": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:P) RETURN date('2024-01-01') + duration(p.n) AS d",
        )
        assert pd.isna(result["d"].iloc[0])


# ===========================================================================
# Category 9 — Cypher integration via Star.execute_query
# ===========================================================================


class TestTemporalArithmeticIntegration:
    """End-to-end Cypher queries using temporal arithmetic."""

    @pytest.fixture
    def star_with_events(self):
        """Context with an Events entity table containing date columns."""
        import pandas as pd
        from pycypher.relational_models import (
            ID_COLUMN,
            Context,
            EntityMapping,
            EntityTable,
            RelationshipMapping,
        )
        from pycypher.star import Star

        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alpha", "Beta", "Gamma"],
                "event_date": ["2024-01-10", "2024-02-15", "2024-03-20"],
            },
        )
        table = EntityTable(
            entity_type="Event",
            identifier="Event",
            column_names=[ID_COLUMN, "name", "event_date"],
            source_obj_attribute_map={
                "name": "name",
                "event_date": "event_date",
            },
            attribute_map={"name": "name", "event_date": "event_date"},
            source_obj=df,
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Event": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
        return Star(context=ctx)

    def test_compute_due_date_in_return(self, star_with_events) -> None:
        """RETURN event.date + duration({days: 30}) AS due_date."""
        result = star_with_events.execute_query(
            "MATCH (e:Event) WHERE e.name = 'Alpha' "
            "RETURN e.event_date + duration({days: 30}) AS due",
        )
        assert result["due"].iloc[0] == "2024-02-09"

    def test_filter_recent_events_with_date_arithmetic(
        self,
        star_with_events,
    ) -> None:
        """Filter events within Jan 1 – Mar 1 2024 (exclusive upper bound)."""
        # Window: date > 2024-01-01  AND  date < 2024-01-01 + 2 months = 2024-03-01
        # Alpha: Jan 10 → in window
        # Beta:  Feb 15 → in window (Feb 15 < Mar 1)
        # Gamma: Mar 20 → excluded (Mar 20 > Mar 1)
        result = star_with_events.execute_query(
            "MATCH (e:Event) "
            "WHERE e.event_date > date('2024-01-01') - duration({days: 0}) "
            "AND e.event_date < date('2024-01-01') + duration({months: 2}) "
            "RETURN e.name AS n ORDER BY e.name",
        )
        names = list(result["n"])
        assert "Alpha" in names
        assert "Beta" in names
        assert "Gamma" not in names

    def test_standalone_date_arithmetic_return(self) -> None:
        """Pure expression RETURN without MATCH."""
        star = _star_empty()
        result = star.execute_query(
            "RETURN date('2024-06-01') + duration({months: 6}) AS d",
        )
        assert result["d"].iloc[0] == "2024-12-01"

    def test_standalone_date_minus_date(self) -> None:
        """Date - date in standalone RETURN."""
        star = _star_empty()
        result = star.execute_query(
            "RETURN date('2024-02-01') - date('2024-01-01') AS dur",
        )
        d = result["dur"].iloc[0]
        assert d["days"] == 31  # January has 31 days

    def test_chained_arithmetic(self) -> None:
        """(date + duration) - duration must round-trip."""
        star = _star_empty()
        result = star.execute_query(
            "RETURN date('2024-06-15') + duration({months: 3}) - duration({months: 3}) AS d",
        )
        # Should round-trip back to the original date
        assert result["d"].iloc[0] == "2024-06-15"
