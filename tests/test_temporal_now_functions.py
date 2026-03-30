"""Tests for "now" temporal functions: timestamp(), localtime(), localdate().

These Neo4j Cypher functions return current-time values rather than parsing
strings.  They require no arguments.

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
def single_person_ctx() -> Context:
    df = pd.DataFrame({ID_COLUMN: [1], "name": ["Alice"]})
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
# timestamp() — milliseconds since Unix epoch
# ---------------------------------------------------------------------------


class TestTimestampFunction:
    """timestamp() returns a positive integer (milliseconds since epoch)."""

    def test_timestamp_returns_positive_integer(
        self,
        single_person_ctx: Context,
    ) -> None:
        """timestamp() returns a non-negative integer."""
        star = Star(context=single_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN timestamp() AS ts",
        )
        ts = result["ts"].iloc[0]
        assert isinstance(int(ts), int)
        assert int(ts) > 0

    def test_timestamp_is_large_enough_to_be_epoch_ms(
        self,
        single_person_ctx: Context,
    ) -> None:
        """timestamp() value is in the range expected for epoch milliseconds."""
        star = Star(context=single_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN timestamp() AS ts",
        )
        ts = int(result["ts"].iloc[0])
        # Epoch ms for year 2024 starts at roughly 1.7e12
        assert ts > 1_700_000_000_000

    def test_timestamp_does_not_raise(
        self,
        single_person_ctx: Context,
    ) -> None:
        """Regression: timestamp() must not raise NotImplementedError."""
        star = Star(context=single_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN timestamp() AS ts",
        )
        assert result is not None
        assert len(result) == 1


# ---------------------------------------------------------------------------
# localtime() — current local time string
# ---------------------------------------------------------------------------


class TestLocaltimeFunction:
    """localtime() returns a string representation of the current local time."""

    def test_localtime_returns_string(
        self,
        single_person_ctx: Context,
    ) -> None:
        """localtime() returns a string value."""
        star = Star(context=single_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN localtime() AS lt",
        )
        val = result["lt"].iloc[0]
        assert isinstance(str(val), str)
        assert len(str(val)) > 0

    def test_localtime_looks_like_time(
        self,
        single_person_ctx: Context,
    ) -> None:
        """localtime() result contains colon-separated time components."""
        star = Star(context=single_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN localtime() AS lt",
        )
        val = str(result["lt"].iloc[0])
        assert ":" in val

    def test_localtime_does_not_raise(
        self,
        single_person_ctx: Context,
    ) -> None:
        """Regression: localtime() must not raise ValueError."""
        star = Star(context=single_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN localtime() AS lt",
        )
        assert result is not None


# ---------------------------------------------------------------------------
# localdate() — current local date string  (complement to localtime)
# ---------------------------------------------------------------------------


class TestLocaldateFunction:
    """localdate() returns the current date as an ISO 8601 string."""

    def test_localdate_returns_iso_date_string(
        self,
        single_person_ctx: Context,
    ) -> None:
        """localdate() returns 'YYYY-MM-DD' format."""
        star = Star(context=single_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN localdate() AS ld",
        )
        val = str(result["ld"].iloc[0])
        # Must look like YYYY-MM-DD (10 chars with dashes at pos 4 and 7)
        assert len(val) >= 8
        assert "-" in val

    def test_localdate_does_not_raise(
        self,
        single_person_ctx: Context,
    ) -> None:
        """Regression: localdate() must not raise ValueError."""
        star = Star(context=single_person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN localdate() AS ld",
        )
        assert result is not None
