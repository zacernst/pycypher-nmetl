"""Tests for CASE expression evaluation — both simple and searched forms.

Cypher supports two CASE forms:

  Searched CASE:  CASE WHEN cond THEN val ... [ELSE default] END
  Simple CASE:    CASE expr WHEN val THEN result ... [ELSE default] END
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def people_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "__ID__": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "age": [30, 25, 35, 28],
            "dept": ["eng", "mktg", "eng", "sales"],
            "salary": [100_000, 80_000, 110_000, 90_000],
        }
    )


@pytest.fixture
def star(people_df: pd.DataFrame) -> Star:
    return Star(context=ContextBuilder.from_dict({"Person": people_df}))


# ===========================================================================
# Searched CASE (CASE WHEN cond THEN val ... END)
# ===========================================================================


class TestSearchedCase:
    """CASE WHEN boolean_condition THEN value ... [ELSE default] END."""

    def test_simple_searched_case_two_branches(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, "
            "CASE WHEN p.age > 28 THEN 'senior' ELSE 'junior' END AS tier "
            "ORDER BY p.name ASC"
        )
        assert list(result["tier"]) == ["senior", "junior", "senior", "junior"]

    def test_searched_case_no_else_returns_null(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, "
            "CASE WHEN p.dept = 'eng' THEN 'Engineering' END AS dept_label "
            "ORDER BY p.name ASC"
        )
        assert result["dept_label"].iloc[0] == "Engineering"  # Alice
        assert result["dept_label"].iloc[1] is None or pd.isna(
            result["dept_label"].iloc[1]
        )  # Bob

    def test_searched_case_multiple_when_clauses(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, "
            "CASE "
            "  WHEN p.age < 27 THEN 'young' "
            "  WHEN p.age < 32 THEN 'mid' "
            "  ELSE 'senior' "
            "END AS tier "
            "ORDER BY p.name ASC"
        )
        tiers = dict(zip(result["name"], result["tier"]))
        assert tiers["Alice"] == "mid"  # 30
        assert tiers["Bob"] == "young"  # 25
        assert tiers["Carol"] == "senior"  # 35
        assert tiers["Dave"] == "mid"  # 28

    def test_searched_case_in_with_clause(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p, CASE WHEN p.salary > 95000 THEN 'high' ELSE 'low' END AS bracket "
            "RETURN p.name AS name, bracket ORDER BY p.name ASC"
        )
        brackets = dict(zip(result["name"], result["bracket"]))
        assert brackets["Alice"] == "high"
        assert brackets["Bob"] == "low"

    def test_searched_case_in_where_clause(self, star: Star) -> None:
        """CASE result used in WHERE."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "WHERE CASE WHEN p.dept = 'eng' THEN p.salary ELSE 0 END > 95000 "
            "RETURN p.name AS name ORDER BY p.name ASC"
        )
        # Engineering people with salary > 95000: Alice (100k) and Carol (110k)
        assert list(result["name"]) == ["Alice", "Carol"]

    def test_searched_case_aggregation(self, star: Star) -> None:
        """sum(CASE WHEN ... THEN ... ELSE 0 END) — conditional aggregation."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH sum(CASE WHEN p.dept = 'eng' THEN p.salary ELSE 0 END) AS eng_total "
            "RETURN eng_total AS eng_total"
        )
        assert result["eng_total"].iloc[0] == pytest.approx(210_000.0)


# ===========================================================================
# Simple CASE (CASE expr WHEN val THEN result ... END)
# ===========================================================================


class TestSimpleCase:
    """CASE expr WHEN value THEN result ... [ELSE default] END."""

    def test_simple_case_dept_mapping(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, "
            "CASE p.dept WHEN 'eng' THEN 'Engineering' WHEN 'mktg' THEN 'Marketing' ELSE 'Other' END AS dept_label "
            "ORDER BY p.name ASC"
        )
        labels = dict(zip(result["name"], result["dept_label"]))
        assert labels["Alice"] == "Engineering"
        assert labels["Bob"] == "Marketing"
        assert labels["Carol"] == "Engineering"
        assert labels["Dave"] == "Other"

    def test_simple_case_no_else_returns_null_on_miss(
        self, star: Star
    ) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, "
            "CASE p.dept WHEN 'eng' THEN 'Engineering' END AS label "
            "ORDER BY p.name ASC"
        )
        assert result["label"].iloc[0] == "Engineering"  # Alice
        assert result["label"].iloc[1] is None or pd.isna(
            result["label"].iloc[1]
        )  # Bob

    def test_simple_case_numeric_discrimination(self, star: Star) -> None:
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name, "
            "CASE p.age WHEN 30 THEN 'thirty' WHEN 25 THEN 'twenty-five' ELSE 'other' END AS age_label "
            "ORDER BY p.name ASC"
        )
        labels = dict(zip(result["name"], result["age_label"]))
        assert labels["Alice"] == "thirty"
        assert labels["Bob"] == "twenty-five"
        assert labels["Carol"] == "other"
