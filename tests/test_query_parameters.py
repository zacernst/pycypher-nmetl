"""Tests for Cypher query parameters ($name substitution).

Parameters allow safe, typed value injection into queries without
string interpolation::

    star.execute_query(
        "MATCH (p:Person) WHERE p.name = $name RETURN p.age AS age",
        parameters={"name": "Alice"},
    )

Parameters are typed: integer, float, boolean, string, and list values
are all supported.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.ingestion import ContextBuilder


@pytest.fixture
def star() -> Star:
    df = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
            "dept": ["eng", "sales", "eng"],
        }
    )
    return Star(context=ContextBuilder.from_dict({"Person": df}))


class TestQueryParameters:
    """$param substitution in queries."""

    def test_string_parameter_in_where(self, star: Star) -> None:
        """$name string parameter filters correctly in WHERE."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = $name RETURN p.age AS age",
            parameters={"name": "Alice"},
        )
        assert list(result["age"]) == [30]

    def test_integer_parameter_in_where(self, star: Star) -> None:
        """Integer parameter filters correctly in WHERE."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name AS name "
            "ORDER BY name ASC",
            parameters={"min_age": 26},
        )
        assert list(result["name"]) == ["Alice", "Carol"]

    def test_parameter_in_return(self, star: Star) -> None:
        """Parameter used in RETURN expression is interpolated correctly."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN p.name + $suffix AS greeting",
            parameters={"suffix": "!"},
        )
        assert list(result["greeting"]) == ["Alice!"]

    def test_multiple_parameters(self, star: Star) -> None:
        """Multiple parameters in a single query all resolve correctly."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept = $dept AND p.age < $max_age "
            "RETURN p.name AS name ORDER BY name ASC",
            parameters={"dept": "eng", "max_age": 35},
        )
        assert list(result["name"]) == ["Alice"]

    def test_parameter_in_with(self, star: Star) -> None:
        """Parameters work in WITH clause expressions."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "WITH p.age + $bonus AS adjusted_age "
            "RETURN adjusted_age",
            parameters={"bonus": 5},
        )
        assert list(result["adjusted_age"]) == [35]

    def test_missing_parameter_raises(self, star: Star) -> None:
        """A query using an undefined parameter raises KeyError."""
        with pytest.raises((KeyError, ValueError)):
            star.execute_query(
                "MATCH (p:Person) WHERE p.name = $missing RETURN p.age",
                parameters={},
            )
