"""End-to-end test suite for the social network example project.

Validates that the ``examples/social_network/`` demo works correctly and
continues to work as the codebase evolves.  Each test class corresponds to
a section of ``run_demo.py``.

Data layout (from ``examples/social_network/data/``):
    - people.csv      — 12 Person nodes  (p1–p12)
    - companies.csv   —  5 Company nodes  (c1–c5)
    - knows.csv       — 18 KNOWS relationships
    - works_at.csv    — 12 WORKS_AT relationships

Column naming: the engine returns aliased columns by their alias
(``name``, ``city_upper``).  Un-aliased property returns use the bare
property name (``name`` not ``p.name``).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from pycypher.ingestion import ContextBuilder
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_DATA_DIR = (
    Path(__file__).resolve().parent.parent
    / "examples"
    / "social_network"
    / "data"
)


# ---------------------------------------------------------------------------
# Fixtures — shared across all test classes
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def social_star() -> Star:
    """Build a Star from the social-network CSV data.

    Uses ``ContextBuilder.from_dict()`` — the same approach as the demo script.
    Module-scoped so the CSV-load cost is paid once per test module.
    """
    people = pd.read_csv(_DATA_DIR / "people.csv")
    companies = pd.read_csv(_DATA_DIR / "companies.csv")
    knows = pd.read_csv(_DATA_DIR / "knows.csv")
    works_at = pd.read_csv(_DATA_DIR / "works_at.csv")

    context = ContextBuilder.from_dict(
        {
            "Person": people,
            "Company": companies,
            "KNOWS": knows,
            "WORKS_AT": works_at,
        },
    )
    return Star(context=context)


def _q(star: Star, cypher: str) -> pd.DataFrame:
    """Convenience: execute *cypher* and return the result DataFrame."""
    return star.execute_query(cypher)


# ---------------------------------------------------------------------------
# Section 1 — Data Loading
# ---------------------------------------------------------------------------


class TestDataLoading:
    """Verify the CSV data loads correctly into the context."""

    def test_person_entity_count(self, social_star: Star) -> None:
        """12 Person nodes should be loaded."""
        df = _q(social_star, "MATCH (p:Person) RETURN p.name AS name")
        assert len(df) == 12

    def test_company_entity_count(self, social_star: Star) -> None:
        """5 Company nodes should be loaded."""
        df = _q(social_star, "MATCH (c:Company) RETURN c.name AS name")
        assert len(df) == 5

    def test_person_has_expected_properties(self, social_star: Star) -> None:
        """Person nodes expose name, age, city, email, join_date."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)
            RETURN p.name AS name, p.age AS age, p.city AS city,
                   p.email AS email, p.join_date AS join_date
            """,
        )
        assert set(df.columns) >= {"name", "age", "city", "email", "join_date"}
        assert len(df) == 12

    def test_company_has_expected_properties(self, social_star: Star) -> None:
        """Company nodes expose name, industry, city, founded_year."""
        df = _q(
            social_star,
            """
            MATCH (c:Company)
            RETURN c.name AS name, c.industry AS industry,
                   c.city AS city, c.founded_year AS founded_year
            """,
        )
        assert set(df.columns) >= {"name", "industry", "city", "founded_year"}
        assert len(df) == 5


# ---------------------------------------------------------------------------
# Section 2 — Basic Queries
# ---------------------------------------------------------------------------


class TestBasicQueries:
    """Basic MATCH / RETURN queries (mirrors demo section 2)."""

    def test_return_all_person_names_and_cities(
        self, social_star: Star
    ) -> None:
        """Demo query: MATCH (p:Person) RETURN p.name AS name, p.city AS city."""
        df = _q(
            social_star,
            "MATCH (p:Person) RETURN p.name AS name, p.city AS city",
        )
        assert len(df) == 12
        assert {"name", "city"} <= set(df.columns)
        assert "Alice Chen" in df["name"].values

    def test_return_multiple_properties(self, social_star: Star) -> None:
        """Return name and age."""
        df = _q(
            social_star, "MATCH (p:Person) RETURN p.name AS name, p.age AS age"
        )
        assert len(df) == 12
        assert {"name", "age"} <= set(df.columns)


# ---------------------------------------------------------------------------
# Section 3 — Filtering (WHERE)
# ---------------------------------------------------------------------------


class TestFiltering:
    """WHERE clause with property predicates (mirrors demo section 3)."""

    def test_filter_age_gt_35(self, social_star: Star) -> None:
        """Demo query: WHERE p.age > 35."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)
            WHERE p.age > 35
            RETURN p.name AS name, p.age AS age, p.city AS city
            """,
        )
        assert len(df) >= 1
        assert all(int(age) > 35 for age in df["age"])

    def test_filter_by_city(self, social_star: Star) -> None:
        """Demo query: WHERE p.city = 'San Francisco'."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)
            WHERE p.city = 'San Francisco'
            RETURN p.name AS name, p.age AS age
            """,
        )
        # CSV has 4 SF residents: Alice, Bob, Grace, Karen
        assert len(df) == 4
        names = set(df["name"])
        assert {"Alice Chen", "Bob Martinez"} <= names

    def test_filter_combined_predicates(self, social_star: Star) -> None:
        """Combined predicates: age between 28 and 35."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)
            WHERE p.age >= 28 AND p.age <= 35
            RETURN p.name AS name, p.age AS age
            """,
        )
        assert len(df) >= 1
        assert all(28 <= int(a) <= 35 for a in df["age"])


# ---------------------------------------------------------------------------
# Section 4 — Relationships
# ---------------------------------------------------------------------------


class TestRelationships:
    """Relationship traversal queries (mirrors demo section 4)."""

    def test_knows_direct_friends(self, social_star: Star) -> None:
        """Demo query: MATCH (p)-[:KNOWS]->(friend) — 18 directed edges."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)-[:KNOWS]->(friend:Person)
            RETURN p.name AS person, friend.name AS friend
            """,
        )
        assert len(df) == 18

    def test_two_hop_friends_of_alice(self, social_star: Star) -> None:
        """Demo query: two-hop path from Alice Chen."""
        df = _q(
            social_star,
            """
            MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
            WHERE a.name = 'Alice Chen'
            RETURN a.name AS person, b.name AS via, c.name AS friend_of_friend
            """,
        )
        assert len(df) >= 1
        assert all(v == "Alice Chen" for v in df["person"])

    def test_works_at_relationship(self, social_star: Star) -> None:
        """MATCH (p)-[:WORKS_AT]->(c) returns 12 employment edges."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)-[:WORKS_AT]->(c:Company)
            RETURN p.name AS person, c.name AS company
            """,
        )
        assert len(df) == 12

    def test_coworkers_at_same_company(self, social_star: Star) -> None:
        """Find pairs who work at the same company."""
        df = _q(
            social_star,
            """
            MATCH (a:Person)-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(b:Person)
            WHERE a.name <> b.name
            RETURN a.name AS person_a, b.name AS person_b, c.name AS company
            """,
        )
        assert len(df) >= 1


# ---------------------------------------------------------------------------
# Section 5 — Aggregation
# ---------------------------------------------------------------------------


class TestAggregation:
    """Aggregation functions (mirrors demo section 5)."""

    def test_count_and_avg_per_city(self, social_star: Star) -> None:
        """Demo query: count + avg grouped by city."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)
            RETURN p.city AS city, count(p) AS population, avg(p.age) AS avg_age
            """,
        )
        # 4 distinct cities: San Francisco, New York, Seattle, Chicago
        assert len(df) == 4
        assert {"city", "population", "avg_age"} <= set(df.columns)

    def test_friend_count_per_person(self, social_star: Star) -> None:
        """Demo query: count friends per person."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)-[:KNOWS]->(friend:Person)
            RETURN p.name AS person, count(friend) AS friend_count
            """,
        )
        assert len(df) >= 1
        assert "friend_count" in df.columns

    def test_collect_names(self, social_star: Star) -> None:
        """Collect names into a list per city."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)
            RETURN p.city AS city, collect(p.name) AS names
            """,
        )
        assert len(df) >= 1
        assert "names" in df.columns


# ---------------------------------------------------------------------------
# Section 6 — WITH clause
# ---------------------------------------------------------------------------


class TestWithClause:
    """Intermediate processing with WITH (mirrors demo section 6)."""

    def test_with_popular_people(self, social_star: Star) -> None:
        """Demo query: WITH + WHERE to find people with >= 3 friends."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)-[:KNOWS]->(friend:Person)
            WITH p.name AS person, count(friend) AS friends
            WHERE friends >= 3
            RETURN person, friends
            """,
        )
        assert len(df) >= 1
        assert all(int(f) >= 3 for f in df["friends"])

    def test_with_filter_pipeline(self, social_star: Star) -> None:
        """WITH passes intermediate results to a second stage."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)
            WITH p.name AS name, p.age AS age
            WHERE age > 30
            RETURN name, age
            """,
        )
        assert len(df) >= 1
        assert all(int(a) > 30 for a in df["age"])


# ---------------------------------------------------------------------------
# Section 7 — SET operations
# ---------------------------------------------------------------------------


class TestSetOperations:
    """Property modification with SET (mirrors demo section 7)."""

    def test_set_region_property(self, social_star: Star) -> None:
        """Demo query: SET p.region = 'West Coast' for SF residents."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)
            WHERE p.city = 'San Francisco'
            SET p.region = 'West Coast'
            RETURN p.name AS name, p.city AS city, p.region AS region
            """,
        )
        assert len(df) == 4
        assert "region" in df.columns
        assert all(r == "West Coast" for r in df["region"])


# ---------------------------------------------------------------------------
# Section 8 — Scalar functions
# ---------------------------------------------------------------------------


class TestScalarFunctions:
    """Scalar functions (mirrors demo section 8)."""

    def test_toupper_and_size(self, social_star: Star) -> None:
        """Demo query: toUpper(city) and size(name)."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)
            RETURN
                p.name AS name,
                toUpper(p.city) AS city_upper,
                size(p.name) AS name_length
            """,
        )
        assert len(df) == 12
        assert all(
            isinstance(n, str) and n == n.upper() for n in df["city_upper"]
        )
        assert all(int(nl) > 0 for nl in df["name_length"])

    def test_abs_function(self, social_star: Star) -> None:
        """abs() returns absolute value."""
        df = _q(
            social_star,
            "MATCH (p:Person) RETURN abs(p.age - 35) AS age_diff",
        )
        assert len(df) == 12
        assert all(float(v) >= 0 for v in df["age_diff"])


# ---------------------------------------------------------------------------
# Section 9 — OPTIONAL MATCH
# ---------------------------------------------------------------------------


class TestOptionalMatch:
    """OPTIONAL MATCH — left-join semantics (mirrors demo section 9)."""

    def test_optional_match_works_at(self, social_star: Star) -> None:
        """Demo query: OPTIONAL MATCH (p)-[:WORKS_AT]->(c)."""
        df = _q(
            social_star,
            """
            MATCH (p:Person)
            OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
            RETURN p.name AS person, c.name AS company
            """,
        )
        # All 12 people appear — those without employment have null company
        assert len(df) >= 12
        assert set(df.columns) >= {"person", "company"}


# ---------------------------------------------------------------------------
# Section 10 — UNWIND
# ---------------------------------------------------------------------------


class TestUnwind:
    """UNWIND — list expansion (mirrors demo section 10)."""

    def test_unwind_with_computation(self, social_star: Star) -> None:
        """Demo query: UNWIND [1,2,3,4,5] with squared computation."""
        df = _q(
            social_star,
            """
            UNWIND [1, 2, 3, 4, 5] AS num
            RETURN num, num * num AS squared
            """,
        )
        assert len(df) == 5
        assert set(df["num"]) == {1, 2, 3, 4, 5}
        assert set(df["squared"]) == {1, 4, 9, 16, 25}

    def test_unwind_literal_list(self, social_star: Star) -> None:
        """Basic UNWIND with simple literal list."""
        df = _q(
            social_star,
            "UNWIND [1, 2, 3] AS x RETURN x",
        )
        assert len(df) == 3
        assert set(df["x"]) == {1, 2, 3}


# ---------------------------------------------------------------------------
# Section 11 — Query Validation
# ---------------------------------------------------------------------------


class TestQueryValidation:
    """validate_query() for syntax checking (mirrors demo section 11)."""

    def test_valid_query_no_errors(self) -> None:
        """A correct query produces no validation errors."""
        from pycypher import validate_query

        errors = validate_query("MATCH (p:Person) RETURN p.name")
        assert errors == []

    def test_undefined_variable_detected(self) -> None:
        """Demo query: MATCH (p) RETURN m.name — undefined variable m."""
        from pycypher import validate_query

        errors = validate_query("MATCH (p:Person) RETURN m.name")
        assert len(errors) >= 1


# ---------------------------------------------------------------------------
# Section 12 — Error Handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Custom exceptions with helpful messages (mirrors demo section 12)."""

    def test_unknown_variable_raises(self, social_star: Star) -> None:
        """Demo: MATCH (p) RETURN x.name — unknown variable x."""
        with pytest.raises(Exception):  # noqa: B017, PT011
            _q(social_star, "MATCH (p:Person) RETURN x.name")

    def test_unknown_function_raises(self, social_star: Star) -> None:
        """Demo: toUppper(p.name) — typo in function name."""
        with pytest.raises(Exception):  # noqa: B017, PT011
            _q(social_star, "MATCH (p:Person) RETURN toUppper(p.name)")

    def test_syntax_error_raises(self) -> None:
        """Malformed Cypher produces a parse error."""
        from pycypher.grammar_parser import GrammarParser

        parser = GrammarParser()
        with pytest.raises(Exception):  # noqa: B017, PT011
            parser.parse("MATCCH (n) RTRN n")


# ---------------------------------------------------------------------------
# Meta-test — Demo script runs without crashing
# ---------------------------------------------------------------------------


class TestDemoScriptRunnable:
    """Verify the demo script itself is importable and runnable."""

    def test_demo_functions_importable(self) -> None:
        """All demo functions can be imported."""
        import importlib
        import sys

        spec = importlib.util.spec_from_file_location(
            "run_demo",
            str(_DATA_DIR.parent / "run_demo.py"),
        )
        assert spec is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules["run_demo"] = mod
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

        # Verify expected functions exist
        assert hasattr(mod, "load_graph")
        assert hasattr(mod, "demo_basic_queries")
        assert hasattr(mod, "demo_filtering")
        assert hasattr(mod, "demo_relationships")
        assert hasattr(mod, "demo_aggregation")
        assert hasattr(mod, "demo_with_clause")
        assert hasattr(mod, "demo_set_operations")
        assert hasattr(mod, "demo_scalar_functions")
        assert hasattr(mod, "demo_optional_match")
        assert hasattr(mod, "demo_unwind")
        assert hasattr(mod, "demo_validation")
        assert hasattr(mod, "demo_error_handling")
        assert hasattr(mod, "main")

        del sys.modules["run_demo"]
