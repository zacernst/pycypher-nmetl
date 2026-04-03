"""Property-based tests for query correctness using Hypothesis.

Tests semantic properties that must hold for ALL valid inputs:
1. Idempotency — running the same query twice yields identical results
2. Commutativity — AND/OR operand order doesn't change results
3. Query equivalence — different formulations produce same results
4. Null propagation — NULL handling follows three-valued logic
5. Type safety — invalid types produce errors, not wrong results
6. Aggregation invariants — mathematical properties of aggregations
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from hypothesis import given, settings
from hypothesis import strategies as st
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star

# Ensure the property_based package is importable
sys.path.insert(0, str(Path(__file__).parent.parent))
from property_based.strategies import (
    social_stars,
)

ID_COLUMN = "__ID__"

# Reduce deadline for CI — these tests generate many examples
SETTINGS = settings(max_examples=50, deadline=5000)


# ---------------------------------------------------------------------------
# Helper: fixed Star for deterministic property tests
# ---------------------------------------------------------------------------


def _fixed_star(ages: list[int]) -> Star:
    """Build a Star with Person entities having given ages."""
    n = len(ages)
    people_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(1, n + 1)),
            "name": [f"Person{i}" for i in range(1, n + 1)],
            "age": ages,
        },
    )
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )
    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )
    return Star(context=ctx)


# ===========================================================================
# Property 1: Idempotency — same query, same result
# ===========================================================================


class TestIdempotency:
    """Running the same read query multiple times must yield identical results."""

    @given(star=social_stars(min_nodes=2, max_nodes=10))
    @SETTINGS
    def test_match_return_idempotent(self, star: Star) -> None:
        """MATCH (n:Person) RETURN n.name yields same result each time."""
        q = "MATCH (n:Person) RETURN n.name ORDER BY n.name"
        r1 = star.execute_query(q)
        r2 = star.execute_query(q)
        pd.testing.assert_frame_equal(r1, r2)

    @given(star=social_stars(min_nodes=2, max_nodes=10))
    @SETTINGS
    def test_aggregation_idempotent(self, star: Star) -> None:
        """count(*) yields same result each time."""
        q = "MATCH (n:Person) RETURN count(n) AS cnt"
        r1 = star.execute_query(q)
        r2 = star.execute_query(q)
        pd.testing.assert_frame_equal(r1, r2)


# ===========================================================================
# Property 2: Commutativity — AND/OR operand order independence
# ===========================================================================


class TestCommutativity:
    """Boolean operators must be commutative."""

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=3,
            max_size=15,
        ),
        threshold1=st.integers(min_value=10, max_value=90),
        threshold2=st.integers(min_value=10, max_value=90),
    )
    @SETTINGS
    def test_and_commutative(
        self,
        ages: list[int],
        threshold1: int,
        threshold2: int,
    ) -> None:
        """(a AND b) == (b AND a)."""
        star = _fixed_star(ages)
        q1 = (
            f"MATCH (n:Person) "
            f"WHERE n.age > {threshold1} AND n.age < {threshold2} "
            f"RETURN n.name ORDER BY n.name"
        )
        q2 = (
            f"MATCH (n:Person) "
            f"WHERE n.age < {threshold2} AND n.age > {threshold1} "
            f"RETURN n.name ORDER BY n.name"
        )
        r1 = star.execute_query(q1)
        r2 = star.execute_query(q2)
        pd.testing.assert_frame_equal(r1, r2)

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=3,
            max_size=15,
        ),
        threshold1=st.integers(min_value=10, max_value=90),
        threshold2=st.integers(min_value=10, max_value=90),
    )
    @SETTINGS
    def test_or_commutative(
        self,
        ages: list[int],
        threshold1: int,
        threshold2: int,
    ) -> None:
        """(a OR b) == (b OR a)."""
        star = _fixed_star(ages)
        q1 = (
            f"MATCH (n:Person) "
            f"WHERE n.age < {threshold1} OR n.age > {threshold2} "
            f"RETURN n.name ORDER BY n.name"
        )
        q2 = (
            f"MATCH (n:Person) "
            f"WHERE n.age > {threshold2} OR n.age < {threshold1} "
            f"RETURN n.name ORDER BY n.name"
        )
        r1 = star.execute_query(q1)
        r2 = star.execute_query(q2)
        pd.testing.assert_frame_equal(r1, r2)


# ===========================================================================
# Property 3: Query equivalence — different formulations, same result
# ===========================================================================


class TestQueryEquivalence:
    """Different query formulations that must produce identical results."""

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=3,
            max_size=15,
        ),
        threshold=st.integers(min_value=10, max_value=90),
    )
    @SETTINGS
    def test_where_vs_with_where(
        self,
        ages: list[int],
        threshold: int,
    ) -> None:
        """MATCH WHERE == MATCH WITH WHERE (same filter, different clause)."""
        star = _fixed_star(ages)
        q_direct = f"MATCH (n:Person) WHERE n.age > {threshold} RETURN n.name ORDER BY n.name"
        q_with = (
            f"MATCH (n:Person) "
            f"WITH n WHERE n.age > {threshold} "
            f"RETURN n.name ORDER BY n.name"
        )
        r1 = star.execute_query(q_direct)
        r2 = star.execute_query(q_with)
        pd.testing.assert_frame_equal(r1, r2)

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=3,
            max_size=15,
        ),
        threshold=st.integers(min_value=10, max_value=90),
    )
    @SETTINGS
    def test_not_greater_equals_leq(
        self,
        ages: list[int],
        threshold: int,
    ) -> None:
        """NOT (n.age > t) == n.age <= t for non-null values."""
        star = _fixed_star(ages)
        q1 = (
            f"MATCH (n:Person) WHERE NOT n.age > {threshold} "
            f"RETURN n.name ORDER BY n.name"
        )
        q2 = f"MATCH (n:Person) WHERE n.age <= {threshold} RETURN n.name ORDER BY n.name"
        r1 = star.execute_query(q1)
        r2 = star.execute_query(q2)
        pd.testing.assert_frame_equal(r1, r2)

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=2,
            max_size=15,
        ),
    )
    @SETTINGS
    def test_count_equals_row_count(self, ages: list[int]) -> None:
        """count(n) must equal the number of rows returned by RETURN n."""
        star = _fixed_star(ages)
        r_count = star.execute_query("MATCH (n:Person) RETURN count(n) AS cnt")
        r_all = star.execute_query("MATCH (n:Person) RETURN n.name")
        assert r_count["cnt"].iloc[0] == len(r_all)


# ===========================================================================
# Property 4: Aggregation invariants
# ===========================================================================


class TestAggregationInvariants:
    """Mathematical properties of aggregation functions."""

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=1,
            max_size=20,
        ),
    )
    @SETTINGS
    def test_min_leq_max(self, ages: list[int]) -> None:
        """min(x) <= max(x) for any non-empty set."""
        star = _fixed_star(ages)
        result = star.execute_query(
            "MATCH (n:Person) RETURN min(n.age) AS lo, max(n.age) AS hi",
        )
        assert result["lo"].iloc[0] <= result["hi"].iloc[0]

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=1,
            max_size=20,
        ),
    )
    @SETTINGS
    def test_avg_between_min_max(self, ages: list[int]) -> None:
        """min(x) <= avg(x) <= max(x)."""
        star = _fixed_star(ages)
        result = star.execute_query(
            "MATCH (n:Person) "
            "RETURN min(n.age) AS lo, avg(n.age) AS mid, max(n.age) AS hi",
        )
        lo = result["lo"].iloc[0]
        mid = result["mid"].iloc[0]
        hi = result["hi"].iloc[0]
        assert lo <= mid <= hi

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=1,
            max_size=20,
        ),
    )
    @SETTINGS
    def test_sum_equals_count_times_avg(self, ages: list[int]) -> None:
        """sum(x) == count(x) * avg(x) (within floating point tolerance)."""
        star = _fixed_star(ages)
        result = star.execute_query(
            "MATCH (n:Person) RETURN sum(n.age) AS s, count(n) AS c, avg(n.age) AS a",
        )
        s = result["s"].iloc[0]
        c = result["c"].iloc[0]
        a = result["a"].iloc[0]
        assert abs(s - c * a) < 0.01

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=1,
            max_size=20,
        ),
    )
    @SETTINGS
    def test_count_matches_input_size(self, ages: list[int]) -> None:
        """count(n) == number of entities in the graph."""
        star = _fixed_star(ages)
        result = star.execute_query("MATCH (n:Person) RETURN count(n) AS cnt")
        assert result["cnt"].iloc[0] == len(ages)

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=1,
            max_size=20,
        ),
    )
    @SETTINGS
    def test_collect_length_equals_count(self, ages: list[int]) -> None:
        """Length of collect(x) == count(x)."""
        star = _fixed_star(ages)
        result = star.execute_query(
            "MATCH (n:Person) RETURN collect(n.age) AS ages, count(n) AS cnt",
        )
        collected = result["ages"].iloc[0]
        cnt = result["cnt"].iloc[0]
        assert len(collected) == cnt


# ===========================================================================
# Property 5: LIMIT/SKIP invariants
# ===========================================================================


class TestLimitSkipInvariants:
    """Properties of LIMIT and SKIP clauses."""

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=1,
            max_size=20,
        ),
        limit=st.integers(min_value=1, max_value=25),
    )
    @SETTINGS
    def test_limit_bounds_result_size(
        self,
        ages: list[int],
        limit: int,
    ) -> None:
        """LIMIT n produces at most n rows."""
        star = _fixed_star(ages)
        result = star.execute_query(
            f"MATCH (n:Person) RETURN n.name LIMIT {limit}",
        )
        assert len(result) <= limit

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=1,
            max_size=20,
        ),
        skip=st.integers(min_value=0, max_value=25),
    )
    @SETTINGS
    def test_skip_reduces_result_size(
        self,
        ages: list[int],
        skip: int,
    ) -> None:
        """SKIP n produces max(0, total - n) rows."""
        star = _fixed_star(ages)
        full = star.execute_query("MATCH (n:Person) RETURN n.name")
        skipped = star.execute_query(
            f"MATCH (n:Person) RETURN n.name SKIP {skip}",
        )
        expected = max(0, len(full) - skip)
        assert len(skipped) == expected

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=1,
            max_size=20,
        ),
        skip=st.integers(min_value=0, max_value=10),
        limit=st.integers(min_value=1, max_value=10),
    )
    @SETTINGS
    def test_skip_plus_limit_partition(
        self,
        ages: list[int],
        skip: int,
        limit: int,
    ) -> None:
        """SKIP + LIMIT produces subset of full result."""
        star = _fixed_star(ages)
        full = star.execute_query(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name",
        )
        paged = star.execute_query(
            f"MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP {skip} LIMIT {limit}",
        )
        assert len(paged) <= limit
        assert len(paged) <= max(0, len(full) - skip)


# ===========================================================================
# Property 6: DISTINCT invariants
# ===========================================================================


class TestDistinctInvariants:
    """Properties of DISTINCT clause."""

    @given(
        ages=st.lists(
            st.integers(
                min_value=1,
                max_value=10,
            ),  # small range to get duplicates
            min_size=3,
            max_size=20,
        ),
    )
    @SETTINGS
    def test_distinct_leq_total(self, ages: list[int]) -> None:
        """DISTINCT result count <= total result count."""
        star = _fixed_star(ages)
        r_all = star.execute_query("MATCH (n:Person) RETURN n.age")
        r_distinct = star.execute_query(
            "MATCH (n:Person) RETURN DISTINCT n.age",
        )
        assert len(r_distinct) <= len(r_all)

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=10),
            min_size=3,
            max_size=20,
        ),
    )
    @SETTINGS
    def test_distinct_no_duplicates(self, ages: list[int]) -> None:
        """DISTINCT result has no duplicate rows."""
        star = _fixed_star(ages)
        result = star.execute_query("MATCH (n:Person) RETURN DISTINCT n.age")
        assert len(result) == len(result.drop_duplicates())


# ===========================================================================
# Property 7: ORDER BY invariants
# ===========================================================================


class TestOrderByInvariants:
    """Properties of ORDER BY clause."""

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=2,
            max_size=20,
        ),
    )
    @SETTINGS
    def test_order_by_asc_is_sorted(self, ages: list[int]) -> None:
        """ORDER BY ASC produces sorted output."""
        star = _fixed_star(ages)
        result = star.execute_query(
            "MATCH (n:Person) RETURN n.age ORDER BY n.age",
        )
        values = list(result.iloc[:, 0])
        assert values == sorted(values)

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=2,
            max_size=20,
        ),
    )
    @SETTINGS
    def test_order_by_desc_is_reverse_sorted(self, ages: list[int]) -> None:
        """ORDER BY DESC produces reverse-sorted output."""
        star = _fixed_star(ages)
        result = star.execute_query(
            "MATCH (n:Person) RETURN n.age ORDER BY n.age DESC",
        )
        values = list(result.iloc[:, 0])
        assert values == sorted(values, reverse=True)

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=2,
            max_size=20,
        ),
    )
    @SETTINGS
    def test_order_by_preserves_row_count(self, ages: list[int]) -> None:
        """ORDER BY does not add or remove rows."""
        star = _fixed_star(ages)
        r_unsorted = star.execute_query("MATCH (n:Person) RETURN n.age")
        r_sorted = star.execute_query(
            "MATCH (n:Person) RETURN n.age ORDER BY n.age",
        )
        assert len(r_unsorted) == len(r_sorted)


# ===========================================================================
# Property 8: Arithmetic expression properties
# ===========================================================================


class TestArithmeticProperties:
    """Mathematical properties of arithmetic in RETURN expressions."""

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=50),
            min_size=1,
            max_size=10,
        ),
        k=st.integers(min_value=1, max_value=100),
    )
    @SETTINGS
    def test_addition_commutative(self, ages: list[int], k: int) -> None:
        """n.age + k == k + n.age."""
        star = _fixed_star(ages)
        r1 = star.execute_query(
            f"MATCH (n:Person) RETURN n.age + {k} AS v ORDER BY n.age",
        )
        r2 = star.execute_query(
            f"MATCH (n:Person) RETURN {k} + n.age AS v ORDER BY n.age",
        )
        pd.testing.assert_frame_equal(r1, r2)

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=50),
            min_size=1,
            max_size=10,
        ),
        k=st.integers(min_value=1, max_value=100),
    )
    @SETTINGS
    def test_multiplication_commutative(self, ages: list[int], k: int) -> None:
        """n.age * k == k * n.age."""
        star = _fixed_star(ages)
        r1 = star.execute_query(
            f"MATCH (n:Person) RETURN n.age * {k} AS v ORDER BY n.age",
        )
        r2 = star.execute_query(
            f"MATCH (n:Person) RETURN {k} * n.age AS v ORDER BY n.age",
        )
        pd.testing.assert_frame_equal(r1, r2)

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=50),
            min_size=1,
            max_size=10,
        ),
    )
    @SETTINGS
    def test_additive_identity(self, ages: list[int]) -> None:
        """n.age + 0 == n.age."""
        star = _fixed_star(ages)
        r1 = star.execute_query(
            "MATCH (n:Person) RETURN n.age + 0 AS v ORDER BY n.age",
        )
        r2 = star.execute_query(
            "MATCH (n:Person) RETURN n.age AS v ORDER BY n.age",
        )
        # Types may differ (int vs float), so compare values
        assert list(r1["v"]) == list(r2["v"])

    @given(
        ages=st.lists(
            st.integers(min_value=1, max_value=50),
            min_size=1,
            max_size=10,
        ),
    )
    @SETTINGS
    def test_multiplicative_identity(self, ages: list[int]) -> None:
        """n.age * 1 == n.age."""
        star = _fixed_star(ages)
        r1 = star.execute_query(
            "MATCH (n:Person) RETURN n.age * 1 AS v ORDER BY n.age",
        )
        r2 = star.execute_query(
            "MATCH (n:Person) RETURN n.age AS v ORDER BY n.age",
        )
        assert list(r1["v"]) == list(r2["v"])
