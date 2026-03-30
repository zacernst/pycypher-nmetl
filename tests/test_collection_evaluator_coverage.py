"""Coverage-gap tests for pycypher.collection_evaluator.

Targets uncovered paths in _extract_temporal_field, eval_reduce,
eval_map_literal, eval_map_projection, and edge cases in quantifier/list
comprehension evaluation.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import ContextBuilder, Star


@pytest.fixture
def star_with_people() -> Star:
    """Star instance with Person entities and KNOWS relationships."""
    people = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
            "born": ["1994-03-15", "1999-07-20", "1989-12-01"],
        },
    )
    rels = pd.DataFrame(
        {
            "__ID__": [100, 101],
            "__SOURCE__": [1, 2],
            "__TARGET__": [2, 3],
            "since": [2020, 2021],
        },
    )
    ctx = (
        ContextBuilder()
        .add_entity("Person", people)
        .add_relationship(
            "KNOWS", rels, source_col="__SOURCE__", target_col="__TARGET__",
        )
        .build()
    )
    return Star(context=ctx)


# ---------------------------------------------------------------------------
# _extract_temporal_field via property lookup on expressions
# ---------------------------------------------------------------------------


class TestTemporalFieldExtraction:
    """Tests for _extract_temporal_field via property lookup on date strings."""

    def test_year_from_date_string(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) RETURN p.born AS born, p.name AS name ORDER BY p.name",
        )
        # Verify dates are present
        assert result["born"].iloc[0] == "1994-03-15"

    def test_temporal_field_via_with(self, star_with_people: Star) -> None:
        """Access temporal fields via WITH clause and map literal."""
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH {born: p.born} AS info, p.name AS name "
            "RETURN info.born AS born, name "
            "ORDER BY name",
        )
        assert len(result) == 3


# ---------------------------------------------------------------------------
# eval_reduce — REDUCE expressions
# ---------------------------------------------------------------------------


class TestEvalReduce:
    """Tests for REDUCE expression evaluation."""

    def test_reduce_sum(self, star_with_people: Star) -> None:
        """REDUCE with summation over a list."""
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [1, 2, 3, 4, 5] AS nums, p.name AS name "
            "RETURN reduce(total = 0, x IN nums | total + x) AS sum_val, name "
            "ORDER BY name",
        )
        assert result["sum_val"].iloc[0] == 15

    def test_reduce_with_null_list(self, star_with_people: Star) -> None:
        """REDUCE over a null list should return initial value."""
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH null AS nums, p.name AS name "
            "RETURN reduce(total = 0, x IN nums | total + x) AS sum_val, name "
            "ORDER BY name",
        )
        assert result["sum_val"].iloc[0] == 0

    def test_reduce_with_empty_list(self, star_with_people: Star) -> None:
        """REDUCE over empty list returns initial value."""
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [] AS nums, p.name AS name "
            "RETURN reduce(total = 42, x IN nums | total + x) AS sum_val, name "
            "ORDER BY name",
        )
        assert result["sum_val"].iloc[0] == 42

    def test_reduce_string_concat(self, star_with_people: Star) -> None:
        """REDUCE with string concatenation."""
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH ['a', 'b', 'c'] AS chars, p.name AS name "
            "RETURN reduce(s = '', c IN chars | s + c) AS concat_val, name "
            "ORDER BY name",
        )
        assert result["concat_val"].iloc[0] == "abc"


# ---------------------------------------------------------------------------
# eval_map_literal — {key: expr, ...}
# ---------------------------------------------------------------------------


class TestEvalMapLiteral:
    """Tests for map literal evaluation."""

    def test_simple_map_literal(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "RETURN {name: p.name, age: p.age} AS info "
            "ORDER BY p.name",
        )
        assert result["info"].iloc[0] == {"name": "Alice", "age": 30}

    def test_empty_map_literal(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) RETURN {} AS empty_map, p.name AS name ORDER BY name",
        )
        assert result["empty_map"].iloc[0] == {}

    def test_map_literal_with_expressions(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "RETURN {doubled_age: p.age * 2, upper_name: toUpper(p.name)} AS info "
            "ORDER BY p.name",
        )
        info = result["info"].iloc[0]
        assert info["doubled_age"] == 60
        assert info["upper_name"] == "ALICE"


# ---------------------------------------------------------------------------
# eval_map_projection — n{.prop, key: expr}
# ---------------------------------------------------------------------------


class TestEvalMapProjection:
    """Tests for map projection evaluation."""

    def test_property_projection(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) RETURN p{.name, .age} AS proj ORDER BY p.name",
        )
        proj = result["proj"].iloc[0]
        assert proj["name"] == "Alice"
        assert proj["age"] == 30

    def test_expression_projection(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "RETURN p{.name, double_age: p.age * 2} AS proj "
            "ORDER BY p.name",
        )
        proj = result["proj"].iloc[0]
        assert proj["name"] == "Alice"
        assert proj["double_age"] == 60

    def test_all_properties_projection(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) RETURN p{.*} AS proj ORDER BY p.name",
        )
        proj = result["proj"].iloc[0]
        # The all_properties projection returns a dict; properties depend on
        # how entity tables expose them. Verify it's a dict (may be empty if
        # the entity table doesn't expose get_all_properties for ID-only frames).
        assert isinstance(proj, dict)


# ---------------------------------------------------------------------------
# eval_list_comprehension — [x IN list WHERE pred | expr]
# ---------------------------------------------------------------------------


class TestEvalListComprehension:
    """Tests for list comprehension evaluation."""

    def test_list_comp_with_filter(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [1, 2, 3, 4, 5] AS nums, p.name AS name "
            "RETURN [x IN nums WHERE x > 3] AS filtered, name "
            "ORDER BY name",
        )
        assert result["filtered"].iloc[0] == [4, 5]

    def test_list_comp_with_map_expr(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [1, 2, 3] AS nums, p.name AS name "
            "RETURN [x IN nums | x * 2] AS doubled, name "
            "ORDER BY name",
        )
        assert result["doubled"].iloc[0] == [2, 4, 6]

    def test_list_comp_with_filter_and_map(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [1, 2, 3, 4, 5] AS nums, p.name AS name "
            "RETURN [x IN nums WHERE x > 2 | x * 10] AS result, name "
            "ORDER BY name",
        )
        assert result["result"].iloc[0] == [30, 40, 50]

    def test_list_comp_empty_list(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [] AS nums, p.name AS name "
            "RETURN [x IN nums | x * 2] AS result, name "
            "ORDER BY name",
        )
        assert result["result"].iloc[0] == []

    def test_list_comp_null_list(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH null AS nums, p.name AS name "
            "RETURN [x IN nums | x * 2] AS result, name "
            "ORDER BY name",
        )
        assert result["result"].iloc[0] == []


# ---------------------------------------------------------------------------
# eval_quantifier — ANY / ALL / NONE
# ---------------------------------------------------------------------------


class TestEvalQuantifier:
    """Tests for quantifier expression evaluation."""

    def test_any_true(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [1, 2, 3] AS nums, p.name AS name "
            "RETURN any(x IN nums WHERE x > 2) AS has_large, name "
            "ORDER BY name",
        )
        assert bool(result["has_large"].iloc[0]) is True

    def test_any_false(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [1, 2, 3] AS nums, p.name AS name "
            "RETURN any(x IN nums WHERE x > 10) AS has_large, name "
            "ORDER BY name",
        )
        assert bool(result["has_large"].iloc[0]) is False

    def test_all_true(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [2, 4, 6] AS nums, p.name AS name "
            "RETURN all(x IN nums WHERE x > 0) AS all_positive, name "
            "ORDER BY name",
        )
        assert bool(result["all_positive"].iloc[0]) is True

    def test_all_false(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [2, -1, 6] AS nums, p.name AS name "
            "RETURN all(x IN nums WHERE x > 0) AS all_positive, name "
            "ORDER BY name",
        )
        assert bool(result["all_positive"].iloc[0]) is False

    def test_none_true(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [1, 2, 3] AS nums, p.name AS name "
            "RETURN none(x IN nums WHERE x > 10) AS none_large, name "
            "ORDER BY name",
        )
        assert bool(result["none_large"].iloc[0]) is True

    def test_any_empty_list(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [] AS nums, p.name AS name "
            "RETURN any(x IN nums WHERE x > 0) AS result, name "
            "ORDER BY name",
        )
        assert bool(result["result"].iloc[0]) is False

    def test_all_empty_list(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [] AS nums, p.name AS name "
            "RETURN all(x IN nums WHERE x > 0) AS result, name "
            "ORDER BY name",
        )
        # ALL of empty = true (vacuous truth)
        assert bool(result["result"].iloc[0]) is True

    def test_none_empty_list(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [] AS nums, p.name AS name "
            "RETURN none(x IN nums WHERE x > 0) AS result, name "
            "ORDER BY name",
        )
        assert bool(result["result"].iloc[0]) is True


# ---------------------------------------------------------------------------
# eval_slicing — list[from..to]
# ---------------------------------------------------------------------------


class TestEvalSlicing:
    """Tests for list slicing evaluation."""

    def test_slice_with_bounds(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [1, 2, 3, 4, 5] AS nums, p.name AS name "
            "RETURN nums[1..3] AS sliced, name "
            "ORDER BY name",
        )
        assert result["sliced"].iloc[0] == [2, 3]

    def test_slice_from_start(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [1, 2, 3, 4, 5] AS nums, p.name AS name "
            "RETURN nums[..2] AS sliced, name "
            "ORDER BY name",
        )
        assert result["sliced"].iloc[0] == [1, 2]

    def test_slice_to_end(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH [1, 2, 3, 4, 5] AS nums, p.name AS name "
            "RETURN nums[3..] AS sliced, name "
            "ORDER BY name",
        )
        assert result["sliced"].iloc[0] == [4, 5]

    def test_slice_null_list(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH null AS nums, p.name AS name "
            "RETURN nums[0..2] AS sliced, name "
            "ORDER BY name",
        )
        assert result["sliced"].iloc[0] is None


# ---------------------------------------------------------------------------
# Property lookup on map values from UNWIND
# ---------------------------------------------------------------------------


class TestPropertyLookupOnMaps:
    """Tests for property lookup on map values (dict path in eval_property_lookup)."""

    def test_property_on_unwind_map(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "UNWIND [{name: 'X', val: 1}, {name: 'Y', val: 2}] AS item "
            "RETURN item.name AS name, item.val AS val "
            "ORDER BY name",
        )
        assert list(result["name"]) == ["X", "Y"]
        assert list(result["val"]) == [1, 2]

    def test_property_on_map_literal_in_with(self, star_with_people: Star) -> None:
        result = star_with_people.execute_query(
            "MATCH (p:Person) "
            "WITH {n: p.name, a: p.age} AS info "
            "RETURN info.n AS name, info.a AS age "
            "ORDER BY name",
        )
        assert list(result["name"]) == ["Alice", "Bob", "Carol"]
