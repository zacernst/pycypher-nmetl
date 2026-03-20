"""Consolidated tests for openCypher three-valued (Kleene) logic with null.

Covers null propagation across all operator families:
- AND/OR (Kleene short-circuit rules)
- XOR (no short-circuit — null XOR anything = null)
- Comparison operators (=, <>, <, >, <=, >=)
- IN operator (three-valued membership)
- String predicates (STARTS WITH, ENDS WITH, CONTAINS, =~)
- Unary operators (-, +)

In RETURN clauses: null-involving operations return null.
In WHERE clauses: null results in row exclusion (via fillna(False)).

Consolidated from:
  test_and_or_null.py, test_xor_null.py, test_comparison_null.py,
  test_in_operator_null.py, test_string_predicate_null.py, test_unary_null.py
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def minimal_star() -> Star:
    """Minimal single-row star for literal-only queries."""
    df = pd.DataFrame({ID_COLUMN: [1], "n": [1]})
    table = EntityTable(
        entity_type="N",
        identifier="N",
        column_names=[ID_COLUMN, "n"],
        source_obj_attribute_map={"n": "n"},
        attribute_map={"n": "n"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"N": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


@pytest.fixture()
def nullable_age_star() -> Star:
    """Three-person star with nullable age column."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [25, None, 35],
            "active": [True, None, False],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "active"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "active": "active",
        },
        attribute_map={"name": "name", "age": "age", "active": "active"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


@pytest.fixture()
def nullable_name_star() -> Star:
    """Three-person star with nullable name column (for string predicates)."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", None],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


@pytest.fixture()
def in_operator_star() -> Star:
    """Four-person star with nullable score (for IN operator tests)."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "score": [1, 4, 7, None],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "score"],
        source_obj_attribute_map={"name": "name", "score": "score"},
        attribute_map={"name": "name", "score": "score"},
        source_obj=df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
    )


# ===========================================================================
# AND / OR  (Kleene three-valued logic)
# ===========================================================================


class TestAndThreeValuedLogic:
    """AND (Kleene) three-valued logic."""

    def test_null_and_true_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null AND true AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_and_false_is_false(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null AND false AS r")
        result = r["r"].iloc[0]
        assert result is False or result == False  # noqa: E712

    def test_null_and_null_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null AND null AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_true_and_null_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN true AND null AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_false_and_null_is_false(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN false AND null AS r")
        result = r["r"].iloc[0]
        assert result is False or result == False  # noqa: E712

    def test_true_and_true_is_true(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN true AND true AS r")
        result = r["r"].iloc[0]
        assert result is True or result == True  # noqa: E712

    def test_true_and_false_is_false(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN true AND false AS r")
        result = r["r"].iloc[0]
        assert result is False or result == False  # noqa: E712


class TestOrThreeValuedLogic:
    """OR (Kleene) three-valued logic."""

    def test_null_or_true_is_true(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null OR true AS r")
        result = r["r"].iloc[0]
        assert result is True or result == True  # noqa: E712

    def test_null_or_false_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null OR false AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_or_null_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null OR null AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_true_or_null_is_true(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN true OR null AS r")
        result = r["r"].iloc[0]
        assert result is True or result == True  # noqa: E712

    def test_false_or_null_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN false OR null AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_false_or_false_is_false(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN false OR false AS r")
        result = r["r"].iloc[0]
        assert result is False or result == False  # noqa: E712

    def test_true_or_false_is_true(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN true OR false AS r")
        result = r["r"].iloc[0]
        assert result is True or result == True  # noqa: E712


class TestAndOrInWhere:
    """In WHERE, null still excludes rows (via fillna(False))."""

    def test_null_and_non_null_condition_excludes_null_row(
        self, nullable_age_star: Star
    ) -> None:
        r = nullable_age_star.execute_query(
            "MATCH (p:Person) WHERE p.age > 20 AND p.name = 'Bob' RETURN p.name"
        )
        assert list(r["name"]) == []

    def test_or_with_null_includes_when_other_is_true(
        self, nullable_age_star: Star
    ) -> None:
        r = nullable_age_star.execute_query(
            "MATCH (p:Person) WHERE p.age > 20 OR p.name = 'Bob' RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Alice", "Bob", "Carol"]


# ===========================================================================
# XOR  (no short-circuit — null XOR anything = null)
# ===========================================================================


class TestXorThreeValuedLogic:
    def test_null_xor_true_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null XOR true AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_xor_false_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null XOR false AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_xor_null_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null XOR null AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_true_xor_null_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN true XOR null AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_false_xor_null_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN false XOR null AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_true_xor_true_is_false(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN true XOR true AS r")
        result = r["r"].iloc[0]
        assert result is False or result == False  # noqa: E712

    def test_true_xor_false_is_true(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN true XOR false AS r")
        result = r["r"].iloc[0]
        assert result is True or result == True  # noqa: E712

    def test_false_xor_false_is_false(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN false XOR false AS r")
        result = r["r"].iloc[0]
        assert result is False or result == False  # noqa: E712


# ===========================================================================
# Comparison operators  (=, <>, <, >, <=, >=)
# ===========================================================================


class TestNullComparisonInReturn:
    """Comparisons with null should return null in RETURN clauses."""

    def test_null_eq_value_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null = 1 AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_ne_value_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null <> 1 AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_eq_null_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null = null AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_value_eq_null_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN 1 = null AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_gt_value_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null > 1 AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_property_null_eq_returns_null(
        self, nullable_age_star: Star
    ) -> None:
        r = nullable_age_star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.age = 25 AS r"
        )
        assert pd.isna(r["r"].iloc[0])


class TestNullComparisonInWhere:
    """WHERE clause comparisons with null must exclude nulls."""

    def test_ne_excludes_null_rows(self, nullable_age_star: Star) -> None:
        r = nullable_age_star.execute_query(
            "MATCH (p:Person) WHERE p.age <> 25 RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Carol"]

    def test_not_eq_excludes_null_rows(self, nullable_age_star: Star) -> None:
        r = nullable_age_star.execute_query(
            "MATCH (p:Person) WHERE NOT (p.age = 25) RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Carol"]

    def test_gt_excludes_null_rows(self, nullable_age_star: Star) -> None:
        r = nullable_age_star.execute_query(
            "MATCH (p:Person) WHERE p.age > 25 RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Carol"]

    def test_eq_with_value_correct(self, nullable_age_star: Star) -> None:
        r = nullable_age_star.execute_query(
            "MATCH (p:Person) WHERE p.age = 25 RETURN p.name"
        )
        assert list(r["name"]) == ["Alice"]


# ===========================================================================
# IN operator  (three-valued membership)
# ===========================================================================


class TestInOperatorNullLiteral:
    """Literal null on the LHS of IN."""

    def test_null_in_nonempty_list_is_null(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null IN [1, 2, 3] AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_null_in_empty_list_is_false(self, minimal_star: Star) -> None:
        r = minimal_star.execute_query("RETURN null IN [] AS r")
        result = r["r"].iloc[0]
        assert result is False or result == False  # noqa: E712


class TestInOperatorNullInList:
    """Null element in the RHS list."""

    def test_definite_nonmember_in_list_with_null_is_null(
        self, minimal_star: Star
    ) -> None:
        r = minimal_star.execute_query("RETURN 4 IN [1, null, 3] AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_definite_member_in_list_with_null_is_true(
        self, minimal_star: Star
    ) -> None:
        r = minimal_star.execute_query("RETURN 1 IN [1, null, 3] AS r")
        result = r["r"].iloc[0]
        assert result is True or result == True  # noqa: E712

    def test_definite_nonmember_in_list_without_null_is_false(
        self, minimal_star: Star
    ) -> None:
        r = minimal_star.execute_query("RETURN 4 IN [1, 2, 3] AS r")
        result = r["r"].iloc[0]
        assert result is False or result == False  # noqa: E712


class TestInOperatorPropertyNull:
    """Property-level null on the LHS of IN."""

    def test_null_property_in_list_excluded_from_where(
        self, in_operator_star: Star
    ) -> None:
        r = in_operator_star.execute_query(
            "MATCH (p:Person) WHERE p.score IN [1, 4] RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Alice", "Bob"]

    def test_property_in_list_with_null_element(
        self, in_operator_star: Star
    ) -> None:
        r = in_operator_star.execute_query(
            "MATCH (p:Person) WHERE p.score IN [7, null] RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Carol"]


# ===========================================================================
# String predicates  (STARTS WITH, ENDS WITH, CONTAINS, =~)
# ===========================================================================


class TestStartsWithNull:
    def test_null_lhs_returns_null_in_return(
        self, nullable_name_star: Star
    ) -> None:
        r = nullable_name_star.execute_query(
            "MATCH (p:Person) WHERE p.name IS NULL RETURN p.name STARTS WITH 'A' AS r"
        )
        assert pd.isna(r["r"].iloc[0])

    def test_null_lhs_excluded_from_where(
        self, nullable_name_star: Star
    ) -> None:
        r = nullable_name_star.execute_query(
            "MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p.name"
        )
        assert list(r["name"]) == ["Alice"]

    def test_not_starts_with_excludes_null(
        self, nullable_name_star: Star
    ) -> None:
        r = nullable_name_star.execute_query(
            "MATCH (p:Person) WHERE NOT (p.name STARTS WITH 'A') RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Bob"]


class TestEndsWithNull:
    def test_null_lhs_returns_null_in_return(
        self, nullable_name_star: Star
    ) -> None:
        r = nullable_name_star.execute_query(
            "MATCH (p:Person) WHERE p.name IS NULL RETURN p.name ENDS WITH 'e' AS r"
        )
        assert pd.isna(r["r"].iloc[0])

    def test_not_ends_with_excludes_null(
        self, nullable_name_star: Star
    ) -> None:
        r = nullable_name_star.execute_query(
            "MATCH (p:Person) WHERE NOT (p.name ENDS WITH 'e') RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Bob"]


class TestContainsNull:
    def test_null_lhs_returns_null_in_return(
        self, nullable_name_star: Star
    ) -> None:
        r = nullable_name_star.execute_query(
            "MATCH (p:Person) WHERE p.name IS NULL RETURN p.name CONTAINS 'li' AS r"
        )
        assert pd.isna(r["r"].iloc[0])

    def test_not_contains_excludes_null(
        self, nullable_name_star: Star
    ) -> None:
        r = nullable_name_star.execute_query(
            "MATCH (p:Person) WHERE NOT (p.name CONTAINS 'li') RETURN p.name ORDER BY p.name"
        )
        assert list(r["name"]) == ["Bob"]


class TestRegexNull:
    def test_null_lhs_returns_null_in_return(
        self, nullable_name_star: Star
    ) -> None:
        r = nullable_name_star.execute_query(
            "MATCH (p:Person) WHERE p.name IS NULL RETURN p.name =~ 'A.*' AS r"
        )
        assert pd.isna(r["r"].iloc[0])


# ===========================================================================
# Unary operators  (-, +)
# ===========================================================================


class TestUnaryMinusNull:
    def test_neg_null_literal_returns_null(
        self, nullable_age_star: Star
    ) -> None:
        r = nullable_age_star.execute_query("RETURN -null AS r")
        assert pd.isna(r["r"].iloc[0])

    def test_neg_null_column_returns_null(
        self, nullable_age_star: Star
    ) -> None:
        r = nullable_age_star.execute_query(
            "MATCH (p:Person) WHERE p.age IS NULL RETURN -p.age AS r"
        )
        assert pd.isna(r["r"].iloc[0])

    def test_neg_mixed_column_propagates_null(
        self, nullable_age_star: Star
    ) -> None:
        r = nullable_age_star.execute_query(
            "MATCH (p:Person) RETURN -p.age AS r ORDER BY p.age"
        )
        vals = r["r"].tolist()
        non_null = [v for v in vals if not pd.isna(v)]
        null_count = sum(1 for v in vals if pd.isna(v))
        assert sorted(non_null) == [-35.0, -25.0]
        assert null_count == 1

    def test_neg_integer_unchanged(self, nullable_age_star: Star) -> None:
        r = nullable_age_star.execute_query("RETURN -5 AS r")
        assert r["r"].iloc[0] == -5

    def test_neg_float_unchanged(self, nullable_age_star: Star) -> None:
        r = nullable_age_star.execute_query("RETURN -3.14 AS r")
        assert abs(float(r["r"].iloc[0]) - (-3.14)) < 1e-9

    def test_neg_in_where_excludes_null(
        self, nullable_age_star: Star
    ) -> None:
        r = nullable_age_star.execute_query(
            "MATCH (p:Person) WHERE -p.age > -30 RETURN p.age ORDER BY p.age"
        )
        assert list(r["age"]) == [25.0]

    def test_pos_null_literal_returns_null(
        self, nullable_age_star: Star
    ) -> None:
        r = nullable_age_star.execute_query("RETURN +null AS r")
        assert pd.isna(r["r"].iloc[0])
