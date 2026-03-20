"""Tests for collection/utility scalar functions: coalesce, size, head, tail, last.

TDD red phase → green phase.
"""

import math

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star


@pytest.fixture
def registry() -> ScalarFunctionRegistry:
    return ScalarFunctionRegistry.get_instance()


@pytest.fixture
def person_context() -> Context:
    """People with optional nickname and tags."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "nickname": ["Ali", None, None],
            "tags": [["python", "graph"], ["java"], ["python", "sql"]],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "nickname", "tags"],
        source_obj_attribute_map={
            "name": "name",
            "nickname": "nickname",
            "tags": "tags",
        },
        attribute_map={"name": "name", "nickname": "nickname", "tags": "tags"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


def _is_null(v: object) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


# ─────────────────────────────────────────────────────────────────────────────
# coalesce()
# ─────────────────────────────────────────────────────────────────────────────


class TestCoalesce:
    def test_coalesce_returns_first_non_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute(
            "coalesce",
            [pd.Series([None]), pd.Series([None]), pd.Series(["default"])],
        )
        assert result.iloc[0] == "default"

    def test_coalesce_returns_first_argument_when_non_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute(
            "coalesce",
            [pd.Series(["first"]), pd.Series(["second"])],
        )
        assert result.iloc[0] == "first"

    def test_coalesce_returns_null_when_all_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute(
            "coalesce",
            [pd.Series([None]), pd.Series([None]), pd.Series([None])],
        )
        assert _is_null(result.iloc[0])

    def test_coalesce_treats_nan_as_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute(
            "coalesce",
            [pd.Series([float("nan")]), pd.Series(["fallback"])],
        )
        assert result.iloc[0] == "fallback"

    def test_coalesce_with_zero_is_not_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute(
            "coalesce",
            [pd.Series([0]), pd.Series(["fallback"])],
        )
        assert result.iloc[0] == 0

    def test_coalesce_with_empty_string_is_not_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute(
            "coalesce",
            [pd.Series([""]), pd.Series(["fallback"])],
        )
        assert result.iloc[0] == ""

    def test_coalesce_vectorized_mixed_nulls(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        """Coalesce works row-by-row: some rows get first arg, others fall through."""
        result = registry.execute(
            "coalesce",
            [
                pd.Series(["Ali", None, None]),
                pd.Series(["Alice", "Bob", "Carol"]),
            ],
        )
        assert result.tolist() == ["Ali", "Bob", "Carol"]

    def test_coalesce_integration(self, person_context: Context) -> None:
        """coalesce(p.nickname, p.name) returns nickname for Alice, name for others."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH coalesce(p.nickname, p.name) AS display "
            "RETURN display"
        )
        assert len(result) == 3
        display_values = set(result["display"].tolist())
        assert "Ali" in display_values
        assert "Bob" in display_values
        assert "Carol" in display_values
        assert "Alice" not in display_values


# ─────────────────────────────────────────────────────────────────────────────
# size()
# ─────────────────────────────────────────────────────────────────────────────


class TestSize:
    def test_size_of_list(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("size", [pd.Series([[1, 2, 3]])])
        assert result.iloc[0] == 3

    def test_size_of_string(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("size", [pd.Series(["hello"])])
        assert result.iloc[0] == 5

    def test_size_of_empty_list(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute("size", [pd.Series([[]])])
        assert result.iloc[0] == 0

    def test_size_of_empty_string(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute("size", [pd.Series([""])])
        assert result.iloc[0] == 0

    def test_size_of_null_returns_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute("size", [pd.Series([None])])
        assert _is_null(result.iloc[0])

    def test_size_vectorized(self, registry: ScalarFunctionRegistry) -> None:
        result = registry.execute("size", [pd.Series([["a", "b"], ["c"], []])])
        assert result.tolist() == [2, 1, 0]

    def test_size_integration_on_list_column(
        self, person_context: Context
    ) -> None:
        """size(p.tags) returns the number of tags each person has."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS name, size(p.tags) AS tag_count "
            "RETURN name, tag_count"
        )
        assert len(result) == 3
        row = result[result["name"] == "Alice"]
        assert int(row["tag_count"].iloc[0]) == 2
        row = result[result["name"] == "Bob"]
        assert int(row["tag_count"].iloc[0]) == 1
        row = result[result["name"] == "Carol"]
        assert int(row["tag_count"].iloc[0]) == 2


# ─────────────────────────────────────────────────────────────────────────────
# head(), tail(), last()
# ─────────────────────────────────────────────────────────────────────────────


class TestHeadTailLast:
    def test_head_returns_first_element(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute("head", [pd.Series([[10, 20, 30]])])
        assert result.iloc[0] == 10

    def test_head_of_empty_list_returns_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute("head", [pd.Series([[]])])
        assert _is_null(result.iloc[0])

    def test_tail_returns_all_but_first(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute("tail", [pd.Series([[10, 20, 30]])])
        assert result.iloc[0] == [20, 30]

    def test_tail_of_singleton_returns_empty(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute("tail", [pd.Series([[42]])])
        assert result.iloc[0] == []

    def test_last_returns_final_element(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute("last", [pd.Series([[10, 20, 30]])])
        assert result.iloc[0] == 30

    def test_last_of_empty_list_returns_null(
        self, registry: ScalarFunctionRegistry
    ) -> None:
        result = registry.execute("last", [pd.Series([[]])])
        assert _is_null(result.iloc[0])

    def test_head_integration(self, person_context: Context) -> None:
        """head(p.tags) returns the first tag."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS name, head(p.tags) AS first_tag "
            "WHERE name = 'Alice' "
            "RETURN name, first_tag"
        )
        assert result["first_tag"].iloc[0] == "python"

    def test_last_integration(self, person_context: Context) -> None:
        """last(p.tags) returns the last tag for Carol."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.name AS name, last(p.tags) AS last_tag "
            "WHERE name = 'Carol' "
            "RETURN name, last_tag"
        )
        assert result["last_tag"].iloc[0] == "sql"
