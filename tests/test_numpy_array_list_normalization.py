"""TDD tests for numpy-array list normalization at the property boundary.

Root cause: ``get_property`` normalises NaN→None (Loop 193) but does NOT
normalise ``numpy.ndarray`` → ``list``.  When a DataFrame column stores
list-valued properties (e.g. ``p.hobbies = ['reading', 'hiking']``),
pandas keeps the values as ``numpy.ndarray`` objects in ``object``-dtype
columns.  Downstream scalar functions (``head``, ``tail``, ``last``,
``IN``, ``size``, list-min/max) all check ``isinstance(x, list)`` and
silently fall through to the "null or unknown type" branch, producing
``None`` or raising a ``ValueError``.

Fix: in ``get_property``, after the NaN→None normalisation introduced in
Loop 193, add a second pass that converts any ``numpy.ndarray`` element
to a Python ``list`` via ``.tolist()``.

TDD: all tests in this file are written *before* the fix (red phase).
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def list_ctx() -> Context:
    """Context with list-valued 'hobbies' and 'scores' properties."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "hobbies": [
                ["reading", "hiking"],
                ["chess"],
                ["painting", "cooking", "hiking"],
            ],
            "scores": [[10, 20, 30], [5], [7, 14]],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "hobbies", "scores"],
        source_obj_attribute_map={
            "name": "name",
            "hobbies": "hobbies",
            "scores": "scores",
        },
        attribute_map={
            "name": "name",
            "hobbies": "hobbies",
            "scores": "scores",
        },
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture()
def star(list_ctx: Context) -> Star:
    return Star(context=list_ctx)


# ---------------------------------------------------------------------------
# Class 1: Property boundary — get_property returns Python list, not ndarray
# ---------------------------------------------------------------------------


class TestListPropertyNormalization:
    """get_property must return Python lists, not numpy.ndarray, for list props."""

    def test_hobbies_property_is_python_list(self, star: Star) -> None:
        """List property values must be Python list instances, not numpy arrays."""
        result = star.execute_query("MATCH (p:Person) RETURN p.hobbies AS h")
        for i, val in enumerate(result["h"]):
            assert isinstance(val, list), (
                f"Row {i}: expected list, got {type(val).__name__}: {val!r}"
            )

    def test_scores_property_is_python_list(self, star: Star) -> None:
        """Numeric list property values must be Python lists."""
        result = star.execute_query("MATCH (p:Person) RETURN p.scores AS s")
        for i, val in enumerate(result["s"]):
            assert isinstance(val, list), (
                f"Row {i}: expected list, got {type(val).__name__}: {val!r}"
            )

    def test_string_property_unaffected(self, star: Star) -> None:
        """Non-list string properties must not be wrapped in a list."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS n ORDER BY p.name"
        )
        assert result["n"].tolist() == ["Alice", "Bob", "Carol"]


# ---------------------------------------------------------------------------
# Class 2: head() / last() / tail() on list properties
# ---------------------------------------------------------------------------


class TestHeadLastTailOnListProperties:
    """head(), last(), and tail() must work on list-typed property columns."""

    def test_head_returns_first_element(self, star: Star) -> None:
        """head(p.hobbies) → first hobby string."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN head(p.hobbies) AS h ORDER BY p.name"
        )
        assert list(result["h"]) == ["reading", "chess", "painting"]

    def test_last_returns_last_element(self, star: Star) -> None:
        """last(p.hobbies) → last hobby string."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN last(p.hobbies) AS h ORDER BY p.name"
        )
        assert list(result["h"]) == ["hiking", "chess", "hiking"]

    def test_tail_returns_rest(self, star: Star) -> None:
        """tail(p.hobbies) → list without first element."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN tail(p.hobbies) AS t ORDER BY p.name"
        )
        assert result["t"].iloc[0] == ["hiking"]
        assert result["t"].iloc[1] == []
        assert result["t"].iloc[2] == ["cooking", "hiking"]

    def test_head_on_numeric_list(self, star: Star) -> None:
        """head(p.scores) → first integer."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN head(p.scores) AS h ORDER BY p.name"
        )
        assert list(result["h"]) == [10, 5, 7]

    def test_tail_single_element_list_returns_empty(self, star: Star) -> None:
        """tail([x]) → [] for a single-element list property."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' RETURN tail(p.hobbies) AS t"
        )
        assert result["t"].iloc[0] == []


# ---------------------------------------------------------------------------
# Class 3: IN operator on list properties
# ---------------------------------------------------------------------------


class TestInOperatorOnListProperties:
    """'value' IN p.listProp must work for list-typed property values."""

    def test_in_string_list_property_true(self, star: Star) -> None:
        """'hiking' IN p.hobbies → True for Alice and Carol."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE 'hiking' IN p.hobbies RETURN p.name ORDER BY p.name"
        )
        assert list(result["name"]) == ["Alice", "Carol"]

    def test_in_string_list_property_false(self, star: Star) -> None:
        """'chess' IN p.hobbies → True only for Bob."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE 'chess' IN p.hobbies RETURN p.name ORDER BY p.name"
        )
        assert list(result["name"]) == ["Bob"]

    def test_not_in_list_property(self, star: Star) -> None:
        """'hiking' NOT IN p.hobbies → True for Bob."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE NOT ('hiking' IN p.hobbies) "
            "RETURN p.name ORDER BY p.name"
        )
        assert list(result["name"]) == ["Bob"]

    def test_in_numeric_list_property(self, star: Star) -> None:
        """20 IN p.scores → True only for Alice."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE 20 IN p.scores RETURN p.name ORDER BY p.name"
        )
        assert list(result["name"]) == ["Alice"]

    def test_in_absent_element_is_false(self, star: Star) -> None:
        """99 IN p.hobbies → False for all rows."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE 99 IN p.scores RETURN p.name ORDER BY p.name"
        )
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Class 4: size() and list comprehension on list properties (regression)
# ---------------------------------------------------------------------------


class TestSizeAndComprehensionRegression:
    """size() and list comprehension already worked; must remain correct."""

    def test_size_still_works(self, star: Star) -> None:
        """size(p.hobbies) must return element count."""
        result = star.execute_query(
            "MATCH (p:Person) RETURN size(p.hobbies) AS n ORDER BY p.name"
        )
        assert list(result["n"]) == [2, 1, 3]

    def test_list_comprehension_still_works(self, star: Star) -> None:
        """[h IN p.hobbies | toUpper(h)] must work on list properties."""
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN [h IN p.hobbies | toUpper(h)] AS uh"
        )
        assert result["uh"].iloc[0] == ["READING", "HIKING"]

    def test_any_quantifier_on_list_property(self, star: Star) -> None:
        """any(h IN p.hobbies WHERE h STARTS WITH 'h') → True for rows with hiking."""
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN any(h IN p.hobbies WHERE h STARTS WITH 'h') AS a "
            "ORDER BY p.name"
        )
        values = list(result["a"])
        assert values[0] is True  # Alice: 'hiking' starts with 'h'
        assert values[1] is False  # Bob: only 'chess'
        assert values[2] is True  # Carol: 'hiking' starts with 'h'


# ---------------------------------------------------------------------------
# Class 5: ContextBuilder / PyArrow path — the broken path before the fix
# ---------------------------------------------------------------------------


@pytest.fixture()
def pyarrow_star() -> Star:
    """Star built via ContextBuilder.from_dict() — stores data as PyArrow Table.

    PyArrow-to-pandas conversion yields numpy.ndarray for list columns.
    This is the primary path that triggered the bug.
    """
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "hobbies": [
                ["reading", "hiking"],
                ["chess"],
                ["painting", "cooking", "hiking"],
            ],
            "scores": [[10, 20, 30], [5], [7, 14]],
        }
    )
    ctx = ContextBuilder.from_dict({"Person": df})
    return Star(context=ctx)


class TestContextBuilderListNormalization:
    """Via ContextBuilder (PyArrow-backed), list operations must work correctly."""

    def test_in_operator_via_contextbuilder(self, pyarrow_star: Star) -> None:
        """'hiking' IN p.hobbies must not raise ValueError via ContextBuilder path."""
        result = pyarrow_star.execute_query(
            "MATCH (p:Person) WHERE 'hiking' IN p.hobbies RETURN p.name ORDER BY p.name"
        )
        assert list(result["name"]) == ["Alice", "Carol"]

    def test_head_via_contextbuilder(self, pyarrow_star: Star) -> None:
        """head(p.hobbies) must return first element via ContextBuilder path."""
        result = pyarrow_star.execute_query(
            "MATCH (p:Person) RETURN head(p.hobbies) AS h ORDER BY p.name"
        )
        assert list(result["h"]) == ["reading", "chess", "painting"]

    def test_tail_via_contextbuilder(self, pyarrow_star: Star) -> None:
        """tail(p.hobbies) must return rest of list via ContextBuilder path."""
        result = pyarrow_star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN tail(p.hobbies) AS t"
        )
        assert result["t"].iloc[0] == ["hiking"]

    def test_property_is_python_list_via_contextbuilder(
        self, pyarrow_star: Star
    ) -> None:
        """List properties must be Python list, not ndarray, via ContextBuilder."""
        result = pyarrow_star.execute_query(
            "MATCH (p:Person) RETURN p.hobbies AS h ORDER BY p.name"
        )
        for i, val in enumerate(result["h"]):
            assert isinstance(val, list), (
                f"Row {i}: expected list, got {type(val).__name__}: {val!r}"
            )

    def test_size_via_contextbuilder(self, pyarrow_star: Star) -> None:
        """size(p.hobbies) must return correct count via ContextBuilder path."""
        result = pyarrow_star.execute_query(
            "MATCH (p:Person) RETURN size(p.hobbies) AS n ORDER BY p.name"
        )
        assert list(result["n"]) == [2, 1, 3]
