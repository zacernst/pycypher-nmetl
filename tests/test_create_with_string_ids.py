"""TDD tests for CREATE/MERGE with string-ID entity contexts.

When a context has string IDs (e.g. ``__ID__: ["p1", "p2", "p3"]``),
``_next_entity_ids`` calls ``int(candidate)`` on the max ID, which raises
``ValueError: invalid literal for int() with base 10: 'p3'``.

The fix must:
- Fall back gracefully when existing IDs cannot be parsed as integers.
- Assign sequential integer IDs (1, 2, 3, ...) starting from 1 for the new rows.
- Not break contexts with integer IDs.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ingestion import ContextBuilder
from pycypher.star import Star


@pytest.fixture()
def string_id_star() -> Star:
    """Star with string IDs like 'p1', 'p2', 'p3'."""
    ctx = ContextBuilder.from_dict(
        {
            "Person": pd.DataFrame(
                {
                    "__ID__": ["p1", "p2", "p3"],
                    "name": ["Alice", "Bob", "Carol"],
                    "age": [30, 25, 35],
                }
            )
        }
    )
    return Star(context=ctx)


@pytest.fixture()
def int_id_star() -> Star:
    """Star with integer IDs — must still work after the fix."""
    ctx = ContextBuilder.from_dict(
        {
            "Person": pd.DataFrame(
                {
                    "__ID__": [1, 2, 3],
                    "name": ["Alice", "Bob", "Carol"],
                    "age": [30, 25, 35],
                }
            )
        }
    )
    return Star(context=ctx)


class TestCreateWithStringIds:
    """CREATE must not raise ValueError when existing IDs are strings."""

    def test_create_does_not_raise_with_string_ids(
        self, string_id_star: Star
    ) -> None:
        """CREATE with string-ID context must not raise ValueError."""
        string_id_star.execute_query(
            "CREATE (:Person {name: 'Dave', age: 40})"
        )

    def test_create_adds_row_with_string_ids(
        self, string_id_star: Star
    ) -> None:
        """After CREATE with string-ID context, new row appears in MATCH."""
        string_id_star.execute_query(
            "CREATE (:Person {name: 'Dave', age: 40})"
        )
        result = string_id_star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name"
        )
        assert "Dave" in result["name"].tolist()

    def test_create_preserves_existing_rows_with_string_ids(
        self, string_id_star: Star
    ) -> None:
        """Existing rows survive CREATE with string IDs."""
        string_id_star.execute_query(
            "CREATE (:Person {name: 'Dave', age: 40})"
        )
        result = string_id_star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name"
        )
        assert "Alice" in result["name"].tolist()
        assert "Bob" in result["name"].tolist()
        assert len(result) == 4


class TestMergeWithStringIds:
    """MERGE must not raise ValueError when existing IDs are strings."""

    def test_merge_does_not_raise_with_string_ids(
        self, string_id_star: Star
    ) -> None:
        """MERGE with string-ID context must not raise ValueError."""
        string_id_star.execute_query("MERGE (:Person {name: 'Dave'})")

    def test_merge_on_create_set_does_not_raise_with_string_ids(
        self, string_id_star: Star
    ) -> None:
        """MERGE … ON CREATE SET with string IDs must not raise ValueError."""
        string_id_star.execute_query(
            "MERGE (p:Person {name: 'Eve'}) ON CREATE SET p.age = 99"
        )

    def test_merge_on_create_set_sets_property_with_string_ids(
        self, string_id_star: Star
    ) -> None:
        """ON CREATE SET applies the property after MERGE creates the node."""
        string_id_star.execute_query(
            "MERGE (p:Person {name: 'Eve'}) ON CREATE SET p.age = 99"
        )
        result = string_id_star.execute_query(
            "MATCH (p:Person {name: 'Eve'}) RETURN p.age AS age"
        )
        assert len(result) == 1
        assert int(result["age"].iloc[0]) == 99


class TestCreateRegressionIntIds:
    """Integer-ID CREATE must still work correctly after the fix."""

    def test_create_with_integer_ids_still_works(
        self, int_id_star: Star
    ) -> None:
        """CREATE with integer IDs produces new row above the current max."""
        int_id_star.execute_query("CREATE (:Person {name: 'Dave', age: 40})")
        result = int_id_star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name"
        )
        assert "Dave" in result["name"].tolist()
        assert len(result) == 4

    def test_create_from_empty_table_with_int_ids(self) -> None:
        """CREATE into an empty entity table assigns ID 1."""
        ctx = ContextBuilder.from_dict({})
        star = Star(context=ctx)
        star.execute_query("CREATE (:Widget {label: 'foo'})")
        result = star.execute_query("MATCH (w:Widget) RETURN w.label AS label")
        assert result["label"].iloc[0] == "foo"
