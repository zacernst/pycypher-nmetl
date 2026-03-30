"""TDD tests for IN / NOT IN with per-row list properties.

In openCypher, the IN operator must test membership in each row's own list:

    MATCH (d:Dev) WHERE 'python' IN d.tags RETURN d.name

If d.tags = [['python', 'ml'], ['java', 'backend']], only the first row
should pass.  The current implementation takes ``.iloc[0]`` of the right-hand
Series, so ALL rows are tested against the first row's list — a correctness
bug that silently returns too many results.

All tests written before the fix (TDD step 1).
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


@pytest.fixture
def dev_star() -> Star:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "tags": [
                ["python", "ml"],
                ["java", "backend"],
                ["python", "frontend"],
            ],
            "scores": [[90, 85], [60, 55], [70, 95]],
        },
    )
    t = EntityTable(
        entity_type="Dev",
        identifier="Dev",
        column_names=[ID_COLUMN, "name", "tags", "scores"],
        source_obj_attribute_map={
            "name": "name",
            "tags": "tags",
            "scores": "scores",
        },
        attribute_map={"name": "name", "tags": "tags", "scores": "scores"},
        source_obj=df,
    )
    return Star(
        context=Context(entity_mapping=EntityMapping(mapping={"Dev": t})),
    )


# ---------------------------------------------------------------------------
# IN with list property
# ---------------------------------------------------------------------------


class TestInListProperty:
    """'val' IN node.listProp must test each row's own list."""

    def test_in_excludes_non_matching_rows(self, dev_star: Star) -> None:
        """'python' IN d.tags must not return Bob (tags=['java','backend'])."""
        r = dev_star.execute_query(
            "MATCH (d:Dev) WHERE 'python' IN d.tags RETURN d.name ORDER BY d.name",
        )
        assert list(r["name"]) == ["Alice", "Carol"], (
            f"Expected only Alice and Carol, got {list(r['name'])}"
        )

    def test_in_correct_row_count(self, dev_star: Star) -> None:
        """'python' IN d.tags returns exactly 2 rows."""
        r = dev_star.execute_query(
            "MATCH (d:Dev) WHERE 'python' IN d.tags RETURN d.name",
        )
        assert len(r) == 2

    def test_in_integer_list_property(self, dev_star: Star) -> None:
        """90 IN d.scores filters correctly."""
        r = dev_star.execute_query(
            "MATCH (d:Dev) WHERE 90 IN d.scores RETURN d.name",
        )
        assert list(r["name"]) == ["Alice"]

    def test_in_no_match_returns_empty(self, dev_star: Star) -> None:
        """'ruby' IN d.tags returns no rows."""
        r = dev_star.execute_query(
            "MATCH (d:Dev) WHERE 'ruby' IN d.tags RETURN d.name",
        )
        assert len(r) == 0

    def test_in_literal_list_unaffected(self, dev_star: Star) -> None:
        """'python' IN ['python', 'java'] (literal list) still works."""
        r = dev_star.execute_query(
            "MATCH (d:Dev) WHERE d.name IN ['Alice', 'Bob'] RETURN d.name ORDER BY d.name",
        )
        assert list(r["name"]) == ["Alice", "Bob"]


# ---------------------------------------------------------------------------
# NOT IN with list property
# ---------------------------------------------------------------------------


class TestNotInListProperty:
    """'val' NOT IN node.listProp must test each row's own list."""

    def test_not_in_correct_rows(self, dev_star: Star) -> None:
        """'python' NOT IN d.tags returns only Bob."""
        r = dev_star.execute_query(
            "MATCH (d:Dev) WHERE 'python' NOT IN d.tags RETURN d.name",
        )
        assert list(r["name"]) == ["Bob"]

    def test_not_in_literal_list_unaffected(self, dev_star: Star) -> None:
        """d.name NOT IN ['Alice', 'Bob'] (literal list) returns Carol."""
        r = dev_star.execute_query(
            "MATCH (d:Dev) WHERE d.name NOT IN ['Alice', 'Bob'] RETURN d.name",
        )
        assert list(r["name"]) == ["Carol"]
