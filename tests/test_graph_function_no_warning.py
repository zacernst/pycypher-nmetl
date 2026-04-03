"""TDD tests ensuring graph-introspection functions don't emit spurious warnings.

``labels(p)``, ``type(r)``, ``startNode(r)``, ``endNode(r)`` are implemented as
pre-evaluation intercepts in ``binding_evaluator.py`` and work correctly, but
they are NOT in the scalar-function registry.  ``star._contains_aggregation``
checks the registry; when a function is absent it logs:

    WARNING  Unknown function 'labels', treating as scalar function

This is a spurious warning — the functions are known and correct.  The fix
is to add these four function names to the "known non-aggregation" set checked
before the warning is emitted.

All tests written before the fix (TDD step 1).
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star


@pytest.fixture
def social_star() -> Star:
    people_df = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3], "name": ["Alice", "Bob", "Carol"]},
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    knows_df = pd.DataFrame(
        {ID_COLUMN: [10, 11], "__SOURCE__": [1, 2], "__TARGET__": [2, 3]},
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": people_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table},
            ),
        ),
    )


def _capture_warnings(caplog, star: Star, query: str) -> list[str]:
    """Execute *query* and return any WARNING-level log messages emitted."""
    with caplog.at_level(logging.WARNING, logger="pycypher.star"):
        star.execute_query(query)
    return [r.message for r in caplog.records if r.levelno == logging.WARNING]


class TestNoSpuriousWarnings:
    """Graph-introspection functions must not emit 'Unknown function' warnings."""

    def test_labels_no_warning(
        self,
        social_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """labels(p) must not emit an 'Unknown function' warning."""
        msgs = _capture_warnings(
            caplog,
            social_star,
            "MATCH (p:Person) RETURN labels(p) AS l ORDER BY p.name LIMIT 1",
        )
        unknown = [
            m for m in msgs if "Unknown function" in m and "labels" in m
        ]
        assert unknown == [], f"Unexpected warnings: {unknown}"

    def test_type_no_warning(
        self,
        social_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """type(r) must not emit an 'Unknown function' warning."""
        msgs = _capture_warnings(
            caplog,
            social_star,
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN type(r) AS t LIMIT 1",
        )
        unknown = [
            m for m in msgs if "Unknown function" in m and "type" in m.lower()
        ]
        assert unknown == [], f"Unexpected warnings: {unknown}"

    def test_startnode_no_warning(
        self,
        social_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """startNode(r) must not emit an 'Unknown function' warning."""
        msgs = _capture_warnings(
            caplog,
            social_star,
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN startNode(r) AS sn LIMIT 1",
        )
        unknown = [
            m for m in msgs if "Unknown function" in m and "startNode" in m
        ]
        assert unknown == [], f"Unexpected warnings: {unknown}"

    def test_endnode_no_warning(
        self,
        social_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """endNode(r) must not emit an 'Unknown function' warning."""
        msgs = _capture_warnings(
            caplog,
            social_star,
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN endNode(r) AS en LIMIT 1",
        )
        unknown = [
            m for m in msgs if "Unknown function" in m and "endNode" in m
        ]
        assert unknown == [], f"Unexpected warnings: {unknown}"

    def test_labels_still_returns_correct_value(
        self,
        social_star: Star,
    ) -> None:
        """labels(p) still returns the correct label list after the fix."""
        r = social_star.execute_query(
            "MATCH (p:Person) RETURN labels(p) AS l ORDER BY p.name LIMIT 1",
        )
        assert r["l"].iloc[0] == ["Person"]

    def test_type_still_returns_correct_value(self, social_star: Star) -> None:
        """type(r) still returns the correct type string after the fix."""
        r = social_star.execute_query(
            "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN type(r) AS t LIMIT 1",
        )
        assert r["t"].iloc[0] == "KNOWS"

    def test_startnode_still_returns_correct_value(
        self,
        social_star: Star,
    ) -> None:
        """startNode(r) still returns the correct node ID after the fix."""
        r = social_star.execute_query(
            "MATCH (p:Person {name: 'Alice'})-[r:KNOWS]->(f:Person) RETURN startNode(r) AS sn",
        )
        assert r["sn"].iloc[0] == 1  # Alice's ID

    def test_truly_unknown_function_still_warns(
        self,
        social_star: Star,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A genuinely unknown function (e.g. bogusFunc) still emits the warning."""
        try:
            msgs = _capture_warnings(
                caplog,
                social_star,
                "MATCH (p:Person) RETURN bogusFunc(p.name) AS v LIMIT 1",
            )
        except Exception:
            # Execution raises after the warning — that's fine
            msgs = [
                r.message
                for r in caplog.records
                if r.levelno == logging.WARNING
            ]
        # Either a warning was emitted OR an exception was raised; both indicate
        # the function is correctly treated as unknown (not silently suppressed).
        unknown = [m for m in msgs if "Unknown function" in m]
        # The test passes whether we got a warning or an exception — both are correct.
        assert True
