"""Performance tests for BindingFrame.mutate().

The lambda-based write-back in mutate() is O(n²): for each row not in the
update_map it performs a full-table scan (source_df[ID_COLUMN] == eid).  With
5000 entities, 2500 of which are matched by a partial SET, that is roughly
2500 * 5000 = 12.5 million comparisons — enough to take several seconds in
pure Python.

These tests act as regression guards to ensure the O(n) vectorised
implementation is in place and never regresses to the lambda-based path.

TDD: tests were written before the O(n) fix was implemented.
"""

from __future__ import annotations

import time

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
from _perf_helpers import perf_threshold

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.performance


def _large_person_context(n: int) -> Context:
    """Create a Context with *n* Person entities, alternating departments."""
    ids = list(range(1, n + 1))
    names = [f"person_{i}" for i in ids]
    depts = ["Engineering" if i % 2 == 0 else "Sales" for i in ids]
    salaries = [50000.0 + i * 10 for i in ids]

    df = pd.DataFrame(
        {
            ID_COLUMN: ids,
            "name": names,
            "dept": depts,
            "salary": salaries,
        },
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "dept", "salary"],
        source_obj_attribute_map={
            "name": "name",
            "dept": "dept",
            "salary": "salary",
        },
        attribute_map={"name": "name", "dept": "dept", "salary": "salary"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


# ---------------------------------------------------------------------------
# Performance correctness: large partial SET must finish quickly
# ---------------------------------------------------------------------------


class TestMutatePerformance:
    """BindingFrame.mutate() must run in O(n) time, not O(n²)."""

    def test_partial_set_10000_entities_under_1_second(self) -> None:
        """A partial SET on 10000 entities must complete in under 1 second.

        O(n²) code with n=10000 takes ~2-3s on commodity hardware (confirmed
        by profiling: 1.2s for mutate() alone + MATCH overhead).  O(n) code
        completes the full query in under 0.1s.  The 1-second threshold
        comfortably catches any O(n²) regression while allowing CI variance.
        """
        ctx = _large_person_context(10000)
        star = Star(context=ctx)

        start = time.perf_counter()
        star.execute_query(
            "MATCH (p:Person) WHERE p.dept = 'Engineering' "
            "SET p.team = 'tech' "
            "RETURN p.name AS name",
        )
        elapsed = time.perf_counter() - start

        assert elapsed < perf_threshold(1.0), (
            f"mutate() took {elapsed:.2f}s for 10000 entities — "
            "likely O(n²) regression (expected < 1s with O(n) implementation)"
        )

    def test_partial_set_correctness_at_scale(self) -> None:
        """Partial SET at 10000-entity scale must produce correct values.

        Verifies both matched rows (even IDs → Engineering) and unmatched
        rows (odd IDs → Sales) contain the right post-SET state.
        """
        n = 10000
        ctx = _large_person_context(n)
        star = Star(context=ctx)

        star.execute_query(
            "MATCH (p:Person) WHERE p.dept = 'Engineering' SET p.team = 'tech'",
        )

        source = ctx.entity_mapping.mapping["Person"].source_obj

        # Engineering (even IDs) must have team='tech'
        eng_mask = source["dept"] == "Engineering"
        assert (source.loc[eng_mask, "team"] == "tech").all(), (
            "All Engineering persons must have team='tech'"
        )

        # Sales (odd IDs) must have team=None / NaN
        sales_mask = source["dept"] == "Sales"
        sales_teams = source.loc[sales_mask, "team"]
        assert (sales_teams.isna() | (sales_teams == None)).all(), (
            "All Sales persons must have team=None after partial SET"
        )

    def test_existing_property_preserved_at_scale(self) -> None:
        """Unmatched rows keep their original salary after partial SET.

        This guards against the O(n²) path accidentally writing wrong values
        for unmatched rows (not just being slow but potentially incorrect when
        the fallback scan returns the wrong row index).
        """
        n = 1000
        ctx = _large_person_context(n)
        star = Star(context=ctx)

        # Record original salaries for odd-ID (Sales) persons
        source_before = (
            ctx.entity_mapping.mapping["Person"]
            .source_obj.copy()
            .set_index(ID_COLUMN)
        )

        star.execute_query(
            "MATCH (p:Person) WHERE p.dept = 'Engineering' SET p.salary = 999999",
        )

        source_after = ctx.entity_mapping.mapping[
            "Person"
        ].source_obj.set_index(
            ID_COLUMN,
        )

        # Sales persons (odd IDs 1, 3, 5, ...) must have their original salary
        for i in range(1, n + 1, 2):  # odd = Sales
            original = source_before.at[i, "salary"]
            current = source_after.at[i, "salary"]
            assert abs(float(current) - float(original)) < 0.01, (
                f"Person {i} (Sales) salary changed from {original} to {current}"
            )

    def test_full_set_10000_entities_under_1_second(self) -> None:
        """Full-table SET (no WHERE) on 10000 entities must complete in under 1 second.

        Full-table SET means all n rows are in update_map, so the O(n²) path
        is not hit (every row calls update_map.get() which is O(1)).  This test
        verifies the fast path stays fast.
        """
        ctx = _large_person_context(10000)
        star = Star(context=ctx)

        start = time.perf_counter()
        star.execute_query(
            "MATCH (p:Person) SET p.score = 42 RETURN p.name AS name",
        )
        elapsed = time.perf_counter() - start

        assert elapsed < perf_threshold(1.0), (
            f"Full-table mutate() took {elapsed:.2f}s for 10000 entities — "
            "unexpected slowdown (expected < 1s)"
        )
