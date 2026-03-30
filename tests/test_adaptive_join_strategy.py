"""Integration tests for adaptive join strategy selection.

Verifies that:
1. QueryPlanner selects correct strategies based on input sizes
2. BackendEngine.join() accepts and routes strategy parameter
3. BindingFrame.join() automatically uses QueryPlanner for strategy selection
4. All strategies produce identical results (correctness invariant)
5. Strategy selection is wired end-to-end through query execution
"""

from __future__ import annotations

import pandas as pd
from pycypher.backend_engine import PandasBackend
from pycypher.query_planner import JoinStrategy, QueryPlanner
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

ID = "__ID__"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _build_asymmetric_context(
    n_persons: int = 100,
    n_companies: int = 5,
) -> tuple[Context, Star]:
    """Build a graph with asymmetric entity sizes for join strategy testing."""
    persons_df = pd.DataFrame(
        {
            ID: list(range(1, n_persons + 1)),
            "name": [f"P{i}" for i in range(1, n_persons + 1)],
        },
    )
    companies_df = pd.DataFrame(
        {
            ID: list(range(1, n_companies + 1)),
            "company_name": [f"C{i}" for i in range(1, n_companies + 1)],
        },
    )
    # Each person works at a company (round-robin)
    works_at_df = pd.DataFrame(
        {
            ID: list(range(1, n_persons + 1)),
            "__SOURCE__": list(range(1, n_persons + 1)),
            "__TARGET__": [(i % n_companies) + 1 for i in range(n_persons)],
            "role": ["engineer"] * n_persons,
        },
    )
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=persons_df,
    )
    company_table = EntityTable(
        entity_type="Company",
        identifier="Company",
        column_names=[ID, "company_name"],
        source_obj_attribute_map={"company_name": "company_name"},
        attribute_map={"company_name": "company_name"},
        source_obj=companies_df,
    )
    works_at_table = RelationshipTable(
        relationship_type="WORKS_AT",
        identifier="WORKS_AT",
        column_names=[ID, "__SOURCE__", "__TARGET__", "role"],
        source_obj_attribute_map={"role": "role"},
        attribute_map={"role": "role"},
        source_obj=works_at_df,
        source_entity_type="Person",
        target_entity_type="Company",
    )
    ctx = Context(
        entity_mapping=EntityMapping(
            mapping={"Person": person_table, "Company": company_table},
        ),
        relationship_mapping=RelationshipMapping(
            mapping={"WORKS_AT": works_at_table},
        ),
    )
    return ctx, Star(context=ctx)


# ---------------------------------------------------------------------------
# Strategy selection tests
# ---------------------------------------------------------------------------


class TestStrategySelection:
    """Verify QueryPlanner selects correct strategy based on sizes."""

    def test_asymmetric_selects_broadcast(self) -> None:
        """Small table joining large table gets broadcast."""
        planner = QueryPlanner()
        plan = planner.plan_join(
            left_name="persons",
            right_name="companies",
            left_rows=100_000,
            right_rows=5,
            join_key="__ID__",
        )
        assert plan.strategy == JoinStrategy.BROADCAST

    def test_symmetric_large_selects_hash(self) -> None:
        """Two large tables get hash join."""
        planner = QueryPlanner()
        plan = planner.plan_join(
            left_name="a",
            right_name="b",
            left_rows=50_000,
            right_rows=50_000,
            join_key="__ID__",
        )
        assert plan.strategy == JoinStrategy.HASH

    def test_sorted_inputs_select_merge(self) -> None:
        """Both sorted inputs get merge join."""
        planner = QueryPlanner()
        plan = planner.plan_join(
            left_name="a",
            right_name="b",
            left_rows=50_000,
            right_rows=50_000,
            join_key="__ID__",
            left_sorted=True,
            right_sorted=True,
        )
        assert plan.strategy == JoinStrategy.MERGE


# ---------------------------------------------------------------------------
# Backend strategy routing tests
# ---------------------------------------------------------------------------


class TestBackendStrategyRouting:
    """Verify PandasBackend.join() handles strategy parameter."""

    def test_auto_strategy_default(self) -> None:
        """Auto strategy produces correct results."""
        backend = PandasBackend()
        left = pd.DataFrame({"id": [1, 2, 3], "v1": ["a", "b", "c"]})
        right = pd.DataFrame({"id": [2, 3, 4], "v2": ["x", "y", "z"]})
        result = backend.join(left, right, on="id", strategy="auto")
        assert sorted(result["id"].tolist()) == [2, 3]

    def test_broadcast_strategy(self) -> None:
        """Broadcast strategy produces correct results."""
        backend = PandasBackend()
        left = pd.DataFrame({"id": range(100), "v1": range(100)})
        right = pd.DataFrame({"id": [10, 20, 30], "v2": ["a", "b", "c"]})
        result = backend.join(left, right, on="id", strategy="broadcast")
        assert sorted(result["id"].tolist()) == [10, 20, 30]

    def test_hash_strategy(self) -> None:
        """Hash strategy produces correct results."""
        backend = PandasBackend()
        left = pd.DataFrame({"id": [1, 2, 3], "v1": ["a", "b", "c"]})
        right = pd.DataFrame({"id": [2, 3, 4], "v2": ["x", "y", "z"]})
        result = backend.join(left, right, on="id", strategy="hash")
        assert sorted(result["id"].tolist()) == [2, 3]

    def test_merge_strategy(self) -> None:
        """Merge strategy produces correct results."""
        backend = PandasBackend()
        left = pd.DataFrame({"id": [1, 2, 3], "v1": ["a", "b", "c"]})
        right = pd.DataFrame({"id": [2, 3, 4], "v2": ["x", "y", "z"]})
        result = backend.join(left, right, on="id", strategy="merge")
        assert sorted(result["id"].tolist()) == [2, 3]

    def test_all_strategies_produce_identical_results(self) -> None:
        """All strategies produce the same output for the same input."""
        backend = PandasBackend()
        left = pd.DataFrame(
            {"id": [1, 2, 3, 4, 5], "val": ["a", "b", "c", "d", "e"]},
        )
        right = pd.DataFrame(
            {"id": [3, 4, 5, 6, 7], "score": [10, 20, 30, 40, 50]},
        )
        results = {}
        for strat in ("auto", "hash", "broadcast", "merge"):
            r = backend.join(left, right, on="id", strategy=strat)
            results[strat] = r.sort_values("id").reset_index(drop=True)

        for strat in ("hash", "broadcast", "merge"):
            pd.testing.assert_frame_equal(results["auto"], results[strat])

    def test_broadcast_swap_with_larger_right(self) -> None:
        """Broadcast with larger right side swaps sides for inner join."""
        backend = PandasBackend()
        small = pd.DataFrame({"id": [1, 2], "v": ["a", "b"]})
        large = pd.DataFrame({"id": list(range(100)), "w": list(range(100))})
        result = backend.join(small, large, on="id", strategy="broadcast")
        assert sorted(result["id"].tolist()) == [1, 2]


# ---------------------------------------------------------------------------
# BindingFrame integration tests
# ---------------------------------------------------------------------------


class TestBindingFrameJoinIntegration:
    """Verify BindingFrame.join() uses QueryPlanner automatically."""

    def test_join_produces_correct_results(self) -> None:
        """BindingFrame.join() still produces correct results with planner."""
        from pycypher.binding_frame import BindingFrame

        ctx, _ = _build_asymmetric_context()
        left = BindingFrame(
            bindings=pd.DataFrame({"a": [1, 2, 3, 4, 5]}),
            type_registry={"a": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame(
                {"a": [3, 4, 5, 6, 7], "b": [10, 20, 30, 40, 50]},
            ),
            type_registry={"b": "Company"},
            context=ctx,
        )
        result = left.join(right, "a", "a")
        assert sorted(result.bindings["a"].tolist()) == [3, 4, 5]

    def test_asymmetric_key_join_still_works(self) -> None:
        """Asymmetric key names fall through to direct merge."""
        from pycypher.binding_frame import BindingFrame

        ctx, _ = _build_asymmetric_context()
        left = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=ctx,
        )
        right = BindingFrame(
            bindings=pd.DataFrame({"_src_r": [1, 2, 3], "r": [10, 20, 30]}),
            type_registry={"r": "WORKS_AT"},
            context=ctx,
        )
        result = left.join(right, "p", "_src_r")
        assert len(result.bindings) == 3
        assert "p" in result.bindings.columns
        assert "r" in result.bindings.columns


# ---------------------------------------------------------------------------
# End-to-end query tests
# ---------------------------------------------------------------------------


class TestEndToEndQueryWithStrategy:
    """Verify queries work correctly with adaptive join strategies active."""

    def test_single_hop_query(self) -> None:
        """Single-hop query uses adaptive join strategy."""
        _, star = _build_asymmetric_context(n_persons=50, n_companies=3)
        result = star.execute_query(
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.company_name",
        )
        assert len(result) == 50  # Every person works at a company

    def test_filtered_hop_query(self) -> None:
        """Filtered query with adaptive joins."""
        _, star = _build_asymmetric_context(n_persons=20, n_companies=4)
        result = star.execute_query(
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
            "WHERE c.company_name = 'C1' RETURN p.name",
        )
        # 20 persons, 4 companies, round-robin assignment
        # Persons assigned to C1: those where (i % 4) + 1 == 1, i.e. i % 4 == 0
        # i=0→C1, i=4→C1, i=8→C1, i=12→C1, i=16→C1 → 5 persons
        assert len(result) == 5

    def test_limit_with_adaptive_join(self) -> None:
        """LIMIT works with adaptive join strategies."""
        _, star = _build_asymmetric_context(n_persons=100, n_companies=5)
        result = star.execute_query(
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
            "RETURN p.name, c.company_name LIMIT 10",
        )
        assert len(result) == 10
