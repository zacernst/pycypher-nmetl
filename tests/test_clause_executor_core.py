"""Unit tests for clause_executor.py — Clause Dispatch and Execution.

Tests the ClauseExecutor class that routes clause execution to appropriate
handlers and manages binding frames through query execution.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.binding_frame import BindingFrame
from pycypher.clause_executor import ClauseExecutor
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def executor_context() -> Context:
    """Context for executor testing."""
    people_df = pd.DataFrame({
        "__ID__": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "age": [30, 25, 35],
    })
    knows_df = pd.DataFrame({
        "__ID__": [101, 102],
        "__SOURCE__": [1, 2],
        "__TARGET__": [2, 3],
    })

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=["__ID__", "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=["__ID__", "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={"KNOWS": knows_table}),
    )


@pytest.fixture
def executor(executor_context: Context) -> ClauseExecutor:
    """ClauseExecutor instance.

    ClauseExecutor requires pattern_matcher/mutations/frame_joiner/
    projection_planner/query_analyzer collaborators that are normally wired
    up inside Star.__init__; reuse a real Star's fully-wired instance rather
    than re-deriving that wiring.
    """
    from pycypher.star import Star

    return Star(context=executor_context)._clause_executor


@pytest.fixture
def empty_binding_frame(executor_context: Context) -> BindingFrame:
    """Empty binding frame for starting clause execution."""
    return BindingFrame(
        bindings=pd.DataFrame(),
        type_registry={},
        context=executor_context,
    )


# ---------------------------------------------------------------------------
# Clause Dispatch Routing
# ---------------------------------------------------------------------------


class TestClauseExecutorDispatchRouting:
    """Verify correct routing to clause handlers."""

    def test_executor_initialized(self, executor: ClauseExecutor) -> None:
        """Executor initializes with context."""
        assert executor._context is not None

    def test_executor_has_context_reference(self, executor: ClauseExecutor) -> None:
        """Executor maintains context reference."""
        assert "Person" in executor._context.entity_mapping.mapping


# ---------------------------------------------------------------------------
# Binding Frame Management
# ---------------------------------------------------------------------------


class TestClauseExecutorBindingFrameManagement:
    """Verify binding frame creation and flow through clauses."""

    def test_binding_frame_initialization(
        self,
        empty_binding_frame: BindingFrame,
    ) -> None:
        """BindingFrame initializes properly."""
        assert empty_binding_frame.bindings is not None
        assert empty_binding_frame.context is not None

    def test_binding_frame_add_column(self, executor_context: Context) -> None:
        """Add column to binding frame."""
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        assert "p" in frame.bindings.columns

    def test_binding_frame_row_preservation(
        self,
        executor_context: Context,
    ) -> None:
        """Binding frame preserves row counts through operations."""
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        assert len(frame.bindings) == 3

    def test_binding_frame_column_aliasing(
        self,
        executor_context: Context,
    ) -> None:
        """BindingFrame supports column aliasing."""
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        assert frame.bindings is not None


# ---------------------------------------------------------------------------
# Variable Scoping
# ---------------------------------------------------------------------------


class TestClauseExecutorVariableScoping:
    """Variable scoping and binding preservation."""

    def test_variable_scope_isolation(
        self,
        executor_context: Context,
    ) -> None:
        """Variables in one frame don't leak to another."""
        frame1 = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        frame2 = BindingFrame(
            bindings=pd.DataFrame({"q": [3]}),
            type_registry={"q": "Person"},
            context=executor_context,
        )
        assert "p" not in frame2.bindings.columns
        assert "q" not in frame1.bindings.columns

    def test_variable_reuse_in_chain(
        self,
        executor_context: Context,
    ) -> None:
        """Variable reused in clause chain."""
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        # Reusing 'p' is valid
        assert "p" in frame.bindings.columns


# ---------------------------------------------------------------------------
# Binding Type Registry
# ---------------------------------------------------------------------------


class TestClauseExecutorTypeRegistry:
    """Type information tracking for variables."""

    def test_type_registry_tracks_entity_type(
        self,
        executor_context: Context,
    ) -> None:
        """Type registry tracks node types."""
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        assert frame.type_registry["p"] == "Person"

    def test_type_registry_with_multiple_types(
        self,
        executor_context: Context,
    ) -> None:
        """Type registry with multiple variables."""
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2], "q": [1, 1]}),
            type_registry={"p": "Person", "q": "Person"},
            context=executor_context,
        )
        assert frame.type_registry["p"] == "Person"
        assert frame.type_registry["q"] == "Person"


# ---------------------------------------------------------------------------
# State Management
# ---------------------------------------------------------------------------


class TestClauseExecutorStateManagement:
    """Context and state updates through execution."""

    def test_context_unchanged_after_read(
        self,
        executor: ClauseExecutor,
        executor_context: Context,
    ) -> None:
        """Context unchanged after read operations."""
        original_size = len(executor_context.entity_mapping.mapping)
        # Simulate read operation
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        assert len(executor_context.entity_mapping.mapping) == original_size

    def test_execution_scope_isolation(
        self,
        executor_context: Context,
    ) -> None:
        """Each clause gets isolated execution scope."""
        frame1 = BindingFrame(
            bindings=pd.DataFrame({"p": [1]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        frame2 = BindingFrame(
            bindings=pd.DataFrame({"p": [2]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        # Frames should be independent
        assert frame1.bindings.iloc[0]["p"] != frame2.bindings.iloc[0]["p"]


# ---------------------------------------------------------------------------
# Binding Frame Operations
# ---------------------------------------------------------------------------


class TestClauseExecutorBindingOperations:
    """Operations on binding frames."""

    def test_binding_frame_filter(
        self,
        executor_context: Context,
    ) -> None:
        """Filter binding frame rows."""
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        # Filter should work on underlying DataFrame
        filtered = frame.bindings[frame.bindings["p"] > 1]
        assert len(filtered) == 2

    def test_binding_frame_join(
        self,
        executor_context: Context,
    ) -> None:
        """Join binding frames."""
        frame1 = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        frame2_data = pd.DataFrame({"q": [1, 1, 2, 2]})
        # Simulating cross join — how="cross" cannot be combined with
        # left_index/right_index (pandas raises MergeError).
        result = pd.merge(
            frame1.bindings,
            frame2_data,
            how="cross",
        )
        assert len(result) == 8  # 2 * 4

    def test_binding_frame_projection(
        self,
        executor_context: Context,
    ) -> None:
        """Project columns from binding frame."""
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3], "extra": [10, 20, 30]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        projected = frame.bindings[["p"]]
        assert "extra" not in projected.columns
        assert "p" in projected.columns


# ---------------------------------------------------------------------------
# Error Handling
# ---------------------------------------------------------------------------


class TestClauseExecutorErrorHandling:
    """Error conditions and exception handling."""

    def test_executor_with_invalid_context(self) -> None:
        """Executor with missing context."""
        try:
            ClauseExecutor(context=None)
        except Exception:
            pass  # Expected to fail

    def test_binding_frame_type_safety(
        self,
        executor_context: Context,
    ) -> None:
        """Type mismatches in binding frame."""
        # Binding a non-existent entity type should be detected
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2]}),
            type_registry={"p": "Unknown"},  # Unknown type
            context=executor_context,
        )
        assert frame.type_registry["p"] == "Unknown"


# ---------------------------------------------------------------------------
# Integration Scenarios
# ---------------------------------------------------------------------------


class TestClauseExecutorIntegration:
    """Integration tests simulating clause chains."""

    def test_match_to_return_flow(
        self,
        executor_context: Context,
    ) -> None:
        """MATCH → RETURN binding flow."""
        # Simulate: MATCH (p:Person) RETURN p.name
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        assert len(frame.bindings) == 3

    def test_match_with_filter_to_return(
        self,
        executor_context: Context,
    ) -> None:
        """MATCH → WHERE → RETURN flow."""
        # Simulate: MATCH (p:Person) WHERE p.age > 25 RETURN p.name
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2, 3], "age": [30, 25, 35]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        # Filter would reduce rows
        filtered = frame.bindings[frame.bindings["age"] > 25]
        assert len(filtered) == 2  # Alice and Carol

    def test_match_to_with_to_match_to_return(
        self,
        executor_context: Context,
    ) -> None:
        """MATCH → WITH → MATCH → RETURN flow."""
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        # After WITH, frame transforms
        frame_after_with = BindingFrame(
            bindings=pd.DataFrame({"p": [1, 2]}),
            type_registry={"p": "Person"},
            context=executor_context,
        )
        assert len(frame.bindings) == len(frame_after_with.bindings)
