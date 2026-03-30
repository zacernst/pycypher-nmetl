"""Dedicated unit tests for ExistsEvaluator.

The existing test_exists_*.py files exercise EXISTS through full query
execution (integration tests).  These tests target ExistsEvaluator methods
directly, covering edge cases and error paths that integration tests may miss.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from pycypher.ast_models import (
    IntegerLiteral,
    Match,
    NodePattern,
    Pattern,
    PatternComprehension,
    PatternPath,
    Query,
    RelationshipDirection,
    RelationshipPattern,
    Return,
    ReturnItem,
    Variable,
)
from pycypher.binding_frame import BindingFrame
from pycypher.exceptions import PatternComprehensionError
from pycypher.exists_evaluator import _EXISTS_SENTINEL, ExistsEvaluator
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def simple_context() -> Context:
    """Minimal context: 3 Person nodes, 2 KNOWS edges (Alice->Bob, Alice->Carol)."""
    people_df = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3], "name": ["Alice", "Bob", "Carol"]}
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [101, 102],
            RELATIONSHIP_SOURCE_COLUMN: [1, 1],
            RELATIONSHIP_TARGET_COLUMN: [2, 3],
        }
    )
    return Context(
        entity_mapping=EntityMapping(
            mapping={
                "Person": EntityTable(
                    entity_type="Person",
                    identifier="Person",
                    column_names=[ID_COLUMN, "name"],
                    source_obj_attribute_map={"name": "name"},
                    attribute_map={"name": "name"},
                    source_obj=people_df,
                )
            }
        ),
        relationship_mapping=RelationshipMapping(
            mapping={
                "KNOWS": RelationshipTable(
                    relationship_type="KNOWS",
                    identifier="KNOWS",
                    column_names=[ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN],
                    source_obj_attribute_map={},
                    attribute_map={},
                    source_obj=knows_df,
                )
            }
        ),
    )


@pytest.fixture()
def alice_frame(simple_context: Context) -> BindingFrame:
    """Frame with 3 bound person rows."""
    bindings = pd.DataFrame({"p": [1, 2, 3]})
    return BindingFrame(
        bindings=bindings,
        type_registry={"p": "Person"},
        context=simple_context,
    )


@pytest.fixture()
def evaluator_mock() -> MagicMock:
    """Mock for ExpressionEvaluatorProtocol."""
    return MagicMock()


# ---------------------------------------------------------------------------
# evaluate_exists: edge cases
# ---------------------------------------------------------------------------


class TestEvaluateExistsEdgeCases:
    """Edge cases in evaluate_exists dispatch logic."""

    def test_non_pattern_non_query_returns_all_false(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """Content that is neither Pattern nor Query → all False."""
        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_exists("not a pattern", evaluator_mock)
        assert list(result) == [False, False, False]

    def test_none_content_returns_all_false(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """None content → all False."""
        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_exists(None, evaluator_mock)
        assert list(result) == [False, False, False]

    def test_integer_content_returns_all_false(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """Arbitrary non-AST content → all False."""
        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_exists(42, evaluator_mock)
        assert list(result) == [False, False, False]

    def test_empty_frame_returns_empty_series(
        self, simple_context: Context, evaluator_mock: MagicMock
    ) -> None:
        """Empty frame → empty boolean series."""
        empty_frame = BindingFrame(
            bindings=pd.DataFrame({"p": pd.Series([], dtype=int)}),
            type_registry={"p": "Person"},
            context=simple_context,
        )
        ee = ExistsEvaluator(empty_frame)
        result = ee.evaluate_exists("anything", evaluator_mock)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# evaluate_exists: Query content
# ---------------------------------------------------------------------------


class TestEvaluateExistsQuery:
    """EXISTS with Query content (full subquery path)."""

    def test_query_without_return_gets_synthetic_return(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """Query lacking RETURN clause gets a synthetic RETURN 1 AS _exists_flag."""
        source = NodePattern(variable=Variable(name="p"), labels=["Person"])
        target = NodePattern(variable=None, labels=["Person"])
        rel = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        path = PatternPath(elements=[source, rel, target])
        pattern = Pattern(paths=[path])
        match = Match(pattern=pattern, where=None)
        subquery = Query(clauses=[match])  # No RETURN

        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_exists(subquery, evaluator_mock)

        # Alice (id=1) knows Bob and Carol → True
        # Bob (id=2) and Carol (id=3) know nobody → False
        assert result.dtype == bool
        assert result.iloc[0] is np.bool_(True)
        assert result.iloc[1] is np.bool_(False)
        assert result.iloc[2] is np.bool_(False)

    def test_query_with_return_uses_existing_return(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """Query with RETURN clause is used as-is."""
        source = NodePattern(variable=Variable(name="p"), labels=["Person"])
        target = NodePattern(variable=Variable(name="f"), labels=["Person"])
        rel = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        path = PatternPath(elements=[source, rel, target])
        pattern = Pattern(paths=[path])
        match = Match(pattern=pattern, where=None)
        ret = Return(
            items=[ReturnItem(expression=Variable(name="f"), alias="f")]
        )
        subquery = Query(clauses=[match, ret])

        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_exists(subquery, evaluator_mock)

        assert result.dtype == bool
        assert result.iloc[0] is np.bool_(True)  # Alice knows someone


# ---------------------------------------------------------------------------
# evaluate_exists: Pattern content
# ---------------------------------------------------------------------------


class TestEvaluateExistsPattern:
    """EXISTS with Pattern content (single-hop shortcut path)."""

    def test_single_hop_pattern_uses_comprehension_shortcut(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """Single-hop pattern goes through pattern comprehension."""
        source = NodePattern(variable=Variable(name="p"), labels=["Person"])
        target = NodePattern(variable=None, labels=["Person"])
        rel = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        path = PatternPath(elements=[source, rel, target])
        pattern = Pattern(paths=[path])

        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_exists(pattern, evaluator_mock)

        assert result.dtype == bool
        assert result.iloc[0] is np.bool_(True)   # Alice knows people
        assert result.iloc[1] is np.bool_(False)   # Bob knows nobody
        assert result.iloc[2] is np.bool_(False)   # Carol knows nobody

    def test_multi_hop_pattern_falls_back_to_query(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """Multi-hop pattern (!=3 elements) falls back to query execution."""
        source = NodePattern(variable=Variable(name="p"), labels=["Person"])
        mid = NodePattern(variable=None, labels=["Person"])
        rel1 = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        rel2 = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        target = NodePattern(variable=None, labels=["Person"])
        path = PatternPath(elements=[source, rel1, mid, rel2, target])
        pattern = Pattern(paths=[path])

        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_exists(pattern, evaluator_mock)

        # Result should be boolean series of correct length
        assert result.dtype == bool
        assert len(result) == 3

    def test_empty_pattern_no_paths(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """Pattern with no paths → falls back to query (0 elements != 3)."""
        pattern = Pattern(paths=[])
        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_exists(pattern, evaluator_mock)
        # Empty pattern with no paths: content.paths is empty list,
        # so the multi-hop check fails and it falls to query execution
        assert len(result) == 3


# ---------------------------------------------------------------------------
# evaluate_pattern_comprehension: error paths
# ---------------------------------------------------------------------------


class TestPatternComprehensionErrors:
    """Error handling in evaluate_pattern_comprehension."""

    def test_none_pattern_returns_empty_lists(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """PatternComprehension with None pattern → empty lists."""
        pc = PatternComprehension(
            pattern=None, variable=None, where=None, map_expr=None
        )
        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_pattern_comprehension(pc, evaluator_mock)
        assert all(lst == [] for lst in result)

    def test_empty_paths_returns_empty_lists(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """PatternComprehension with empty paths → empty lists."""
        pc = PatternComprehension(
            pattern=Pattern(paths=[]),
            variable=None,
            where=None,
            map_expr=None,
        )
        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_pattern_comprehension(pc, evaluator_mock)
        assert all(lst == [] for lst in result)

    def test_non_single_hop_raises_error(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """Multi-hop pattern in comprehension → PatternComprehensionError."""
        source = NodePattern(variable=Variable(name="p"), labels=["Person"])
        mid = NodePattern(variable=None, labels=["Person"])
        rel1 = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        rel2 = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        target = NodePattern(variable=None, labels=["Person"])
        path = PatternPath(elements=[source, rel1, mid, rel2, target])

        pc = PatternComprehension(
            pattern=Pattern(paths=[path]),
            variable=None,
            where=None,
            map_expr=None,
        )
        ee = ExistsEvaluator(alice_frame)
        with pytest.raises(PatternComprehensionError, match="single-hop"):
            ee.evaluate_pattern_comprehension(pc, evaluator_mock)

    def test_non_node_source_raises_error(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """Non-NodePattern as source element → PatternComprehensionError."""
        rel_as_source = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        rel = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        target = NodePattern(variable=None, labels=["Person"])
        path = PatternPath(elements=[rel_as_source, rel, target])

        pc = PatternComprehension(
            pattern=Pattern(paths=[path]),
            variable=None,
            where=None,
            map_expr=None,
        )
        ee = ExistsEvaluator(alice_frame)
        with pytest.raises(PatternComprehensionError, match="NodePattern"):
            ee.evaluate_pattern_comprehension(pc, evaluator_mock)

    def test_non_relationship_middle_raises_error(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """Non-RelationshipPattern as middle element → PatternComprehensionError."""
        source = NodePattern(variable=Variable(name="p"), labels=["Person"])
        node_as_rel = NodePattern(variable=None, labels=["Person"])
        target = NodePattern(variable=None, labels=["Person"])
        path = PatternPath(elements=[source, node_as_rel, target])

        pc = PatternComprehension(
            pattern=Pattern(paths=[path]),
            variable=None,
            where=None,
            map_expr=None,
        )
        ee = ExistsEvaluator(alice_frame)
        with pytest.raises(PatternComprehensionError, match="RelationshipPattern"):
            ee.evaluate_pattern_comprehension(pc, evaluator_mock)


# ---------------------------------------------------------------------------
# evaluate_pattern_comprehension: functional paths
# ---------------------------------------------------------------------------


class TestPatternComprehensionFunctional:
    """Functional tests for pattern comprehension evaluation."""

    def test_right_direction_returns_targets(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """(p)-[:KNOWS]->(f) returns target IDs for each anchor."""
        source = NodePattern(variable=Variable(name="p"), labels=["Person"])
        target = NodePattern(variable=Variable(name="f"), labels=["Person"])
        rel = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        path = PatternPath(elements=[source, rel, target])
        pc = PatternComprehension(
            pattern=Pattern(paths=[path]),
            variable=None,
            where=None,
            map_expr=None,
        )
        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_pattern_comprehension(pc, evaluator_mock)

        # Alice (1) knows Bob (2) and Carol (3)
        assert sorted(result.iloc[0]) == [2, 3]
        # Bob and Carol know nobody
        assert result.iloc[1] == []
        assert result.iloc[2] == []

    def test_left_direction_returns_sources(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """(p)<-[:KNOWS]-(f) returns source IDs (incoming edges)."""
        source = NodePattern(variable=Variable(name="p"), labels=["Person"])
        target = NodePattern(variable=Variable(name="f"), labels=["Person"])
        rel = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.LEFT,
        )
        path = PatternPath(elements=[source, rel, target])
        pc = PatternComprehension(
            pattern=Pattern(paths=[path]),
            variable=None,
            where=None,
            map_expr=None,
        )
        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_pattern_comprehension(pc, evaluator_mock)

        # Alice (1) has no incoming KNOWS
        assert result.iloc[0] == []
        # Bob (2) is known by Alice (1)
        assert result.iloc[1] == [1]
        # Carol (3) is known by Alice (1)
        assert result.iloc[2] == [1]

    def test_unknown_rel_type_returns_empty_lists(
        self, alice_frame: BindingFrame, evaluator_mock: MagicMock
    ) -> None:
        """Relationship type not in context → empty lists."""
        source = NodePattern(variable=Variable(name="p"), labels=["Person"])
        target = NodePattern(variable=Variable(name="f"), labels=["Person"])
        rel = RelationshipPattern(
            variable=None,
            labels=["NONEXISTENT"],
            direction=RelationshipDirection.RIGHT,
        )
        path = PatternPath(elements=[source, rel, target])
        pc = PatternComprehension(
            pattern=Pattern(paths=[path]),
            variable=None,
            where=None,
            map_expr=None,
        )
        ee = ExistsEvaluator(alice_frame)
        result = ee.evaluate_pattern_comprehension(pc, evaluator_mock)

        assert all(lst == [] for lst in result)

    def test_anchor_var_not_in_frame_tries_other(
        self, simple_context: Context, evaluator_mock: MagicMock
    ) -> None:
        """When source var not in frame but target var is, uses target as anchor."""
        # Frame has 'f' bound but source pattern uses 'p'
        frame = BindingFrame(
            bindings=pd.DataFrame({"f": [2, 3]}),  # Bob, Carol
            type_registry={"f": "Person"},
            context=simple_context,
        )
        source = NodePattern(variable=Variable(name="p"), labels=["Person"])
        target = NodePattern(variable=Variable(name="f"), labels=["Person"])
        rel = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        path = PatternPath(elements=[source, rel, target])
        pc = PatternComprehension(
            pattern=Pattern(paths=[path]),
            variable=None,
            where=None,
            map_expr=None,
        )
        ee = ExistsEvaluator(frame)
        result = ee.evaluate_pattern_comprehension(pc, evaluator_mock)

        # f=2 (Bob) is target of Alice(1)->Bob(2), so anchor flips: source=1
        assert result.iloc[0] == [1]  # Bob's incoming source
        # f=3 (Carol) is target of Alice(1)->Carol(3)
        assert result.iloc[1] == [1]  # Carol's incoming source

    def test_neither_var_in_frame_returns_empty(
        self, simple_context: Context, evaluator_mock: MagicMock
    ) -> None:
        """Neither source nor target var in frame → empty lists."""
        frame = BindingFrame(
            bindings=pd.DataFrame({"x": [1, 2]}),
            type_registry={},
            context=simple_context,
        )
        source = NodePattern(variable=Variable(name="p"), labels=["Person"])
        target = NodePattern(variable=Variable(name="f"), labels=["Person"])
        rel = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        path = PatternPath(elements=[source, rel, target])
        pc = PatternComprehension(
            pattern=Pattern(paths=[path]),
            variable=None,
            where=None,
            map_expr=None,
        )
        ee = ExistsEvaluator(frame)
        result = ee.evaluate_pattern_comprehension(pc, evaluator_mock)
        assert all(lst == [] for lst in result)


# ---------------------------------------------------------------------------
# _exists_via_query_execution: edge cases
# ---------------------------------------------------------------------------


class TestExistsViaQueryExecution:
    """Edge cases for the batch query execution path."""

    def test_sentinel_column_name(self) -> None:
        """Sentinel column name is defined and non-empty."""
        assert _EXISTS_SENTINEL == "__exists_row_idx__"

    def test_no_matches_returns_all_false(
        self, simple_context: Context
    ) -> None:
        """Subquery where no rows match → all False.

        Uses IDs that don't appear in the KNOWS relationship source column,
        so the MATCH finds no paths.
        """
        frame = BindingFrame(
            bindings=pd.DataFrame({"p": [99, 98, 97]}),  # IDs not in any relationship
            type_registry={"p": "Person"},
            context=simple_context,
        )
        source = NodePattern(variable=Variable(name="p"), labels=["Person"])
        target = NodePattern(variable=None, labels=["Person"])
        rel = RelationshipPattern(
            variable=None,
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
        )
        path = PatternPath(elements=[source, rel, target])
        pattern = Pattern(paths=[path])
        match = Match(pattern=pattern, where=None)
        ret = Return(
            items=[ReturnItem(expression=IntegerLiteral(value=1), alias="_flag")]
        )
        subquery = Query(clauses=[match, ret])

        ee = ExistsEvaluator(frame)
        result = ee._exists_via_query_execution(subquery)
        assert list(result) == [False, False, False]
