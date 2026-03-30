"""Unit tests for :mod:`pycypher.frame_joiner`.

Tests the five public methods of ``FrameJoiner``:

- ``coerce_join`` — strategy selection (keyed vs cross join).
- ``merge_frames_for_match`` — multi-MATCH frame merging.
- ``process_optional_match`` — OPTIONAL MATCH left-join semantics.
- ``process_optional_match_failure`` — null-column injection on failure.
- ``make_seed_frame`` — synthetic single-row seed frame.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pandas as pd
from pycypher.frame_joiner import FrameJoiner

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_joiner(
    *,
    context: Any = None,
    match_fn: Any = None,
    where_fn: Any = None,
) -> FrameJoiner:
    """Build a FrameJoiner with mocked dependencies."""
    return FrameJoiner(
        context=context or MagicMock(),
        match_fn=match_fn or MagicMock(),
        where_fn=where_fn or MagicMock(),
    )


def _mock_frame(
    var_names: list[str], bindings: pd.DataFrame | None = None,
) -> MagicMock:
    """Create a mock BindingFrame with specified var_names."""
    frame = MagicMock()
    frame.var_names = var_names
    frame.bindings = bindings if bindings is not None else pd.DataFrame()
    frame.type_registry = {}
    frame.context = MagicMock()
    return frame


# ---------------------------------------------------------------------------
# coerce_join
# ---------------------------------------------------------------------------


class TestCoerceJoin:
    """Test join strategy selection."""

    def test_shared_variable_uses_keyed_join(self) -> None:
        """When frames share a variable, inner join is used."""
        joiner = _make_joiner()
        frame_a = _mock_frame(["x", "y"])
        frame_b = _mock_frame(["y", "z"])
        joined = MagicMock()
        frame_a.join = MagicMock(return_value=joined)

        result = joiner.coerce_join(frame_a, frame_b)

        frame_a.join.assert_called_once_with(frame_b, "y", "y", join_plan=None)
        assert result is joined

    def test_no_shared_variable_uses_cross_join(self) -> None:
        """When no variables are shared, cross join is used."""
        joiner = _make_joiner()
        frame_a = _mock_frame(["x"])
        frame_b = _mock_frame(["y"])
        crossed = MagicMock()
        frame_a.cross_join = MagicMock(return_value=crossed)

        result = joiner.coerce_join(frame_a, frame_b)

        frame_a.cross_join.assert_called_once_with(frame_b)
        assert result is crossed

    def test_multiple_shared_variables_picks_one(self) -> None:
        """When multiple variables are shared, one is picked for the join."""
        joiner = _make_joiner()
        frame_a = _mock_frame(["x", "y", "z"])
        frame_b = _mock_frame(["y", "z"])
        joined = MagicMock()
        frame_a.join = MagicMock(return_value=joined)

        result = joiner.coerce_join(frame_a, frame_b)

        # Should have used one of the shared vars
        frame_a.join.assert_called_once()
        call_args = frame_a.join.call_args[0]
        assert call_args[1] in {"y", "z"}
        assert call_args[1] == call_args[2]  # left_col == right_col

    def test_empty_var_names_uses_cross_join(self) -> None:
        """Frames with no variables do a cross join."""
        joiner = _make_joiner()
        frame_a = _mock_frame([])
        frame_b = _mock_frame([])
        crossed = MagicMock()
        frame_a.cross_join = MagicMock(return_value=crossed)

        result = joiner.coerce_join(frame_a, frame_b)

        frame_a.cross_join.assert_called_once()


# ---------------------------------------------------------------------------
# merge_frames_for_match
# ---------------------------------------------------------------------------


class TestMergeFramesForMatch:
    """Test multi-MATCH frame merging."""

    def test_merge_without_where(self) -> None:
        joiner = _make_joiner()
        current = _mock_frame(["x"])
        match = _mock_frame(["y"])
        joined = MagicMock()
        current.cross_join = MagicMock(return_value=joined)

        result = joiner.merge_frames_for_match(current, match)
        assert result is joined

    def test_merge_with_where(self) -> None:
        where_fn = MagicMock()
        filtered = MagicMock()
        where_fn.return_value = filtered

        joiner = _make_joiner(where_fn=where_fn)
        current = _mock_frame(["x"])
        match = _mock_frame(["y"])
        joined = MagicMock()
        current.cross_join = MagicMock(return_value=joined)

        where_expr = MagicMock()
        result = joiner.merge_frames_for_match(current, match, where_clause=where_expr)

        where_fn.assert_called_once_with(where_expr, joined)
        assert result is filtered

    def test_merge_shared_variable_uses_keyed_join(self) -> None:
        joiner = _make_joiner()
        current = _mock_frame(["p"])
        match = _mock_frame(["p", "r"])
        joined = MagicMock()
        current.join = MagicMock(return_value=joined)

        result = joiner.merge_frames_for_match(current, match)

        current.join.assert_called_once_with(match, "p", "p", join_plan=None)

    def test_pushdown_filters_match_only_vars_before_join(self) -> None:
        """WHERE referencing only match_frame vars is applied before the join."""
        from pycypher.ast_models import Comparison, PropertyLookup, Variable

        where_fn = MagicMock(side_effect=lambda expr, frame: frame)
        joiner = _make_joiner(where_fn=where_fn)
        current = _mock_frame(["x"])
        match = _mock_frame(["y"])
        joined = MagicMock()
        current.cross_join = MagicMock(return_value=joined)

        # WHERE y.name = 'Alice' — references only 'y' (match_frame var)
        where_expr = Comparison(
            left=PropertyLookup(variable=Variable(name="y"), property_name="name"),
            operator="=",
            right=Variable(name="y"),
        )
        joiner.merge_frames_for_match(current, match, where_clause=where_expr)

        # where_fn should be called with match frame (pre-join), not joined
        assert where_fn.call_count == 1
        call_args = where_fn.call_args
        assert call_args[0][1] is match  # applied to match_frame, not joined

    def test_cross_frame_predicate_stays_post_join(self) -> None:
        """WHERE referencing vars from both frames stays after the join."""
        from pycypher.ast_models import Comparison, PropertyLookup, Variable

        filtered = MagicMock()
        where_fn = MagicMock(return_value=filtered)
        joiner = _make_joiner(where_fn=where_fn)
        current = _mock_frame(["x"])
        match = _mock_frame(["y"])
        joined = MagicMock()
        current.cross_join = MagicMock(return_value=joined)

        # WHERE x.id = y.id — references both 'x' and 'y'
        where_expr = Comparison(
            left=PropertyLookup(variable=Variable(name="x"), property_name="id"),
            operator="=",
            right=PropertyLookup(variable=Variable(name="y"), property_name="id"),
        )
        result = joiner.merge_frames_for_match(current, match, where_clause=where_expr)

        # where_fn should be called post-join on the joined frame
        where_fn.assert_called_once_with(where_expr, joined)
        assert result is filtered

    def test_and_predicate_split_pushdown(self) -> None:
        """AND with mixed vars: match-only conjuncts push down, rest stay."""
        from pycypher.ast_models import And, Comparison, PropertyLookup, Variable

        call_log: list[tuple[Any, Any]] = []

        def tracking_where(expr: Any, frame: Any) -> Any:
            call_log.append((expr, frame))
            return frame

        joiner = _make_joiner(where_fn=tracking_where)
        current = _mock_frame(["x"])
        match = _mock_frame(["y"])
        joined = MagicMock()
        current.cross_join = MagicMock(return_value=joined)

        # y.age > 21 (pushable) AND x.id = y.id (not pushable)
        conj_push = Comparison(
            left=PropertyLookup(variable=Variable(name="y"), property_name="age"),
            operator=">",
            right=Variable(name="y"),
        )
        conj_stay = Comparison(
            left=PropertyLookup(variable=Variable(name="x"), property_name="id"),
            operator="=",
            right=PropertyLookup(variable=Variable(name="y"), property_name="id"),
        )
        where_expr = And(operands=[conj_push, conj_stay])

        joiner.merge_frames_for_match(current, match, where_clause=where_expr)

        # Two calls: first pre-join on match, then post-join
        assert len(call_log) == 2
        # First call: pushable predicate on match_frame
        assert call_log[0][0] is conj_push
        assert call_log[0][1] is match
        # Second call: remaining predicate on joined frame
        assert call_log[1][0] is conj_stay
        assert call_log[1][1] is joined


# ---------------------------------------------------------------------------
# process_optional_match
# ---------------------------------------------------------------------------


class TestProcessOptionalMatch:
    """Test OPTIONAL MATCH processing."""

    def test_successful_match_with_shared_variable(self) -> None:
        match_frame = _mock_frame(["p", "f"])
        left_joined = MagicMock()

        match_fn = MagicMock(return_value=match_frame)
        current = _mock_frame(["p"])
        current.left_join = MagicMock(return_value=left_joined)

        joiner = _make_joiner(match_fn=match_fn)

        clause = MagicMock()
        result = joiner.process_optional_match(clause, current)

        current.left_join.assert_called_once_with(match_frame, "p", "p")
        assert result is left_joined

    def test_successful_match_no_shared_variable_nonempty(self) -> None:
        match_frame = _mock_frame(["q"], bindings=pd.DataFrame({"q": [1]}))
        cross_joined = MagicMock()

        match_fn = MagicMock(return_value=match_frame)
        current = _mock_frame(["p"])
        current.cross_join = MagicMock(return_value=cross_joined)

        joiner = _make_joiner(match_fn=match_fn)

        clause = MagicMock()
        result = joiner.process_optional_match(clause, current)

        current.cross_join.assert_called_once()
        assert result is cross_joined

    def test_successful_match_no_shared_variable_empty(self) -> None:
        """Empty match result with no shared vars → failure path."""
        match_frame = _mock_frame(
            ["q"], bindings=pd.DataFrame({"q": pd.Series([], dtype=object)}),
        )

        match_fn = MagicMock(return_value=match_frame)
        current = _mock_frame(["p"])
        current.bindings = pd.DataFrame({"p": [1, 2]})
        current.type_registry = {}
        current.context = MagicMock()

        joiner = _make_joiner(match_fn=match_fn)

        # Build a clause with a pattern that has no new variables
        clause = MagicMock()
        clause.pattern.paths = []

        result = joiner.process_optional_match(clause, current)
        # Should go through failure path and return current (no new vars)

    def test_match_raises_value_error_triggers_failure(self) -> None:
        """ValueError from match_fn → optional match failure path."""
        match_fn = MagicMock(side_effect=ValueError("GraphTypeNotFoundError"))
        current = _mock_frame(["p"])
        current.bindings = pd.DataFrame({"p": [1]})
        current.type_registry = {}
        current.context = MagicMock()

        joiner = _make_joiner(match_fn=match_fn)

        clause = MagicMock()
        clause.pattern.paths = []

        result = joiner.process_optional_match(clause, current)
        # Should not raise, should return current unchanged

    def test_match_raises_key_error_triggers_failure(self) -> None:
        """KeyError from match_fn → optional match failure path."""
        match_fn = MagicMock(side_effect=KeyError("missing_col"))
        current = _mock_frame(["p"])
        current.bindings = pd.DataFrame({"p": [1]})
        current.type_registry = {}
        current.context = MagicMock()

        joiner = _make_joiner(match_fn=match_fn)

        clause = MagicMock()
        clause.pattern.paths = []

        result = joiner.process_optional_match(clause, current)


# ---------------------------------------------------------------------------
# process_optional_match_failure
# ---------------------------------------------------------------------------


class TestProcessOptionalMatchFailure:
    """Test null-column injection for failed OPTIONAL MATCH."""

    def test_no_new_variables_returns_current(self) -> None:
        """If the failed match introduces no new variables, return frame unchanged."""
        joiner = _make_joiner()
        current = _mock_frame(["p"])
        current.bindings = pd.DataFrame({"p": [1, 2]})
        current.type_registry = {"p": "Person"}
        current.context = MagicMock()

        clause = MagicMock()
        clause.pattern.paths = []

        result = joiner.process_optional_match_failure(clause, current)
        assert list(result.bindings.columns) == ["p"]

    def test_new_node_variable_gets_null_column(self) -> None:
        """Failed OPTIONAL MATCH adds null column for unbound node variable."""
        from pycypher.ast_models import NodePattern, Variable

        joiner = _make_joiner()
        current = _mock_frame(["p"])
        current.var_names = ["p"]
        current.bindings = pd.DataFrame({"p": [1, 2]})
        current.type_registry = {"p": "Person"}
        current.context = MagicMock()

        node = NodePattern(
            variable=Variable(name="f"),
            labels=["Friend"],
        )
        path = MagicMock()
        path.elements = [node]

        clause = MagicMock()
        clause.pattern.paths = [path]

        result = joiner.process_optional_match_failure(clause, current)

        assert "f" in result.bindings.columns
        assert result.bindings["f"].isna().all()
        assert result.type_registry.get("f") == "Friend"

    def test_new_relationship_variable_gets_null_column(self) -> None:
        """Failed OPTIONAL MATCH adds null column for unbound relationship variable."""
        from pycypher.ast_models import RelationshipPattern, Variable

        joiner = _make_joiner()
        current = _mock_frame(["p"])
        current.var_names = ["p"]
        current.bindings = pd.DataFrame({"p": [1, 2]})
        current.type_registry = {}
        current.context = MagicMock()

        # Use a mock that matches what process_optional_match_failure expects:
        # isinstance check for RelationshipPattern + .variable + .rel_types
        rel = MagicMock(spec=RelationshipPattern)
        rel.variable = Variable(name="r")
        rel.rel_types = ["KNOWS"]
        path = MagicMock()
        path.elements = [rel]

        clause = MagicMock()
        clause.pattern.paths = [path]

        result = joiner.process_optional_match_failure(clause, current)

        assert "r" in result.bindings.columns
        assert result.bindings["r"].isna().all()
        assert result.type_registry.get("r") == "KNOWS"

    def test_existing_variable_not_duplicated(self) -> None:
        """Variables already in current frame are not added again."""
        from pycypher.ast_models import NodePattern, Variable

        joiner = _make_joiner()
        current = _mock_frame(["p"])
        current.var_names = ["p"]
        current.bindings = pd.DataFrame({"p": [1, 2]})
        current.type_registry = {"p": "Person"}
        current.context = MagicMock()

        # p already exists in current
        node = NodePattern(variable=Variable(name="p"), labels=["Person"])
        path = MagicMock()
        path.elements = [node]

        clause = MagicMock()
        clause.pattern.paths = [path]

        result = joiner.process_optional_match_failure(clause, current)
        # No new columns added
        assert list(result.bindings.columns) == ["p"]

    def test_anonymous_nodes_skipped(self) -> None:
        """Nodes without variables don't add columns."""
        from pycypher.ast_models import NodePattern

        joiner = _make_joiner()
        current = _mock_frame(["p"])
        current.var_names = ["p"]
        current.bindings = pd.DataFrame({"p": [1]})
        current.type_registry = {}
        current.context = MagicMock()

        anon_node = NodePattern(variable=None, labels=["Thing"])
        path = MagicMock()
        path.elements = [anon_node]

        clause = MagicMock()
        clause.pattern.paths = [path]

        result = joiner.process_optional_match_failure(clause, current)
        assert list(result.bindings.columns) == ["p"]


# ---------------------------------------------------------------------------
# make_seed_frame
# ---------------------------------------------------------------------------


class TestMakeSeedFrame:
    """Test synthetic single-row seed frame creation."""

    def test_seed_frame_has_one_row(self) -> None:
        ctx = MagicMock()
        joiner = _make_joiner(context=ctx)

        result = joiner.make_seed_frame()

        assert len(result.bindings) == 1

    def test_seed_frame_has_row_column(self) -> None:
        joiner = _make_joiner()
        result = joiner.make_seed_frame()
        assert "_row" in result.bindings.columns

    def test_seed_frame_empty_type_registry(self) -> None:
        joiner = _make_joiner()
        result = joiner.make_seed_frame()
        assert result.type_registry == {}

    def test_seed_frame_context_propagated(self) -> None:
        ctx = MagicMock()
        joiner = _make_joiner(context=ctx)
        result = joiner.make_seed_frame()
        assert result.context is ctx
