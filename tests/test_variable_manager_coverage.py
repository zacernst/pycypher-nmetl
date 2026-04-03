"""Coverage gap tests for pycypher.variable_manager.

Targets uncovered code paths:
- SecurityError when generate_unique_name exhausts attempts
- SecurityError when _deep_copy_ast exceeds nesting depth
- _copy_field_value branches: dict, tuple, plain values, direct Variable
- _deep_copy_ast when no fields change (model_copy without update)
- rename_variables with empty rename_map (distinct code path)
"""

from __future__ import annotations

from typing import Any

import pytest
from pycypher.ast_models import (
    ASTNode,
    MapLiteral,
    Match,
    NodePattern,
    Pattern,
    PatternPath,
    Query,
    RelationshipDirection,
    RelationshipPattern,
    Return,
    ReturnItem,
    Variable,
)
from pycypher.exceptions import SecurityError
from pycypher.variable_manager import (
    _MAX_NAME_GENERATION_ATTEMPTS,
    VariableManager,
    _copy_field_value,
    _deep_copy_ast,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _node(var_name: str, label: str | None = None) -> NodePattern:
    """Build a NodePattern with a variable and optional label."""
    return NodePattern(
        variable=Variable(name=var_name),
        labels=[label] if label else [],
    )


def _rel(
    var_name: str | None = None,
    label: str | None = None,
) -> RelationshipPattern:
    """Build a RelationshipPattern."""
    return RelationshipPattern(
        variable=Variable(name=var_name) if var_name else None,
        labels=[label] if label else [],
        direction=RelationshipDirection.RIGHT,
    )


def _path(*elements: NodePattern | RelationshipPattern) -> PatternPath:
    """Build a PatternPath from alternating node/rel elements."""
    return PatternPath(variable=None, elements=list(elements))


def _match(*paths: PatternPath) -> Match:
    """Build a MATCH clause."""
    return Match(pattern=Pattern(paths=list(paths)))


def _return(*names: str) -> Return:
    """Build a RETURN clause."""
    return Return(
        items=[
            ReturnItem(expression=Variable(name=n), alias=None) for n in names
        ],
    )


def _query(*clauses: ASTNode) -> Query:
    """Build a Query from clauses."""
    return Query(clauses=list(clauses))


# ---------------------------------------------------------------------------
# generate_unique_name: SecurityError on exhaustion
# ---------------------------------------------------------------------------


class TestGenerateUniqueNameExhaustion:
    """Test SecurityError when all candidate names are taken."""

    def test_raises_security_error_when_all_names_exhausted(self) -> None:
        """SecurityError raised after _MAX_NAME_GENERATION_ATTEMPTS collisions."""
        manager = VariableManager()
        # Build a set containing the base candidate plus every suffixed variant
        existing: set[str] = {"__vx"}
        for i in range(1, _MAX_NAME_GENERATION_ATTEMPTS + 1):
            existing.add(f"__vx_{i}")

        with pytest.raises(
            SecurityError,
            match="Could not generate a unique variable name",
        ):
            manager.generate_unique_name("x", existing=existing)

    def test_succeeds_just_before_exhaustion(self) -> None:
        """Name generation succeeds when the last slot is still free."""
        manager = VariableManager()
        existing: set[str] = {"__vx"}
        # Fill all but the very last suffixed candidate
        for i in range(1, _MAX_NAME_GENERATION_ATTEMPTS):
            existing.add(f"__vx_{i}")

        result = manager.generate_unique_name("x", existing=existing)
        assert result == f"__vx_{_MAX_NAME_GENERATION_ATTEMPTS}"
        assert result not in existing


# ---------------------------------------------------------------------------
# _deep_copy_ast: SecurityError on excessive nesting depth
# ---------------------------------------------------------------------------


class TestDeepCopyNestingDepthLimit:
    """Test that _deep_copy_ast raises SecurityError on deep nesting."""

    def test_raises_security_error_on_excessive_depth(self) -> None:
        """SecurityError raised when depth exceeds MAX_QUERY_NESTING_DEPTH."""
        from pycypher.config import MAX_QUERY_NESTING_DEPTH

        node = Variable(name="n")
        with pytest.raises(SecurityError, match="maximum nesting depth"):
            _deep_copy_ast(node, {}, depth=MAX_QUERY_NESTING_DEPTH + 1)


# ---------------------------------------------------------------------------
# _copy_field_value: branch coverage
# ---------------------------------------------------------------------------


class TestCopyFieldValueBranches:
    """Test each branch in _copy_field_value."""

    def test_variable_field_renamed(self) -> None:
        """Variable values are renamed via the rename_map."""
        var = Variable(name="n")
        result = _copy_field_value(var, {"n": "renamed_n"}, depth=0)
        assert isinstance(result, Variable)
        assert result.name == "renamed_n"

    def test_variable_field_not_in_map_unchanged(self) -> None:
        """Variable not in rename_map keeps its original name."""
        var = Variable(name="n")
        result = _copy_field_value(var, {"other": "x"}, depth=0)
        assert isinstance(result, Variable)
        assert result.name == "n"

    def test_ast_node_field_recursed(self) -> None:
        """Non-Variable ASTNode fields are recursed into."""
        node = NodePattern(
            variable=Variable(name="a"),
            labels=["Person"],
        )
        result = _copy_field_value(node, {"a": "b"}, depth=0)
        assert isinstance(result, NodePattern)
        vars_found = {v.name for v in result.find_all(Variable)}
        assert "b" in vars_found
        assert "a" not in vars_found

    def test_list_field_recursed(self) -> None:
        """List fields have each element processed."""
        items = [Variable(name="x"), Variable(name="y")]
        result = _copy_field_value(items, {"x": "xx"}, depth=0)
        assert isinstance(result, list)
        assert result[0].name == "xx"
        assert result[1].name == "y"

    def test_tuple_field_recursed(self) -> None:
        """Tuple fields have each element processed and return a tuple."""
        items = (Variable(name="a"), Variable(name="b"))
        result = _copy_field_value(items, {"a": "aa"}, depth=0)
        assert isinstance(result, tuple)
        assert result[0].name == "aa"
        assert result[1].name == "b"

    def test_dict_field_recursed(self) -> None:
        """Dict field values are recursed into."""
        d: dict[str, Any] = {
            "key1": Variable(name="n"),
            "key2": "plain_string",
        }
        result = _copy_field_value(d, {"n": "renamed"}, depth=0)
        assert isinstance(result, dict)
        assert isinstance(result["key1"], Variable)
        assert result["key1"].name == "renamed"
        assert result["key2"] == "plain_string"

    def test_plain_value_returned_as_is(self) -> None:
        """Non-AST, non-collection values are returned unchanged."""
        assert _copy_field_value(42, {}, depth=0) == 42
        assert _copy_field_value("hello", {}, depth=0) == "hello"
        assert _copy_field_value(None, {}, depth=0) is None
        assert _copy_field_value(3.14, {}, depth=0) == 3.14

    def test_empty_list_returns_empty_list(self) -> None:
        """Empty list is handled without error."""
        result = _copy_field_value([], {}, depth=0)
        assert result == []

    def test_empty_tuple_returns_empty_tuple(self) -> None:
        """Empty tuple is handled without error."""
        result = _copy_field_value((), {}, depth=0)
        assert result == ()

    def test_empty_dict_returns_empty_dict(self) -> None:
        """Empty dict is handled without error."""
        result = _copy_field_value({}, {}, depth=0)
        assert result == {}


# ---------------------------------------------------------------------------
# _deep_copy_ast: Variable node handling
# ---------------------------------------------------------------------------


class TestDeepCopyASTVariableNode:
    """Test _deep_copy_ast when node is a Variable."""

    def test_variable_renamed(self) -> None:
        """Variable node is renamed when present in rename_map."""
        var = Variable(name="n")
        result = _deep_copy_ast(var, {"n": "new_n"}, depth=0)
        assert isinstance(result, Variable)
        assert result.name == "new_n"

    def test_variable_not_in_map_keeps_name(self) -> None:
        """Variable not in rename_map retains its name."""
        var = Variable(name="n")
        result = _deep_copy_ast(var, {"other": "x"}, depth=0)
        assert isinstance(result, Variable)
        assert result.name == "n"

    def test_variable_deep_copy_returns_new_instance(self) -> None:
        """Deep copy always returns a new Variable instance."""
        var = Variable(name="n")
        result = _deep_copy_ast(var, {}, depth=0)
        assert isinstance(result, Variable)
        assert result.name == "n"
        assert result is not var


# ---------------------------------------------------------------------------
# _deep_copy_ast: no-field-change path (model_copy without update)
# ---------------------------------------------------------------------------


class TestDeepCopyASTNoFieldChange:
    """Test _deep_copy_ast when no fields need updating."""

    def test_node_with_no_ast_children_copies_without_update(self) -> None:
        """A node whose fields are all plain values triggers model_copy()."""
        # NodePattern with no variable and no labels — all fields are plain
        node = NodePattern(variable=None, labels=[])
        result = _deep_copy_ast(node, {"nonexistent": "x"}, depth=0)
        assert isinstance(result, NodePattern)
        assert result is not node
        assert result.variable is None
        assert result.labels == []


# ---------------------------------------------------------------------------
# rename_variables: empty map vs non-empty map code paths
# ---------------------------------------------------------------------------


class TestRenameVariablesCodePaths:
    """Test both branches in rename_variables (empty vs non-empty map)."""

    def test_empty_rename_map_takes_fast_path(self) -> None:
        """Empty rename_map returns the original node (structural sharing)."""
        manager = VariableManager()
        original = _query(
            _match(_path(_node("n", "Person"))),
            _return("n"),
        )
        result = manager.rename_variables(original, {})
        result_vars = {v.name for v in result.find_all(Variable)}
        assert "n" in result_vars
        # With structural sharing, empty rename returns the same object
        assert result is original

    def test_non_empty_rename_map_applies_renames(self) -> None:
        """Non-empty rename_map applies all renames."""
        manager = VariableManager()
        original = _query(
            _match(_path(_node("n", "Person"))),
            _return("n"),
        )
        result = manager.rename_variables(original, {"n": "renamed"})
        result_vars = {v.name for v in result.find_all(Variable)}
        assert "renamed" in result_vars
        assert "n" not in result_vars


# ---------------------------------------------------------------------------
# MapLiteral dict field coverage via rename_variables
# ---------------------------------------------------------------------------


class TestRenameWithMapLiteral:
    """Test that dict fields on real AST nodes are traversed during rename."""

    def test_map_literal_entries_renamed(self) -> None:
        """Variables inside MapLiteral.entries dict values are renamed."""
        manager = VariableManager()
        map_lit = MapLiteral(
            value={},
            entries={"name": Variable(name="n")},
        )
        result = manager.rename_variables(map_lit, {"n": "renamed_n"})
        assert isinstance(result, MapLiteral)
        entry_var = result.entries["name"]
        assert isinstance(entry_var, Variable)
        assert entry_var.name == "renamed_n"

    def test_map_literal_entries_no_rename_needed(self) -> None:
        """MapLiteral entries not in rename_map are left unchanged."""
        manager = VariableManager()
        map_lit = MapLiteral(
            value={},
            entries={"name": Variable(name="x")},
        )
        result = manager.rename_variables(map_lit, {"other": "y"})
        assert isinstance(result, MapLiteral)
        entry_var = result.entries["name"]
        assert isinstance(entry_var, Variable)
        assert entry_var.name == "x"


# ---------------------------------------------------------------------------
# collect_variables edge cases
# ---------------------------------------------------------------------------


class TestCollectVariablesEdgeCases:
    """Test collect_variables on edge-case ASTs."""

    def test_query_with_no_variables(self) -> None:
        """A query with no variable bindings returns empty set."""
        manager = VariableManager()
        # Anonymous node pattern
        node = NodePattern(variable=None, labels=["Person"])
        match = _match(PatternPath(variable=None, elements=[node]))
        query = _query(match)
        result = manager.collect_variables(query)
        assert result == set()

    def test_duplicate_variable_names_deduplicated(self) -> None:
        """Variables with the same name appear only once in the set."""
        manager = VariableManager()
        query = _query(
            _match(_path(_node("n", "Person"))),
            _return("n"),
        )
        result = manager.collect_variables(query)
        assert result == {"n"}


# ---------------------------------------------------------------------------
# detect_conflicts: no-conflict branch (no logging)
# ---------------------------------------------------------------------------


class TestDetectConflictsNonOverlapping:
    """Test detect_conflicts when there is zero overlap."""

    def test_both_empty_queries(self) -> None:
        """Two empty queries have no conflicts."""
        manager = VariableManager()
        q1 = _query()
        q2 = _query()
        assert manager.detect_conflicts(q1, q2) == set()

    def test_one_empty_one_populated(self) -> None:
        """An empty query cannot conflict with a populated one."""
        manager = VariableManager()
        q1 = _query()
        q2 = _query(
            _match(_path(_node("n"))),
            _return("n"),
        )
        assert manager.detect_conflicts(q1, q2) == set()
