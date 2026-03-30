"""TDD RED Phase — Variable Namespace Management interface contracts.

Tests define the protocol for:
- Variable conflict detection between queries/subqueries
- Systematic variable renaming with configurable prefixes
- AST node rewriting with new variable names
- Variable binding preservation across query boundaries

Everything that can go wrong with variable conflicts, will go wrong.
These tests encode the edge cases that catch subtle namespace collisions.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pycypher.ast_models import (
    ASTNode,
    Match,
    NodePattern,
    Pattern,
    PatternPath,
    PropertyLookup,
    Query,
    RelationshipDirection,
    RelationshipPattern,
    Return,
    ReturnItem,
    Variable,
    With,
)

# ---------------------------------------------------------------------------
# Helper: build minimal AST fragments for testing
# ---------------------------------------------------------------------------


def _make_node(var_name: str, label: str | None = None) -> NodePattern:
    """Build a NodePattern with a variable and optional label."""
    return NodePattern(
        variable=Variable(name=var_name),
        labels=[label] if label else [],
    )


def _make_rel(
    var_name: str | None = None,
    label: str | None = None,
) -> RelationshipPattern:
    """Build a RelationshipPattern."""
    return RelationshipPattern(
        variable=Variable(name=var_name) if var_name else None,
        labels=[label] if label else [],
        direction=RelationshipDirection.RIGHT,
    )


def _make_match(*paths: PatternPath) -> Match:
    """Build a MATCH clause from PatternPath instances."""
    return Match(pattern=Pattern(paths=list(paths)))


def _make_path(
    *elements: NodePattern | RelationshipPattern,
    var_name: str | None = None,
) -> PatternPath:
    """Build a PatternPath from alternating node/rel elements."""
    return PatternPath(
        variable=Variable(name=var_name) if var_name else None,
        elements=list(elements),
    )


def _make_return(*names: str) -> Return:
    """Build a RETURN clause returning named variables."""
    return Return(
        items=[ReturnItem(expression=Variable(name=n), alias=None) for n in names],
    )


def _make_with(*names: str) -> With:
    """Build a WITH clause projecting named variables."""
    return With(
        items=[ReturnItem(expression=Variable(name=n), alias=None) for n in names],
    )


def _make_query(*clauses: ASTNode) -> Query:
    """Build a Query from clauses."""
    return Query(clauses=list(clauses))


# ---------------------------------------------------------------------------
# 1. Interface Contract — Protocol definition
# ---------------------------------------------------------------------------


@runtime_checkable
class VariableManagerProtocol(Protocol):
    """Protocol that VariableManager must satisfy."""

    def collect_variables(self, node: ASTNode) -> set[str]:
        """Collect all variable names defined in the AST subtree."""
        ...

    def detect_conflicts(
        self,
        query_a: ASTNode,
        query_b: ASTNode,
    ) -> set[str]:
        """Return variable names that appear in both queries."""
        ...

    def generate_unique_name(
        self,
        base_name: str,
        existing: set[str],
        prefix: str = "__v",
    ) -> str:
        """Generate a unique variable name avoiding collisions."""
        ...

    def rename_variables(
        self,
        node: ASTNode,
        rename_map: dict[str, str],
    ) -> ASTNode:
        """Return a new AST with variables renamed per the mapping."""
        ...


class TestVariableManagerInterfaceContract:
    """Verify VariableManager satisfies the protocol."""

    def test_variable_manager_is_protocol_compliant(self) -> None:
        """VariableManager must implement VariableManagerProtocol."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        assert isinstance(manager, VariableManagerProtocol)

    def test_variable_manager_has_collect_variables(self) -> None:
        """collect_variables must accept an ASTNode and return set[str]."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        assert callable(getattr(manager, "collect_variables", None))

    def test_variable_manager_has_detect_conflicts(self) -> None:
        """detect_conflicts must accept two ASTNodes and return set[str]."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        assert callable(getattr(manager, "detect_conflicts", None))

    def test_variable_manager_has_generate_unique_name(self) -> None:
        """generate_unique_name must return a non-colliding string."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        assert callable(getattr(manager, "generate_unique_name", None))

    def test_variable_manager_has_rename_variables(self) -> None:
        """rename_variables must accept AST + mapping and return new AST."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        assert callable(getattr(manager, "rename_variables", None))


# ---------------------------------------------------------------------------
# 2. Variable Conflict Detection
# ---------------------------------------------------------------------------


class TestDetectVariableConflicts:
    """Test conflict detection between queries sharing a namespace."""

    def test_no_conflicts_disjoint_variables(self) -> None:
        """Queries with entirely different variables have no conflicts."""
        from pycypher.variable_manager import VariableManager

        q1 = _make_query(
            _make_match(_make_path(_make_node("a", "Person"))),
            _make_return("a"),
        )
        q2 = _make_query(
            _make_match(_make_path(_make_node("b", "Company"))),
            _make_return("b"),
        )
        manager = VariableManager()
        conflicts = manager.detect_conflicts(q1, q2)
        assert conflicts == set()

    def test_detects_overlapping_node_variables(self) -> None:
        """Same variable name in both queries is a conflict."""
        from pycypher.variable_manager import VariableManager

        q1 = _make_query(
            _make_match(_make_path(_make_node("n", "Person"))),
            _make_return("n"),
        )
        q2 = _make_query(
            _make_match(_make_path(_make_node("n", "Company"))),
            _make_return("n"),
        )
        manager = VariableManager()
        conflicts = manager.detect_conflicts(q1, q2)
        assert conflicts == {"n"}

    def test_detects_multiple_conflicts(self) -> None:
        """Multiple overlapping variables are all reported."""
        from pycypher.variable_manager import VariableManager

        q1 = _make_query(
            _make_match(
                _make_path(
                    _make_node("a", "Person"),
                    _make_rel("r", "KNOWS"),
                    _make_node("b", "Person"),
                ),
            ),
            _make_return("a", "b"),
        )
        q2 = _make_query(
            _make_match(
                _make_path(
                    _make_node("a", "Company"),
                    _make_rel("r", "OWNS"),
                    _make_node("c", "Asset"),
                ),
            ),
            _make_return("a", "c"),
        )
        manager = VariableManager()
        conflicts = manager.detect_conflicts(q1, q2)
        assert conflicts == {"a", "r"}

    def test_detects_path_variable_conflicts(self) -> None:
        """Path-level variable bindings are included in conflict detection."""
        from pycypher.variable_manager import VariableManager

        q1 = _make_query(
            _make_match(
                _make_path(
                    _make_node("a"),
                    _make_rel(None, "KNOWS"),
                    _make_node("b"),
                    var_name="p",
                ),
            ),
            _make_return("p"),
        )
        q2 = _make_query(
            _make_match(
                _make_path(
                    _make_node("x"),
                    _make_rel(None, "LIKES"),
                    _make_node("p"),  # "p" conflicts with path var
                ),
            ),
            _make_return("p"),
        )
        manager = VariableManager()
        conflicts = manager.detect_conflicts(q1, q2)
        assert "p" in conflicts

    def test_empty_query_has_no_conflicts(self) -> None:
        """An empty query cannot conflict with anything."""
        from pycypher.variable_manager import VariableManager

        q1 = _make_query()
        q2 = _make_query(
            _make_match(_make_path(_make_node("n"))),
            _make_return("n"),
        )
        manager = VariableManager()
        assert manager.detect_conflicts(q1, q2) == set()


# ---------------------------------------------------------------------------
# 3. Unique Variable Name Generation
# ---------------------------------------------------------------------------


class TestGenerateUniqueVariableNames:
    """Test the naming strategy for conflict-free variable names."""

    def test_no_collision_returns_prefixed_base(self) -> None:
        """When no collision, returns prefix + base_name."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        result = manager.generate_unique_name("n", existing=set())
        assert result == "__vn"

    def test_collision_appends_counter(self) -> None:
        """When prefixed name collides, a numeric suffix is appended."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        existing = {"__vn"}
        result = manager.generate_unique_name("n", existing=existing)
        assert result not in existing
        assert result.startswith("__vn")

    def test_custom_prefix(self) -> None:
        """Custom prefix is respected."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        result = manager.generate_unique_name(
            "x",
            existing=set(),
            prefix="sub_",
        )
        assert result == "sub_x"

    def test_multiple_collisions_resolved(self) -> None:
        """Even with many existing names, a unique name is found."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        existing = {"__vn", "__vn_1", "__vn_2", "__vn_3"}
        result = manager.generate_unique_name("n", existing=existing)
        assert result not in existing

    def test_empty_base_name_still_works(self) -> None:
        """Edge case: empty string base name produces a valid name."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        result = manager.generate_unique_name("", existing=set())
        assert isinstance(result, str)
        assert len(result) > 0

    def test_generated_name_is_valid_identifier(self) -> None:
        """Generated names must be valid Python/Cypher identifiers."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        # Even with weird base names, output should be identifier-safe
        result = manager.generate_unique_name("n", existing=set())
        assert result.isidentifier()


# ---------------------------------------------------------------------------
# 4. AST Rewriting with New Variable Names
# ---------------------------------------------------------------------------


class TestRewriteASTVariables:
    """Test AST node rewriting with variable rename mappings."""

    def test_rename_node_pattern_variable(self) -> None:
        """NodePattern variables are renamed in the new AST."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        original = _make_match(_make_path(_make_node("n", "Person")))
        rewritten = manager.rename_variables(original, {"n": "__vn"})

        # The rewritten AST should contain __vn, not n
        all_vars = {v.name for v in rewritten.find_all(Variable)}
        assert "__vn" in all_vars
        assert "n" not in all_vars

    def test_rename_relationship_variable(self) -> None:
        """RelationshipPattern variables are renamed."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        original = _make_match(
            _make_path(
                _make_node("a"),
                _make_rel("r", "KNOWS"),
                _make_node("b"),
            ),
        )
        rewritten = manager.rename_variables(original, {"r": "__vr"})
        all_vars = {v.name for v in rewritten.find_all(Variable)}
        assert "__vr" in all_vars
        assert "r" not in all_vars

    def test_rename_preserves_unrenamed_variables(self) -> None:
        """Variables not in the rename map are left unchanged."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        original = _make_match(
            _make_path(
                _make_node("a", "Person"),
                _make_rel("r", "KNOWS"),
                _make_node("b", "Person"),
            ),
        )
        rewritten = manager.rename_variables(original, {"a": "__va"})
        all_vars = {v.name for v in rewritten.find_all(Variable)}
        assert "__va" in all_vars
        assert "a" not in all_vars
        assert "r" in all_vars  # unchanged
        assert "b" in all_vars  # unchanged

    def test_rename_returns_new_ast_not_mutated_original(self) -> None:
        """Renaming must return a new tree — the original is immutable."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        original = _make_match(_make_path(_make_node("n", "Person")))
        rewritten = manager.rename_variables(original, {"n": "__vn"})

        # Original must be untouched
        original_vars = {v.name for v in original.find_all(Variable)}
        assert "n" in original_vars
        assert "__vn" not in original_vars

        # Rewritten must have the new name
        rewritten_vars = {v.name for v in rewritten.find_all(Variable)}
        assert "__vn" in rewritten_vars

    def test_rename_with_empty_map_returns_equivalent_ast(self) -> None:
        """Empty rename map returns an AST structurally equal to original."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        original = _make_match(_make_path(_make_node("n", "Person")))
        rewritten = manager.rename_variables(original, {})
        rewritten_vars = {v.name for v in rewritten.find_all(Variable)}
        assert "n" in rewritten_vars

    def test_rename_propagates_to_return_clause(self) -> None:
        """Variable references in RETURN are also renamed."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        original = _make_query(
            _make_match(_make_path(_make_node("n", "Person"))),
            _make_return("n"),
        )
        rewritten = manager.rename_variables(original, {"n": "__vn"})
        all_vars = {v.name for v in rewritten.find_all(Variable)}
        assert "__vn" in all_vars
        assert "n" not in all_vars

    def test_rename_propagates_to_with_clause(self) -> None:
        """Variable references in WITH are also renamed."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        original = _make_query(
            _make_match(_make_path(_make_node("n", "Person"))),
            _make_with("n"),
            _make_return("n"),
        )
        rewritten = manager.rename_variables(original, {"n": "__vn"})
        all_vars = {v.name for v in rewritten.find_all(Variable)}
        assert "__vn" in all_vars
        assert "n" not in all_vars


# ---------------------------------------------------------------------------
# 5. Variable Binding Preservation
# ---------------------------------------------------------------------------


class TestPreserveVariableBindings:
    """Test that renaming preserves structural binding relationships."""

    def test_property_lookup_follows_renamed_variable(self) -> None:
        """PropertyLookup referencing a renamed variable updates correctly."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        prop = PropertyLookup(
            expression=Variable(name="n"),
            property_name="name",
        )
        original = _make_query(
            _make_match(_make_path(_make_node("n", "Person"))),
            Return(
                items=[ReturnItem(expression=prop, alias=None)],
            ),
        )
        rewritten = manager.rename_variables(original, {"n": "__vn"})
        all_vars = {v.name for v in rewritten.find_all(Variable)}
        assert "__vn" in all_vars
        assert "n" not in all_vars

    def test_collect_variables_from_match_pattern(self) -> None:
        """collect_variables finds all defined variables in MATCH patterns."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        query = _make_query(
            _make_match(
                _make_path(
                    _make_node("a", "Person"),
                    _make_rel("r", "KNOWS"),
                    _make_node("b", "Person"),
                ),
            ),
            _make_return("a", "b"),
        )
        variables = manager.collect_variables(query)
        assert variables == {"a", "r", "b"}

    def test_collect_variables_includes_path_variable(self) -> None:
        """Path-level variable bindings are collected."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        query = _make_query(
            _make_match(
                _make_path(
                    _make_node("a"),
                    _make_rel(None, "KNOWS"),
                    _make_node("b"),
                    var_name="p",
                ),
            ),
            _make_return("p"),
        )
        variables = manager.collect_variables(query)
        assert "p" in variables
        assert "a" in variables
        assert "b" in variables

    def test_rename_path_variable_preserved(self) -> None:
        """Renaming a path-level variable updates the PatternPath binding."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        original = _make_query(
            _make_match(
                _make_path(
                    _make_node("a"),
                    _make_rel(None, "KNOWS"),
                    _make_node("b"),
                    var_name="p",
                ),
            ),
            _make_return("p"),
        )
        rewritten = manager.rename_variables(original, {"p": "__vp"})
        all_vars = {v.name for v in rewritten.find_all(Variable)}
        assert "__vp" in all_vars
        assert "p" not in all_vars

    def test_multiple_renames_applied_atomically(self) -> None:
        """All renames in the map are applied in a single pass — no cascading."""
        from pycypher.variable_manager import VariableManager

        manager = VariableManager()
        original = _make_query(
            _make_match(
                _make_path(
                    _make_node("a", "Person"),
                    _make_rel("r", "KNOWS"),
                    _make_node("b", "Person"),
                ),
            ),
            _make_return("a", "b"),
        )
        rewritten = manager.rename_variables(
            original,
            {"a": "__va", "b": "__vb", "r": "__vr"},
        )
        all_vars = {v.name for v in rewritten.find_all(Variable)}
        assert all_vars == {"__va", "__vb", "__vr"}
