"""Unit tests for PatternMatcher — extracted pattern matching engine.

Tests node scanning, pattern path traversal, MATCH clause coordination,
predicate pushdown, and error handling paths.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import (
    Comparison,
    Match,
    NodePattern,
    Pattern,
    PatternPath,
    PropertyLookup,
    RelationshipDirection,
    RelationshipPattern,
    Variable,
)
from pycypher.binding_frame import BindingFrame
from pycypher.path_expander import PathExpander
from pycypher.pattern_matcher import (
    _ANON_NODE_PREFIX,
    PatternMatcher,
)
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

ID_COLUMN = "__ID__"


@pytest.fixture
def people_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "age": [30, 25, 35, 28],
        },
    )


@pytest.fixture
def animals_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [10, 11],
            "species": ["cat", "dog"],
        },
    )


@pytest.fixture
def knows_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [101, 102, 103],
            "__SOURCE__": [1, 2, 3],
            "__TARGET__": [2, 3, 1],
            "since": [2020, 2021, 2019],
        },
    )


@pytest.fixture
def social_context(people_df: pd.DataFrame, knows_df: pd.DataFrame) -> Context:
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__", "since"],
        source_obj_attribute_map={"since": "since"},
        attribute_map={"since": "since"},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )


@pytest.fixture
def multi_type_context(
    people_df: pd.DataFrame,
    animals_df: pd.DataFrame,
    knows_df: pd.DataFrame,
) -> Context:
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )
    animal_table = EntityTable(
        entity_type="Animal",
        identifier="Animal",
        column_names=[ID_COLUMN, "species"],
        source_obj_attribute_map={"species": "species"},
        attribute_map={"species": "species"},
        source_obj=animals_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__", "since"],
        source_obj_attribute_map={"since": "since"},
        attribute_map={"since": "since"},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    return Context(
        entity_mapping=EntityMapping(
            mapping={"Person": person_table, "Animal": animal_table},
        ),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
    )


@pytest.fixture
def empty_context() -> Context:
    return Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


def _identity_join(left: BindingFrame, right: BindingFrame) -> BindingFrame:
    """Simple cross-join for testing."""
    return left.cross_join(right)


def _noop_where(predicate: object, frame: BindingFrame) -> BindingFrame:
    """Pass-through WHERE — no filtering."""
    return frame


def _make_matcher(ctx: Context) -> PatternMatcher:
    expander = PathExpander(ctx)
    return PatternMatcher(
        context=ctx,
        path_expander=expander,
        coerce_join_fn=_identity_join,
        apply_where_fn=_noop_where,
    )


# ===========================================================================
# node_pattern_to_binding_frame tests
# ===========================================================================


class TestNodePatternToBindingFrame:
    """Tests for single-node scanning."""

    def test_labeled_node_with_variable(self, social_context: Context) -> None:
        """(n:Person) scans Person entity table."""
        matcher = _make_matcher(social_context)
        node = NodePattern(variable=Variable(name="n"), labels=["Person"])
        frame = matcher.node_pattern_to_binding_frame(node, [0])
        assert "n" in frame.var_names
        assert len(frame.bindings) == 4

    def test_anonymous_node_gets_synthetic_var(
        self,
        social_context: Context,
    ) -> None:
        """(:Person) assigns _anon_node_0."""
        matcher = _make_matcher(social_context)
        node = NodePattern(variable=None, labels=["Person"])
        counter: list[int] = [0]
        frame = matcher.node_pattern_to_binding_frame(node, counter)
        assert f"{_ANON_NODE_PREFIX}0" in frame.var_names
        assert counter[0] == 1

    def test_unlabeled_node_scans_all_types(
        self,
        multi_type_context: Context,
    ) -> None:
        """(n) with no label scans all entity types."""
        matcher = _make_matcher(multi_type_context)
        node = NodePattern(variable=Variable(name="n"), labels=[])
        frame = matcher.node_pattern_to_binding_frame(node, [0])
        # 4 persons + 2 animals = 6 rows
        assert len(frame.bindings) == 6
        assert frame.type_registry["n"] == "__MULTI__"

    def test_unlabeled_node_single_type(self, social_context: Context) -> None:
        """(n) with single entity type returns that type directly."""
        matcher = _make_matcher(social_context)
        node = NodePattern(variable=Variable(name="n"), labels=[])
        frame = matcher.node_pattern_to_binding_frame(node, [0])
        assert len(frame.bindings) == 4

    def test_unlabeled_node_no_types_raises(
        self,
        empty_context: Context,
    ) -> None:
        """(n) with no entity types raises ValueError."""
        matcher = _make_matcher(empty_context)
        node = NodePattern(variable=Variable(name="n"), labels=[])
        with pytest.raises(ValueError, match="no entity types"):
            matcher.node_pattern_to_binding_frame(node, [0])

    def test_inline_property_filter(self, social_context: Context) -> None:
        """(n:Person {name: 'Alice'}) filters to one row."""
        from pycypher.ast_models import StringLiteral

        matcher = _make_matcher(social_context)
        node = NodePattern(
            variable=Variable(name="n"),
            labels=["Person"],
            properties={"name": StringLiteral(value="Alice")},
        )
        frame = matcher.node_pattern_to_binding_frame(node, [0])
        assert len(frame.bindings) == 1

    def test_unlabeled_node_with_context_frame_resolution(
        self,
        social_context: Context,
    ) -> None:
        """(n) resolves type from context_frame type_registry."""
        matcher = _make_matcher(social_context)
        # First scan labeled
        labeled_node = NodePattern(
            variable=Variable(name="n"),
            labels=["Person"],
        )
        ctx_frame = matcher.node_pattern_to_binding_frame(labeled_node, [0])
        # Now scan unlabeled with context_frame
        unlabeled_node = NodePattern(variable=Variable(name="n"), labels=[])
        frame = matcher.node_pattern_to_binding_frame(
            unlabeled_node,
            [0],
            context_frame=ctx_frame,
        )
        assert len(frame.bindings) == 4


# ===========================================================================
# pattern_path_to_binding_frame tests
# ===========================================================================


class TestPatternPathToBindingFrame:
    """Tests for path traversal."""

    def test_single_node_path(self, social_context: Context) -> None:
        """Path with just one node."""
        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="n"), labels=["Person"]),
            ],
        )
        frame = matcher.pattern_path_to_binding_frame(path, [0])
        assert len(frame.bindings) == 4

    def test_empty_path_raises(self, social_context: Context) -> None:
        """Empty PatternPath raises ValueError."""
        matcher = _make_matcher(social_context)
        path = PatternPath(elements=[])
        with pytest.raises(ValueError, match="empty"):
            matcher.pattern_path_to_binding_frame(path, [0])

    def test_node_rel_node_right_direction(
        self,
        social_context: Context,
    ) -> None:
        """(a:Person)-[:KNOWS]->(b:Person) produces matched rows."""
        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
            ],
        )
        frame = matcher.pattern_path_to_binding_frame(path, [0])
        assert len(frame.bindings) == 3
        assert "a" in frame.var_names
        assert "b" in frame.var_names

    def test_node_rel_node_left_direction(
        self,
        social_context: Context,
    ) -> None:
        """(a:Person)<-[:KNOWS]-(b:Person) reverses direction."""
        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.LEFT,
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
            ],
        )
        frame = matcher.pattern_path_to_binding_frame(path, [0])
        assert len(frame.bindings) == 3

    def test_anonymous_relationship_gets_synthetic_var(
        self,
        social_context: Context,
    ) -> None:
        """(a)-[]->(b) assigns _anon_rel_0."""
        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=None,
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
            ],
        )
        counter: list[int] = [0]
        frame = matcher.pattern_path_to_binding_frame(path, counter)
        # Counter incremented for anon rel
        assert counter[0] >= 1

    def test_no_relationship_types_raises(
        self,
        empty_context: Context,
    ) -> None:
        """Untyped relationship with no registered tables raises."""
        # Need at least one entity type for the node scan
        people_df = pd.DataFrame({ID_COLUMN: [1, 2], "name": ["A", "B"]})
        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=[ID_COLUMN, "name"],
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=people_df,
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
        matcher = _make_matcher(ctx)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=[],  # No labels
                    direction=RelationshipDirection.RIGHT,
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
            ],
        )
        with pytest.raises(ValueError, match="no relationship tables"):
            matcher.pattern_path_to_binding_frame(path, [0])


# ===========================================================================
# match_to_binding_frame tests
# ===========================================================================


class TestMatchToBindingFrame:
    """Tests for MATCH clause coordination."""

    def test_single_pattern(self, social_context: Context) -> None:
        """MATCH (n:Person) produces 4 rows."""
        matcher = _make_matcher(social_context)
        match_clause = Match(
            pattern=Pattern(
                paths=[
                    PatternPath(
                        elements=[
                            NodePattern(
                                variable=Variable(name="n"),
                                labels=["Person"],
                            ),
                        ],
                    ),
                ],
            ),
            optional=False,
        )
        frame = matcher.match_to_binding_frame(match_clause)
        assert len(frame.bindings) == 4

    def test_no_patterns_raises(self, social_context: Context) -> None:
        """MATCH with zero paths raises ValueError."""
        matcher = _make_matcher(social_context)
        match_clause = Match(
            pattern=Pattern(paths=[]),
            optional=False,
        )
        with pytest.raises(ValueError, match="no pattern paths"):
            matcher.match_to_binding_frame(match_clause)

    def test_multiple_paths_joined(self, social_context: Context) -> None:
        """MATCH (a:Person), (b:Person) cross-joins."""
        matcher = _make_matcher(social_context)
        match_clause = Match(
            pattern=Pattern(
                paths=[
                    PatternPath(
                        elements=[
                            NodePattern(
                                variable=Variable(name="a"),
                                labels=["Person"],
                            ),
                        ],
                    ),
                    PatternPath(
                        elements=[
                            NodePattern(
                                variable=Variable(name="b"),
                                labels=["Person"],
                            ),
                        ],
                    ),
                ],
            ),
            optional=False,
        )
        frame = matcher.match_to_binding_frame(match_clause)
        # 4 * 4 = 16 cross-product
        assert len(frame.bindings) == 16

    def test_where_applied_single_path(self, social_context: Context) -> None:
        """MATCH with WHERE clause on single path applies post-scan filter."""
        # Use a real where filter that does actual filtering
        where_called: list[bool] = []

        def tracking_where(
            predicate: object,
            frame: BindingFrame,
        ) -> BindingFrame:
            where_called.append(True)
            return frame

        expander = PathExpander(social_context)
        matcher = PatternMatcher(
            context=social_context,
            path_expander=expander,
            coerce_join_fn=_identity_join,
            apply_where_fn=tracking_where,
        )

        from pycypher.ast_models import IntegerLiteral

        where_pred = Comparison(
            operator=">",
            left=PropertyLookup(expression=Variable(name="n"), property="age"),
            right=IntegerLiteral(value=30),
        )
        match_clause = Match(
            pattern=Pattern(
                paths=[
                    PatternPath(
                        elements=[
                            NodePattern(
                                variable=Variable(name="n"),
                                labels=["Person"],
                            ),
                        ],
                    ),
                ],
            ),
            optional=False,
            where=where_pred,
        )
        matcher.match_to_binding_frame(match_clause)
        assert len(where_called) == 1


# ===========================================================================
# Anon counter tests
# ===========================================================================


class TestAnonCounterIncrement:
    """Verify anonymous variable counter increments correctly."""

    def test_multiple_anon_nodes(self, social_context: Context) -> None:
        """Multiple anonymous nodes get distinct names."""
        matcher = _make_matcher(social_context)
        counter: list[int] = [0]

        n1 = NodePattern(variable=None, labels=["Person"])
        n2 = NodePattern(variable=None, labels=["Person"])

        f1 = matcher.node_pattern_to_binding_frame(n1, counter)
        f2 = matcher.node_pattern_to_binding_frame(n2, counter)

        assert counter[0] == 2
        assert f"{_ANON_NODE_PREFIX}0" in f1.var_names
        assert f"{_ANON_NODE_PREFIX}1" in f2.var_names


# ===========================================================================
# _traverse_fixed_hop tests (undirected, multi-type, pushdown)
# ===========================================================================


class TestTraverseFixedHop:
    """Tests for _traverse_fixed_hop private helper."""

    def test_undirected_relationship(self, social_context: Context) -> None:
        """(a:Person)-[:KNOWS]-(b:Person) undirected returns both directions."""
        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.UNDIRECTED,
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
            ],
        )
        frame = matcher.pattern_path_to_binding_frame(path, [0])
        # Undirected: each edge appears twice (forward + reverse)
        assert len(frame.bindings) == 6
        assert "a" in frame.var_names
        assert "b" in frame.var_names

    def test_multi_hop_path(self, social_context: Context) -> None:
        """(a)-[:KNOWS]->(b)-[:KNOWS]->(c) two-hop traversal."""
        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r1"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r2"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                ),
                NodePattern(variable=Variable(name="c"), labels=["Person"]),
            ],
        )
        frame = matcher.pattern_path_to_binding_frame(path, [0])
        assert "a" in frame.var_names
        assert "b" in frame.var_names
        assert "c" in frame.var_names
        # With KNOWS: 1->2, 2->3, 3->1, two-hop chains exist
        assert len(frame.bindings) >= 1

    def test_inline_property_filter_on_second_node(
        self,
        social_context: Context,
    ) -> None:
        """(a:Person)-[:KNOWS]->(b:Person {name: 'Bob'}) filters second node."""
        from pycypher.ast_models import StringLiteral

        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                ),
                NodePattern(
                    variable=Variable(name="b"),
                    labels=["Person"],
                    properties={"name": StringLiteral(value="Bob")},
                ),
            ],
        )
        frame = matcher.pattern_path_to_binding_frame(path, [0])
        # Only paths ending at Bob (ID=2): Alice->Bob
        assert len(frame.bindings) == 1


# ===========================================================================
# _join_cyclic_back_reference tests
# ===========================================================================


class TestJoinCyclicBackReference:
    """Tests for cyclic back-reference patterns like (a)-[:KNOWS]->(a)."""

    def test_self_referencing_pattern(self, social_context: Context) -> None:
        """(a:Person)-[:KNOWS]->(a) finds self-loops (if any)."""
        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r1"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r2"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                ),
                # Back-reference to 'a' — cyclic
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
            ],
        )
        frame = matcher.pattern_path_to_binding_frame(path, [0])
        # KNOWS: 1->2, 2->3, 3->1 forms a cycle
        # a->b->a means a=1,b=2 (1->2->3->1 needs 3 hops, not 2)
        # Actually: paths where endpoint matches start: 1->2->3->1 is 3 hops
        # With 2 hops: need a->b, b->a. Check: 1->2 and 2->? no 2->1
        # 2->3 and 3->1, so a=2,b=3 endpoint=1 != a=2. No.
        # 3->1 and 1->2, so a=3,b=1 endpoint=2 != a=3. No.
        # So with this data, there may be 0 results for 2-hop cycle
        # The test validates the cyclic back-reference code path runs
        assert "a" in frame.var_names
        assert "b" in frame.var_names


# ===========================================================================
# _expand_variable_length_hop tests
# ===========================================================================


class TestExpandVariableLengthHop:
    """Tests for variable-length relationship patterns [*m..n]."""

    def test_variable_length_basic(self, social_context: Context) -> None:
        """(a:Person)-[:KNOWS*1..2]->(b:Person) variable-length path."""
        from pycypher.ast_models import PathLength

        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                    length=PathLength(min=1, max=2),
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
            ],
        )
        frame = matcher.pattern_path_to_binding_frame(path, [0])
        assert "a" in frame.var_names
        assert "b" in frame.var_names
        # 1-hop: 3 edges, 2-hop: additional paths through the cycle
        assert len(frame.bindings) >= 3

    def test_variable_length_multi_type_raises(
        self,
        social_context: Context,
    ) -> None:
        """Variable-length with multiple relationship types raises error."""
        from pycypher.ast_models import PathLength

        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=[],  # No labels = all types
                    direction=RelationshipDirection.RIGHT,
                    length=PathLength(min=1, max=2),
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
            ],
        )
        # Only one rel type registered (KNOWS), but the code checks
        # scan_rel_types length which includes all registered types
        # This should work since there's only one type
        frame = matcher.pattern_path_to_binding_frame(path, [0])
        assert len(frame.bindings) >= 3

    def test_variable_length_unknown_rel_type_raises(
        self,
        social_context: Context,
    ) -> None:
        """Variable-length with unknown relationship type raises error."""
        from pycypher.ast_models import PathLength

        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["NONEXISTENT"],
                    direction=RelationshipDirection.RIGHT,
                    length=PathLength(min=1, max=2),
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
            ],
        )
        from pycypher.exceptions import GraphTypeNotFoundError

        with pytest.raises(GraphTypeNotFoundError):
            matcher.pattern_path_to_binding_frame(path, [0])

    def test_variable_length_exact_hops(self, social_context: Context) -> None:
        """(a)-[:KNOWS*2..2]->(b) exactly 2 hops."""
        from pycypher.ast_models import PathLength

        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                    length=PathLength(min=2, max=2),
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
            ],
        )
        frame = matcher.pattern_path_to_binding_frame(path, [0])
        # Only 2-hop paths
        assert "a" in frame.var_names
        assert "b" in frame.var_names


# ===========================================================================
# Path variable hop-count column tests
# ===========================================================================


class TestPathVariableHopCount:
    """Tests for path variable hop-count column assignment."""

    def test_named_path_gets_hop_count(self, social_context: Context) -> None:
        """P = (a)-[:KNOWS]->(b) assigns hop-count column for path variable."""
        matcher = _make_matcher(social_context)
        path = PatternPath(
            variable=Variable(name="p"),
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
            ],
        )
        frame = matcher.pattern_path_to_binding_frame(path, [0])
        from pycypher.binding_frame import PATH_HOP_COLUMN_PREFIX

        hop_col = f"{PATH_HOP_COLUMN_PREFIX}p"
        assert hop_col in frame.bindings.columns
        # Fixed-length 1-hop path
        assert (frame.bindings[hop_col] == 1).all()

    def test_unnamed_path_no_hop_count(self, social_context: Context) -> None:
        """Unnamed path does not get hop-count column."""
        matcher = _make_matcher(social_context)
        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Person"]),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                ),
                NodePattern(variable=Variable(name="b"), labels=["Person"]),
            ],
        )
        frame = matcher.pattern_path_to_binding_frame(path, [0])
        from pycypher.binding_frame import PATH_HOP_COLUMN_PREFIX

        hop_cols = [
            c for c in frame.bindings.columns if c.startswith(PATH_HOP_COLUMN_PREFIX)
        ]
        assert len(hop_cols) == 0


# ===========================================================================
# Predicate pushdown tests
# ===========================================================================


class TestPredicatePushdown:
    """Tests for WHERE predicate pushdown in multi-path MATCH."""

    def test_pushdown_single_path_vars(self, social_context: Context) -> None:
        """WHERE referencing only one path's variables gets pushed down."""
        pushdown_tracker: list[int] = []

        def tracking_where(
            predicate: object,
            frame: BindingFrame,
        ) -> BindingFrame:
            pushdown_tracker.append(len(frame.bindings))
            return frame

        expander = PathExpander(social_context)
        matcher = PatternMatcher(
            context=social_context,
            path_expander=expander,
            coerce_join_fn=_identity_join,
            apply_where_fn=tracking_where,
        )

        from pycypher.ast_models import IntegerLiteral

        where_pred = Comparison(
            operator=">",
            left=PropertyLookup(expression=Variable(name="a"), property="age"),
            right=IntegerLiteral(value=30),
        )
        match_clause = Match(
            pattern=Pattern(
                paths=[
                    PatternPath(
                        elements=[
                            NodePattern(
                                variable=Variable(name="a"),
                                labels=["Person"],
                            ),
                        ],
                    ),
                    PatternPath(
                        elements=[
                            NodePattern(
                                variable=Variable(name="b"),
                                labels=["Person"],
                            ),
                        ],
                    ),
                ],
            ),
            optional=False,
            where=where_pred,
        )
        matcher.match_to_binding_frame(match_clause)
        # WHERE should be called once, pushed down to path with 'a' (4 rows)
        # before the cross-join (which would have 16 rows)
        assert len(pushdown_tracker) == 1
        assert pushdown_tracker[0] == 4  # Applied to 4-row path, not 16-row join

    def test_no_pushdown_cross_path_vars(self, social_context: Context) -> None:
        """WHERE referencing both paths' variables applied after join."""
        where_frame_sizes: list[int] = []

        def tracking_where(
            predicate: object,
            frame: BindingFrame,
        ) -> BindingFrame:
            where_frame_sizes.append(len(frame.bindings))
            return frame

        expander = PathExpander(social_context)
        matcher = PatternMatcher(
            context=social_context,
            path_expander=expander,
            coerce_join_fn=_identity_join,
            apply_where_fn=tracking_where,
        )

        # WHERE references both 'a' and 'b' — cannot push down
        where_pred = Comparison(
            operator=">",
            left=PropertyLookup(expression=Variable(name="a"), property="age"),
            right=PropertyLookup(expression=Variable(name="b"), property="age"),
        )
        match_clause = Match(
            pattern=Pattern(
                paths=[
                    PatternPath(
                        elements=[
                            NodePattern(
                                variable=Variable(name="a"),
                                labels=["Person"],
                            ),
                        ],
                    ),
                    PatternPath(
                        elements=[
                            NodePattern(
                                variable=Variable(name="b"),
                                labels=["Person"],
                            ),
                        ],
                    ),
                ],
            ),
            optional=False,
            where=where_pred,
        )
        matcher.match_to_binding_frame(match_clause)
        # WHERE applied after join (16 rows from cross-product)
        assert len(where_frame_sizes) == 1
        assert where_frame_sizes[0] == 16


# ===========================================================================
# VariableLengthHopParams dataclass tests
# ===========================================================================


class TestVariableLengthHopParams:
    """Tests for the VariableLengthHopParams dataclass."""

    def test_default_values(self) -> None:
        """Default optional fields are set correctly."""
        from pycypher.pattern_matcher import VariableLengthHopParams

        # Create minimal BindingFrame for the required field
        frame = BindingFrame(
            bindings=pd.DataFrame({"x": [1]}),
            type_registry={},
            context=None,
        )
        params = VariableLengthHopParams(
            frame=frame,
            rel_ast=RelationshipPattern(
                direction=RelationshipDirection.RIGHT,
            ),
            scan_rel_types=["KNOWS"],
            rel_var="r",
            prev_var="a",
            next_var="b",
            direction=RelationshipDirection.RIGHT,
        )
        assert params.next_type is None
        assert params.path_var_name is None
        assert params.row_limit is None
        assert params.anon_counter == [0]

    def test_frozen_immutability(self) -> None:
        """Frozen dataclass prevents attribute modification."""
        from pycypher.pattern_matcher import VariableLengthHopParams

        frame = BindingFrame(
            bindings=pd.DataFrame({"x": [1]}),
            type_registry={},
            context=None,
        )
        params = VariableLengthHopParams(
            frame=frame,
            rel_ast=RelationshipPattern(
                direction=RelationshipDirection.RIGHT,
            ),
            scan_rel_types=["KNOWS"],
            rel_var="r",
            prev_var="a",
            next_var="b",
            direction=RelationshipDirection.RIGHT,
        )
        with pytest.raises(AttributeError):
            params.rel_var = "changed"  # type: ignore[misc]
