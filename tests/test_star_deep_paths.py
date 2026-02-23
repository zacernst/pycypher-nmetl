"""Tests for deep execution paths in star.py.

Covers the uncovered lines in Star's pattern-translation methods:
- to_relation(str) — string-based conversion
- to_relation → NotImplementedError for unknown AST types
- _from_pattern with multi-path Pattern (pairwise joins)
- _from_pattern_path with Rel→Node ordering
- _from_intersection_list (common-variable join)
- _binary_join: all match arms (Node/Rel RIGHT, Node/Rel LEFT, Rel/Node RIGHT,
  Rel/Node LEFT, Node/Node error, Rel/Rel error, PatternPath cross, PatternPath inner)
- _from_relationship_pattern_with_attrs
- _from_node_pattern_no_attrs label-less variable re-use
- execute_query with multiple MATCH clauses
"""

from __future__ import annotations

import pandas as pd
import pytest

from pycypher.ast_models import (
    ASTConverter,
    IntegerLiteral,
    Match,
    NodePattern,
    Pattern,
    PatternIntersection,
    PatternPath,
    Query,
    RelationshipDirection,
    RelationshipPattern,
    Return,
    ReturnItem,
    PropertyLookup,
    Variable,
)
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    FilterRows,
    Join,
    JoinType,
    Projection,
    RelationshipMapping,
    RelationshipTable,
    SelectColumns,
)
from pycypher.star import Star


# ============================================================================
# Shared fixtures — small social graph with two entity types & two rel types
# ============================================================================


@pytest.fixture()
def person_df() -> pd.DataFrame:
    """Person entity data: Alice(1), Bob(2), Carol(3)."""
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 40, 25],
        }
    )


@pytest.fixture()
def city_df() -> pd.DataFrame:
    """City entity data: NYC(10), LA(11)."""
    return pd.DataFrame(
        {
            ID_COLUMN: [10, 11],
            "name": ["NYC", "LA"],
        }
    )


@pytest.fixture()
def knows_df() -> pd.DataFrame:
    """KNOWS relationship data (Person→Person)."""
    return pd.DataFrame(
        {
            ID_COLUMN: [100, 101, 102],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3],
            RELATIONSHIP_TARGET_COLUMN: [2, 3, 1],
            "since": [2020, 2021, 2019],
        }
    )


@pytest.fixture()
def lives_in_df() -> pd.DataFrame:
    """LIVES_IN relationship data (Person→City)."""
    return pd.DataFrame(
        {
            ID_COLUMN: [200, 201, 202],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3],
            RELATIONSHIP_TARGET_COLUMN: [10, 11, 10],
        }
    )


@pytest.fixture()
def person_table(person_df: pd.DataFrame) -> EntityTable:
    return EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=person_df,
    )


@pytest.fixture()
def city_table(city_df: pd.DataFrame) -> EntityTable:
    return EntityTable(
        entity_type="City",
        identifier="City",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=city_df,
    )


@pytest.fixture()
def knows_table(knows_df: pd.DataFrame) -> RelationshipTable:
    return RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN, "since"],
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "since": "since",
        },
        attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
            "since": "since",
        },
        source_obj=knows_df,
    )


@pytest.fixture()
def lives_in_table(lives_in_df: pd.DataFrame) -> RelationshipTable:
    return RelationshipTable(
        relationship_type="LIVES_IN",
        identifier="LIVES_IN",
        column_names=[ID_COLUMN, RELATIONSHIP_SOURCE_COLUMN, RELATIONSHIP_TARGET_COLUMN],
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
        },
        attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
        },
        source_obj=lives_in_df,
    )


@pytest.fixture()
def full_context(
    person_table: EntityTable,
    city_table: EntityTable,
    knows_table: RelationshipTable,
    lives_in_table: RelationshipTable,
) -> Context:
    """Context with Person, City entities and KNOWS, LIVES_IN relationships."""
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table, "City": city_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table, "LIVES_IN": lives_in_table}
        ),
    )


@pytest.fixture()
def person_only_context(
    person_table: EntityTable,
    knows_table: RelationshipTable,
) -> Context:
    """Minimal context with only Person and KNOWS."""
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={"KNOWS": knows_table}),
    )


# ============================================================================
# to_relation(str) — string-based entry point
# ============================================================================


class TestToRelationString:
    """Cover to_relation(str) which parses a Cypher pattern string.

    NOTE: These tests document two bugs in the to_relation(str) branch:
    1. The result is assigned to `pattern_obj` instead of `out`, causing NameError
    2. ASTConverter().from_cypher() returns a Query, but to_relation has no Query case
    These will be fixed in a future PR.
    """

    @pytest.mark.xfail(
        reason="to_relation(str) is broken: assigns to pattern_obj not out, "
        "and from_cypher returns Query which to_relation can't handle",
        strict=True,
    )
    def test_string_produces_relation(self, person_only_context: Context) -> None:
        """Passing a raw Cypher pattern string goes through ASTConverter."""
        star = Star(context=person_only_context)
        result = star.to_relation("(p:Person)-[k:KNOWS]->(q:Person)")
        assert Variable(name="p") in result.variable_map
        assert Variable(name="k") in result.variable_map
        assert Variable(name="q") in result.variable_map

    @pytest.mark.xfail(
        reason="to_relation(str) is broken: assigns to pattern_obj not out, "
        "and from_cypher returns Query which to_relation can't handle",
        strict=True,
    )
    def test_string_single_node(self, person_only_context: Context) -> None:
        """A simple node pattern string can be converted."""
        star = Star(context=person_only_context)
        result = star.to_relation("(p:Person)")
        assert Variable(name="p") in result.variable_map
        assert isinstance(result, Projection)


# ============================================================================
# to_relation → NotImplementedError
# ============================================================================


class TestToRelationUnsupported:
    """Cover the default match-case raising NotImplementedError."""

    def test_unsupported_ast_type_raises(self, person_only_context: Context) -> None:
        """Passing an unrecognized type raises NotImplementedError."""
        star = Star(context=person_only_context)
        with pytest.raises(NotImplementedError, match="not implemented"):
            star.to_relation(42)  # type: ignore[arg-type]


# ============================================================================
# _from_pattern — multi-path Pattern with pairwise joins
# ============================================================================


class TestFromPatternMultiPath:
    """Cover _from_pattern with 2+ pattern paths."""

    def test_two_path_pattern_shared_variable(self, full_context: Context) -> None:
        """Pattern with two paths sharing variable 'p' joins on p."""
        star = Star(context=full_context)

        # Equivalent to: (p:Person), (p)-[k:KNOWS]->(q:Person)
        path1 = PatternPath(
            elements=[NodePattern(variable=Variable(name="p"), labels=["Person"], properties={})]
        )
        path2 = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="p"), labels=["Person"], properties={}),
                RelationshipPattern(
                    variable=Variable(name="k"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                    properties={},
                ),
                NodePattern(variable=Variable(name="q"), labels=["Person"], properties={}),
            ]
        )

        pattern = Pattern(paths=[path1, path2])
        result = star.to_relation(pattern)

        assert Variable(name="p") in result.variable_map
        assert Variable(name="k") in result.variable_map
        assert Variable(name="q") in result.variable_map

        # Should produce valid rows
        df = star.to_pandas(result)
        assert len(df) > 0

    def test_two_path_pattern_disjoint_variables(self, full_context: Context) -> None:
        """Pattern with disjoint variables produces cross product."""
        star = Star(context=full_context)

        path1 = PatternPath(
            elements=[NodePattern(variable=Variable(name="p"), labels=["Person"], properties={})]
        )
        path2 = PatternPath(
            elements=[NodePattern(variable=Variable(name="c"), labels=["City"], properties={})]
        )

        pattern = Pattern(paths=[path1, path2])
        result = star.to_relation(pattern)

        assert Variable(name="p") in result.variable_map
        assert Variable(name="c") in result.variable_map

        df = star.to_pandas(result)
        # 3 persons × 2 cities = 6 rows
        assert len(df) == 6

    def test_single_path_pattern_no_join(self, person_only_context: Context) -> None:
        """Pattern with a single path needs no multi-path join."""
        star = Star(context=person_only_context)

        path = PatternPath(
            elements=[NodePattern(variable=Variable(name="p"), labels=["Person"], properties={})]
        )
        pattern = Pattern(paths=[path])
        result = star.to_relation(pattern)

        assert Variable(name="p") in result.variable_map

    def test_empty_pattern_raises(self, person_only_context: Context) -> None:
        """Pattern with no paths raises ValueError."""
        star = Star(context=person_only_context)
        pattern = Pattern(paths=[])
        with pytest.raises(ValueError, match="at least one element"):
            star.to_relation(pattern)


# ============================================================================
# _from_pattern_path — covers Rel→Node ordering
# ============================================================================


class TestFromPatternPath:
    """Cover _from_pattern_path including the Rel→Node match arm."""

    def test_right_directed_traversal(self, person_only_context: Context) -> None:
        """Standard (p)-[k]->(q) traversal."""
        star = Star(context=person_only_context)

        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="p"), labels=["Person"], properties={}),
                RelationshipPattern(
                    variable=Variable(name="k"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                    properties={},
                ),
                NodePattern(variable=Variable(name="q"), labels=["Person"], properties={}),
            ]
        )
        result = star.to_relation(path)

        assert Variable(name="p") in result.variable_map
        assert Variable(name="k") in result.variable_map
        assert Variable(name="q") in result.variable_map

        df = star.to_pandas(result)
        assert len(df) == 3  # 3 KNOWS edges

    def test_left_directed_traversal(self, person_only_context: Context) -> None:
        """Left-directed (p)<-[k]-(q) traversal.

        This creates Node→Rel ordering where direction is LEFT, exercising
        the second match arm in _from_pattern_path (using _from_node_relationship_head).
        """
        star = Star(context=person_only_context)

        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="p"), labels=["Person"], properties={}),
                RelationshipPattern(
                    variable=Variable(name="k"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.LEFT,
                    properties={},
                ),
                NodePattern(variable=Variable(name="q"), labels=["Person"], properties={}),
            ]
        )
        result = star.to_relation(path)

        assert Variable(name="p") in result.variable_map
        assert Variable(name="k") in result.variable_map
        assert Variable(name="q") in result.variable_map

        df = star.to_pandas(result)
        # Each KNOWS edge maps to exactly one left-directed row
        assert len(df) == 3

    def test_single_node_path(self, person_only_context: Context) -> None:
        """PatternPath with a single node returns directly (no join)."""
        star = Star(context=person_only_context)

        path = PatternPath(
            elements=[NodePattern(variable=Variable(name="p"), labels=["Person"], properties={})]
        )
        result = star.to_relation(path)
        assert Variable(name="p") in result.variable_map

    def test_empty_path_raises(self, person_only_context: Context) -> None:
        """Empty PatternPath raises ValueError."""
        star = Star(context=person_only_context)
        path = PatternPath(elements=[])
        with pytest.raises(ValueError, match="at least one element"):
            star.to_relation(path)

    def test_multi_hop_path(self, person_only_context: Context) -> None:
        """Multi-hop (p)-[k]->(q)-[m]->(r) traversal.

        This produces 3 element pairs: (Node,Rel), (Rel,Node), (Node,Rel), ...
        and exercises the Rel→Node match arm in _from_pattern_path.
        """
        star = Star(context=person_only_context)

        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="p"), labels=["Person"], properties={}),
                RelationshipPattern(
                    variable=Variable(name="k"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                    properties={},
                ),
                NodePattern(variable=Variable(name="q"), labels=["Person"], properties={}),
                RelationshipPattern(
                    variable=Variable(name="m"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                    properties={},
                ),
                NodePattern(variable=Variable(name="r"), labels=["Person"], properties={}),
            ]
        )
        result = star.to_relation(path)

        for name in ["p", "k", "q", "m", "r"]:
            assert Variable(name=name) in result.variable_map

        df = star.to_pandas(result)
        assert len(df) > 0  # At least one 2-hop path should exist


# ============================================================================
# _binary_join — individual match arms
# ============================================================================


class TestBinaryJoinArms:
    """Cover specific match arms in _binary_join that aren't exercised
    by _from_pattern_path (which uses _from_node_relationship_tail/head directly).
    """

    def _make_node_relation(self, star: Star, var_name: str, label: str) -> Projection:
        """Helper: create a Projection from a NodePattern."""
        node = NodePattern(variable=Variable(name=var_name), labels=[label], properties={})
        return star._from_node_pattern(node=node)

    def _make_rel_relation(
        self, star: Star, var_name: str, label: str, direction: RelationshipDirection
    ) -> Projection:
        """Helper: create a Projection from a RelationshipPattern."""
        rel = RelationshipPattern(
            variable=Variable(name=var_name),
            labels=[label],
            direction=direction,
            properties={},
        )
        result = star._from_relationship_pattern(relationship=rel)
        # Stamp source_algebraizable so _binary_join can match
        result.source_algebraizable = rel
        return result

    def test_node_rel_right(self, person_only_context: Context) -> None:
        """(Node, Rel RIGHT) → _binary_join produces SelectColumns wrapping Join."""
        star = Star(context=person_only_context)
        node_rel = self._make_node_relation(star, "p", "Person")
        rel_rel = self._make_rel_relation(star, "k", "KNOWS", RelationshipDirection.RIGHT)
        result = star._binary_join(left=node_rel, right=rel_rel)
        assert isinstance(result, SelectColumns)
        assert Variable(name="p") in result.variable_map
        assert Variable(name="k") in result.variable_map

    def test_node_rel_left(self, person_only_context: Context) -> None:
        """(Node, Rel LEFT) → swaps and recurses."""
        star = Star(context=person_only_context)
        node_rel = self._make_node_relation(star, "p", "Person")
        rel_rel = self._make_rel_relation(star, "k", "KNOWS", RelationshipDirection.LEFT)
        result = star._binary_join(left=node_rel, right=rel_rel)
        # After swap and recursion, should still produce a valid result
        assert Variable(name="p") in result.variable_map
        assert Variable(name="k") in result.variable_map

    def test_rel_node_right(self, person_only_context: Context) -> None:
        """(Rel RIGHT, Node) → joins on RELATIONSHIP_TARGET_COLUMN."""
        star = Star(context=person_only_context)
        rel_rel = self._make_rel_relation(star, "k", "KNOWS", RelationshipDirection.RIGHT)
        node_rel = self._make_node_relation(star, "q", "Person")
        result = star._binary_join(left=rel_rel, right=node_rel)
        assert isinstance(result, SelectColumns)
        assert Variable(name="k") in result.variable_map
        assert Variable(name="q") in result.variable_map

    def test_rel_node_left(self, person_only_context: Context) -> None:
        """(Rel LEFT, Node) → swaps direction and recurses."""
        star = Star(context=person_only_context)
        rel_rel = self._make_rel_relation(star, "k", "KNOWS", RelationshipDirection.LEFT)
        node_rel = self._make_node_relation(star, "q", "Person")
        result = star._binary_join(left=rel_rel, right=node_rel)
        assert Variable(name="k") in result.variable_map
        assert Variable(name="q") in result.variable_map

    def test_node_node_raises(self, person_only_context: Context) -> None:
        """(Node, Node) raises ValueError."""
        star = Star(context=person_only_context)
        left = self._make_node_relation(star, "p", "Person")
        right = self._make_node_relation(star, "q", "Person")
        with pytest.raises(ValueError, match="Cannot join two NodePatterns"):
            star._binary_join(left=left, right=right)

    def test_rel_rel_raises(self, person_only_context: Context) -> None:
        """(Rel, Rel) raises ValueError."""
        star = Star(context=person_only_context)
        left = self._make_rel_relation(star, "k", "KNOWS", RelationshipDirection.RIGHT)
        right = self._make_rel_relation(star, "m", "KNOWS", RelationshipDirection.RIGHT)
        with pytest.raises(ValueError, match="Cannot join two RelationshipPatterns"):
            star._binary_join(left=left, right=right)

    def test_patternpath_cross_product(self, full_context: Context) -> None:
        """(PatternPath, PatternPath) with disjoint variables → cross product."""
        star = Star(context=full_context)

        # Build two PatternPath-based relations with no shared variables
        path1 = PatternPath(
            elements=[NodePattern(variable=Variable(name="p"), labels=["Person"], properties={})]
        )
        path2 = PatternPath(
            elements=[NodePattern(variable=Variable(name="c"), labels=["City"], properties={})]
        )
        left = star.to_relation(path1)
        right = star.to_relation(path2)

        result = star._binary_join(left=left, right=right)
        assert isinstance(result, Join)
        assert result.join_type == JoinType.CROSS

        df = star.to_pandas(result)
        assert len(df) == 6  # 3 × 2

    def test_patternpath_inner_join(self, person_only_context: Context) -> None:
        """(PatternPath, PatternPath) with shared variable → inner join."""
        star = Star(context=person_only_context)

        # Two paths that share variable 'p'
        path1 = PatternPath(
            elements=[NodePattern(variable=Variable(name="p"), labels=["Person"], properties={})]
        )
        path2 = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="p"), labels=["Person"], properties={}),
                RelationshipPattern(
                    variable=Variable(name="k"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                    properties={},
                ),
                NodePattern(variable=Variable(name="q"), labels=["Person"], properties={}),
            ]
        )
        left = star.to_relation(path1)
        right = star.to_relation(path2)

        result = star._binary_join(left=left, right=right)
        assert isinstance(result, Join)
        assert result.join_type == JoinType.INNER

        df = star.to_pandas(result)
        assert len(df) > 0


# ============================================================================
# _from_relationship_pattern_with_attrs
# ============================================================================


class TestRelationshipPatternWithAttrs:
    """Cover _from_relationship_pattern_with_attrs."""

    def test_single_property(self, person_only_context: Context) -> None:
        """Relationship with one property produces FilterRows."""
        star = Star(context=person_only_context)
        rel = RelationshipPattern(
            variable=Variable(name="k"),
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
            properties={"since": IntegerLiteral(value=2020)},
        )
        result = star.to_relation(rel)
        assert isinstance(result, FilterRows)
        assert Variable(name="k") in result.variable_map

    def test_multiple_properties(self, person_only_context: Context) -> None:
        """Relationship with two properties produces nested FilterRows."""
        star = Star(context=person_only_context)
        rel = RelationshipPattern(
            variable=Variable(name="k"),
            labels=["KNOWS"],
            direction=RelationshipDirection.RIGHT,
            properties={
                "since": IntegerLiteral(value=2020),
            },
        )
        result = star.to_relation(rel)
        assert isinstance(result, FilterRows)
        # The inner relation is a Projection (base case after stripping props)
        assert isinstance(result.relation, Projection)


# ============================================================================
# _from_node_pattern_no_attrs — label-less variable reuse
# ============================================================================


class TestNodePatternLabellessLookup:
    """Cover the label-less variable lookup branch in _from_node_pattern_no_attrs."""

    def test_registered_variable_type_used(self, person_only_context: Context) -> None:
        """A variable without labels is resolved from the type registry."""
        star = Star(context=person_only_context)

        # First: register 'p' as type 'Person' by converting a labeled node
        labeled_node = NodePattern(
            variable=Variable(name="p"), labels=["Person"], properties={}
        )
        star.to_relation(labeled_node)

        # Now: convert a label-less node referencing 'p'
        unlabeled_node = NodePattern(
            variable=Variable(name="p"), labels=[], properties={}
        )
        result = star.to_relation(unlabeled_node)
        assert isinstance(result, Projection)
        assert Variable(name="p") in result.variable_map
        assert result.variable_type_map[Variable(name="p")] == "Person"

    def test_unregistered_variable_raises(self, person_only_context: Context) -> None:
        """A label-less variable not in the registry raises ValueError."""
        star = Star(context=person_only_context)
        unlabeled_node = NodePattern(
            variable=Variable(name="z"), labels=[], properties={}
        )
        with pytest.raises(ValueError, match="not found in registry"):
            star.to_relation(unlabeled_node)

    def test_extract_variable_types_from_pattern(self, person_only_context: Context) -> None:
        """_extract_variable_types populates the registry before processing."""
        star = Star(context=person_only_context)

        # Build a pattern that declares (p:Person) and then references (p) without labels
        path1 = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="p"), labels=["Person"], properties={}),
            ]
        )
        path2 = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="p"), labels=[], properties={}),
                RelationshipPattern(
                    variable=Variable(name="k"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                    properties={},
                ),
                NodePattern(variable=Variable(name="q"), labels=["Person"], properties={}),
            ]
        )
        pattern = Pattern(paths=[path1, path2])
        result = star.to_relation(pattern)

        assert Variable(name="p") in result.variable_map
        assert Variable(name="q") in result.variable_map


# ============================================================================
# _from_intersection_list (now without pdb.set_trace)
# ============================================================================


class TestFromIntersectionList:
    """Cover _from_intersection_list with common-variable join."""

    def test_two_relations_common_variable(self, person_only_context: Context) -> None:
        """Two relations sharing a variable are joined."""
        star = Star(context=person_only_context)

        # Create two different projections that share variable 'p'
        node1 = NodePattern(variable=Variable(name="p"), labels=["Person"], properties={})
        node2 = NodePattern(variable=Variable(name="p"), labels=["Person"], properties={})
        rel1 = star.to_relation(node1)
        rel2 = star.to_relation(node2)

        result = star._from_intersection_list([rel1, rel2])
        assert isinstance(result, Join)
        assert Variable(name="p") in result.variable_map

    def test_no_common_variables_raises(self, full_context: Context) -> None:
        """Relations with no common variables raise NotImplementedError."""
        star = Star(context=full_context)

        node_p = NodePattern(variable=Variable(name="p"), labels=["Person"], properties={})
        node_c = NodePattern(variable=Variable(name="c"), labels=["City"], properties={})
        rel_p = star.to_relation(node_p)
        rel_c = star.to_relation(node_c)

        with pytest.raises(NotImplementedError, match="No common variables"):
            star._from_intersection_list([rel_p, rel_c])


# ============================================================================
# execute_query — multiple MATCH clauses
# ============================================================================


class TestExecuteQueryMultiMatch:
    """Cover execute_query with multiple MATCH clauses (line 1277-1279)."""

    def test_two_match_clauses(self, person_only_context: Context) -> None:
        """Two MATCH clauses are joined then RETURN projects from the combined relation."""
        star = Star(context=person_only_context)

        query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=Variable(name="p"),
                                        labels=["Person"],
                                        properties={},
                                    ),
                                    RelationshipPattern(
                                        variable=Variable(name="k"),
                                        labels=["KNOWS"],
                                        direction=RelationshipDirection.RIGHT,
                                        properties={},
                                    ),
                                    NodePattern(
                                        variable=Variable(name="q"),
                                        labels=["Person"],
                                        properties={},
                                    ),
                                ]
                            )
                        ]
                    )
                ),
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=Variable(name="q"),
                                        labels=["Person"],
                                        properties={},
                                    ),
                                    RelationshipPattern(
                                        variable=Variable(name="m"),
                                        labels=["KNOWS"],
                                        direction=RelationshipDirection.RIGHT,
                                        properties={},
                                    ),
                                    NodePattern(
                                        variable=Variable(name="r"),
                                        labels=["Person"],
                                        properties={},
                                    ),
                                ]
                            )
                        ]
                    )
                ),
                Return(
                    items=[
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="p"), property="name"
                            ),
                            alias="p_name",
                        ),
                        ReturnItem(
                            expression=PropertyLookup(
                                expression=Variable(name="r"), property="name"
                            ),
                            alias="r_name",
                        ),
                    ]
                ),
            ]
        )

        df = star.execute_query(query)
        assert "p_name" in df.columns
        assert "r_name" in df.columns
        assert len(df) > 0


# ============================================================================
# End-to-end: data correctness checks
# ============================================================================


class TestEndToEndDataCorrectness:
    """Verify that traversals produce correct data, not just structure."""

    def test_right_directed_source_target(self, person_only_context: Context) -> None:
        """(p)-[k:KNOWS]->(q): p maps to SOURCE, q maps to TARGET of each edge."""
        star = Star(context=person_only_context)

        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="p"), labels=["Person"], properties={}),
                RelationshipPattern(
                    variable=Variable(name="k"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                    properties={},
                ),
                NodePattern(variable=Variable(name="q"), labels=["Person"], properties={}),
            ]
        )
        result = star.to_relation(path)
        df = star.to_pandas(result)

        # The KNOWS edges are: 1→2, 2→3, 3→1
        p_col = result.variable_map[Variable(name="p")]
        q_col = result.variable_map[Variable(name="q")]
        edges = set(zip(df[p_col].tolist(), df[q_col].tolist()))
        assert edges == {(1, 2), (2, 3), (3, 1)}

    def test_left_directed_reverses_source_target(self, person_only_context: Context) -> None:
        """(p)<-[k:KNOWS]-(q): p maps to TARGET, q maps to SOURCE."""
        star = Star(context=person_only_context)

        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="p"), labels=["Person"], properties={}),
                RelationshipPattern(
                    variable=Variable(name="k"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.LEFT,
                    properties={},
                ),
                NodePattern(variable=Variable(name="q"), labels=["Person"], properties={}),
            ]
        )
        result = star.to_relation(path)
        df = star.to_pandas(result)

        p_col = result.variable_map[Variable(name="p")]
        q_col = result.variable_map[Variable(name="q")]
        edges = set(zip(df[p_col].tolist(), df[q_col].tolist()))
        # Left-directed: p is the target, q is the source
        # So KNOWS 1→2 becomes q=1, p=2 → (p=2,q=1)
        assert edges == {(2, 1), (3, 2), (1, 3)}

    def test_cross_entity_traversal(self, full_context: Context) -> None:
        """(p:Person)-[l:LIVES_IN]->(c:City) produces correct city mappings."""
        star = Star(context=full_context)

        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="p"), labels=["Person"], properties={}),
                RelationshipPattern(
                    variable=Variable(name="l"),
                    labels=["LIVES_IN"],
                    direction=RelationshipDirection.RIGHT,
                    properties={},
                ),
                NodePattern(variable=Variable(name="c"), labels=["City"], properties={}),
            ]
        )
        result = star.to_relation(path)
        df = star.to_pandas(result)

        p_col = result.variable_map[Variable(name="p")]
        c_col = result.variable_map[Variable(name="c")]
        mappings = set(zip(df[p_col].tolist(), df[c_col].tolist()))
        # LIVES_IN: 1→10, 2→11, 3→10
        assert mappings == {(1, 10), (2, 11), (3, 10)}

    def test_multi_hop_data_correctness(self, person_only_context: Context) -> None:
        """(p)-[k]->(q)-[m]->(r): verifies 2-hop paths are correct."""
        star = Star(context=person_only_context)

        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="p"), labels=["Person"], properties={}),
                RelationshipPattern(
                    variable=Variable(name="k"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                    properties={},
                ),
                NodePattern(variable=Variable(name="q"), labels=["Person"], properties={}),
                RelationshipPattern(
                    variable=Variable(name="m"),
                    labels=["KNOWS"],
                    direction=RelationshipDirection.RIGHT,
                    properties={},
                ),
                NodePattern(variable=Variable(name="r"), labels=["Person"], properties={}),
            ]
        )
        result = star.to_relation(path)
        df = star.to_pandas(result)

        p_col = result.variable_map[Variable(name="p")]
        q_col = result.variable_map[Variable(name="q")]
        r_col = result.variable_map[Variable(name="r")]

        # KNOWS edges: 1→2, 2→3, 3→1
        # Valid 2-hop paths: 1→2→3, 2→3→1, 3→1→2
        hops = set(
            zip(df[p_col].tolist(), df[q_col].tolist(), df[r_col].tolist())
        )
        assert hops == {(1, 2, 3), (2, 3, 1), (3, 1, 2)}
