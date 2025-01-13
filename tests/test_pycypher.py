'''All the tests.'''
# pylint: disable=missing-function-docstring,protected-access,redefined-outer-name,too-many-lines

from unittest.mock import patch, Mock

import networkx as nx
import pytest
from pytest_unordered import unordered

from pycypher.cypher_parser import CypherParser
from pycypher.exceptions import WrongCypherTypeError
from pycypher.fact import (  # We might get rid of this class entirely
    FactCollection,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactNodeRelatedToNode,
)
from pycypher.fixtures import (  # pylint: disable=unused-import
    fact_collection_0,
    fact_collection_1,
    fact_collection_2,
    fact_collection_3,
    fact_collection_4,
    fact_collection_5,
    fact_collection_6,
    fact_collection_7,
    networkx_graph,
)
from pycypher.solver import IsTrue
from pycypher.node_classes import Alias  # pylint: disable=unused-import
from pycypher.node_classes import (
    Addition,
    Aggregation,
    AliasedName,
    And,
    Collect,
    Collection,
    Cypher,
    Distinct,
    Division,
    Equals,
    Evaluable,
    GreaterThan,
    LessThan,
    Literal,
    Mapping,
    MappingSet,
    Match,
    Multiplication,
    Node,
    NodeNameLabel,
    Not,
    ObjectAsSeries,
    ObjectAttributeLookup,
    Or,
    Predicate,
    Projection,
    Query,
    Relationship,
    RelationshipChain,
    RelationshipChainList,
    RelationshipLeftRight,
    RelationshipRightLeft,
    Return,
    Size,
    Subtraction,
    Where,
    WithClause,
)
from pycypher.query import QueryValueOfNodeAttribute
from pycypher.shims.networkx_cypher import NetworkX
from pycypher.solver import (
    ConstraintNodeHasAttributeWithValue,
    ConstraintNodeHasLabel,
    ConstraintRelationshipHasLabel,
    ConstraintRelationshipHasSourceNode,
    ConstraintRelationshipHasTargetNode,
)
from pycypher.tree_mixin import TreeMixin


def test_fact_collection_has_facts(fact_collection_0: FactCollection):
    assert fact_collection_0


def test_fact_collection_del_item(fact_collection_0: FactCollection):
    first_fact = fact_collection_0[0]
    assert first_fact in fact_collection_0
    del fact_collection_0[0]
    assert first_fact not in fact_collection_0


def test_fact_collection_set_item(fact_collection_0: FactCollection):
    fact = FactNodeHasLabel("3", "Thing")
    fact_collection_0[0] = fact
    assert fact_collection_0[0] == fact


def test_fact_collection_get_item(fact_collection_0: FactCollection):
    fact = fact_collection_0[0]
    assert isinstance(fact, FactNodeHasLabel)


def test_fact_collection_insert(fact_collection_0: FactCollection):
    fact = FactNodeHasLabel("3", "Thing")
    assert fact not in fact_collection_0
    fact_collection_0.insert(0, fact)
    assert fact in fact_collection_0


def test_can_parse_simple_cypher():
    obj = CypherParser("MATCH (n) RETURN n.foo")
    assert isinstance(obj, CypherParser)


def test_parser_builds_cypher_object():
    obj = CypherParser("MATCH (n) RETURN n.foo")
    assert isinstance(obj.parsed, Cypher)


def test_parser_creates_simple_node_object():
    obj = CypherParser("MATCH (n) RETURN n.foo")
    assert isinstance(obj.parsed.cypher, Query)


def test_parser_parses_complicated_query():
    query = (
        """MATCH (n:Thing {key1: "value", key2: 5})-[r]->(m:OtherThing {key3: "hithere"}) """
        """WHERE n.key = 2, n.foo = 3 """
        """RETURN n.foobar, n.baz"""
    )
    obj = CypherParser(query)
    assert isinstance(obj.parsed, Cypher)


def test_parser_handles_node_label():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) RETURN n.foobar"""
    obj = CypherParser(query)
    assert (
        obj.parsed.cypher.match_clause.pattern.relationships[0]
        .steps[0]
        .node_name_label.label
        == "Thingy"
    )


def test_parser_handles_where_clause():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(obj.parsed.cypher.match_clause.where, Where)


def test_parser_handles_where_clause_predicate():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(obj.parsed.cypher.match_clause.where.predicate, Equals)


def test_parser_handles_where_clause_predicate_lookup():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.match_clause.where.predicate.left_side,
        ObjectAttributeLookup,
    )


def test_parser_handles_where_clause_predicate_literal():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.match_clause.where.predicate.right_side, Literal
    )


def test_parser_generates_alias_in_return_statement():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar AS myfoobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.return_clause.projection.lookups[0], Alias
    )


def test_parser_generates_alias_with_correct_name_in_return_statement():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar AS myfoobar"""
    obj = CypherParser(query)
    assert (
        obj.parsed.cypher.return_clause.projection.lookups[0].alias
        == "myfoobar"
    )


def test_node_has_label_equality():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("1", "Thing")
    assert fact1 == fact2


def test_node_has_label_inequality():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "Thing")
    assert fact1 != fact2


def test_node_has_attribute_with_value_equality():
    fact1 = FactNodeHasAttributeWithValue("1", "key", 2)
    fact2 = FactNodeHasAttributeWithValue("1", "key", 2)
    assert fact1 == fact2


def test_node_has_attribute_with_value_inequality():
    fact1 = FactNodeHasAttributeWithValue("1", "key", 2)
    fact2 = FactNodeHasAttributeWithValue("1", "key", 3)
    assert fact1 != fact2


def test_node_has_related_node_equality():
    fact1 = FactNodeRelatedToNode("1", "2", "MyRelationship")
    fact2 = FactNodeRelatedToNode("1", "2", "MyRelationship")
    assert fact1 == fact2


def test_node_has_related_node_inequality():
    fact1 = FactNodeRelatedToNode("1", "2", "MyRelationship")
    fact2 = FactNodeRelatedToNode("1", "2", "MyOtherRelationship")
    assert fact1 != fact2


def test_aggregate_constraints_node_label():
    cypher = "MATCH (m:Thing) RETURN m.foobar"
    result = CypherParser(cypher)
    constraints = result.parsed.cypher.match_clause.constraints
    assert len(constraints) == 1


def test_aggregate_constraints_node_and_mapping():
    cypher = "MATCH (m:Thing {key: 2}) RETURN m.foobar"
    result = CypherParser(cypher)
    constraints = result.parsed.cypher.match_clause.constraints
    assert len(constraints) == 2


def test_parse_anonymous_node_no_label_no_mapping_gets_variable():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH () RETURN m.foobar"
        result = CypherParser(cypher)
        assert (
            result.parsed.cypher.match_clause.pattern.node_name_label.name
            == "SOME_HEX"
        )


def test_parse_anonymous_node_with_label_no_mapping_gets_variable():
    cypher = "MATCH (:Thing) RETURN m.foobar"
    result = CypherParser(cypher)
    assert int(
        result.parsed.cypher.match_clause.pattern.node_name_label.name, 32
    )


def test_parse_anonymous_node_with_label_no_mapping_has_right_label():
    cypher = "MATCH (:Thing) RETURN m.foobar"
    result = CypherParser(cypher)
    assert (
        result.parsed.cypher.match_clause.pattern.node_name_label.label
        == "Thing"
    )


def test_source_node_constraint_from_left_right_relationship():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH (n:Thing)-[:Relationship]->(m:Other) RETURN n.foobar"
        result = CypherParser(cypher)
        assert (
            ConstraintRelationshipHasSourceNode("n", "SOME_HEX")
            in result.parsed.cypher.match_clause.constraints
        )


def test_source_node_constraint_from_left_right_relationship_with_label():
    cypher = "MATCH (n:Thing)-[r:Relationship]->(m:Other) RETURN n.foobar"
    result = CypherParser(cypher)
    assert (
        ConstraintRelationshipHasSourceNode("n", "r")
        in result.parsed.cypher.match_clause.constraints
    )


def test_target_node_constraint_from_left_right_relationship():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH (n:Thing)-[:Relationship]->(m:Other) RETURN n.foobar"
        result = CypherParser(cypher)
        assert (
            ConstraintRelationshipHasTargetNode("m", "SOME_HEX")
            in result.parsed.cypher.match_clause.constraints
        )


def test_target_node_constraint_from_left_right_relationship_with_label():
    cypher = "MATCH (n:Thing)-[r:Relationship]->(m:Other) RETURN n.foobar"
    result = CypherParser(cypher)
    assert (
        ConstraintRelationshipHasTargetNode("m", "r")
        in result.parsed.cypher.match_clause.constraints
    )


def test_constraint_node_has_label():
    cypher = "MATCH (n:Thing) RETURN n.foobar"
    result = CypherParser(cypher)
    assert (
        ConstraintNodeHasLabel("n", "Thing")
        in result.parsed.cypher.match_clause.constraints
    )


class patched_uuid:  # pylint: disable=invalid-name,too-few-public-methods
    """Creates a deterministic value for uuid hex"""

    @property
    def hex(self):
        return "SOME_HEX"


def test_constraint_relationship_has_label():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH (n:Thing)-[:Relationship]->(m:Other) RETURN n.foobar"
        result = CypherParser(cypher)
        assert (
            ConstraintRelationshipHasLabel("SOME_HEX", "Relationship")
            in result.parsed.cypher.match_clause.constraints
        )


def test_constraint_relationship_has_source_node():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH (n:Thing)-[:Relationship]->(m:Other) RETURN n.foobar"
        result = CypherParser(cypher)
        assert (
            ConstraintRelationshipHasSourceNode("n", "SOME_HEX")
            in result.parsed.cypher.match_clause.constraints
        )


def test_constraint_relationship_has_target_node():
    with patch("uuid.uuid4", patched_uuid) as _:
        cypher = "MATCH (n:Thing)-[:Relationship]->(m:Other) RETURN n.foobar"
        result = CypherParser(cypher)
        assert (
            ConstraintRelationshipHasTargetNode("m", "SOME_HEX")
            in result.parsed.cypher.match_clause.constraints
        )


def test_find_solution_node_has_label(fact_collection_0: FactCollection):
    cypher = "MATCH (n:Thing) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_0)
    expected = [{"n": "1"}]
    assert solutions == expected


def test_find_solution_node_has_wrong_label(fact_collection_0: FactCollection):
    cypher = "MATCH (n:WrongLabel) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_0)
    assert not solutions


def test_find_solution_node_with_relationship(
    fact_collection_0: FactCollection,
):
    # Hash variable for relationship not being added to variable list
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:OtherThing) RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_0)
    expected = [{"n": "1", "m": "2", "r": "relationship_123"}]
    assert solutions == expected


def test_find_solution_node_with_relationship_nonexistant(
    fact_collection_0: FactCollection,
):
    # Hash variable for relationship not being added to variable list
    cypher = "MATCH (n:Thing)-[r:NotExistingRelationship]->(m:OtherThing) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_0)
    expected = []
    assert solutions == expected


def test_find_solution_node_with_attribute_value(
    fact_collection_0: FactCollection,
):
    cypher = "MATCH (n:Thing {key: 2}) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_0)
    expected = [{"n": "1"}]
    assert solutions == expected


def test_find_no_solution_node_with_wrong_attribute_value(
    fact_collection_0: FactCollection,
):
    cypher = "MATCH (n:Thing {key: 123}) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_0)
    expected = []
    assert solutions == expected


def test_find_solution_node_with_attribute_and_relationship(
    fact_collection_0: FactCollection,
):
    cypher = "MATCH (n:Thing {key: 2})-[r:MyRelationship]->(m:OtherThing) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_0)
    expected = [{"n": "1", "m": "2", "r": "relationship_123"}]
    assert solutions == expected


def test_find_no_solution_node_with_wrong_attribute_and_relationship(
    fact_collection_0: FactCollection,
):
    cypher = "MATCH (n:Thing {key: 3})-[r:MyRelationship]->(m:OtherThing) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_0)
    expected = []
    assert solutions == expected


def test_find_no_solution_node_with_wrong_attribute_type_and_relationship(
    fact_collection_0: FactCollection,
):
    cypher = 'MATCH (n:Thing {key: "3"})-[r:MyRelationship]->(m:OtherThing) RETURN n.foobar'
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_0)
    expected = []
    assert solutions == expected


def test_find_solution_node_with_attribute_type_and_relationship_target_node_attribute(
    fact_collection_0: FactCollection,
):
    cypher = "MATCH (n:Thing {key: 2})-[r:MyRelationship]->(m:OtherThing {key: 5}) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_0)
    expected = [{"n": "1", "m": "2", "r": "relationship_123"}]
    assert solutions == expected


def test_find_no_solution_node_with_attribute_type_and_wrong_relationship_target_node_attribute(
    fact_collection_0: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {key: 2})-[r:NoRelationshipLikeMeExists]->(m:OtherThing {key: 5}) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_0)
    expected = []
    assert solutions == expected


def test_find_two_solutions_node_has_label(fact_collection_1: FactCollection):
    cypher = "MATCH (n:Thing) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_1)
    expected = [{"n": "1"}, {"n": "2"}]
    assert solutions == unordered(expected)


def test_constraints_from_relationship_chain():
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->"
        "(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    constraint1 = ConstraintRelationshipHasSourceNode(
        source_node_name="n", relationship_name="r"
    )
    constraint2 = ConstraintRelationshipHasTargetNode(
        target_node_name="m", relationship_name="r"
    )
    constraint3 = ConstraintRelationshipHasSourceNode(
        source_node_name="m", relationship_name="s"
    )
    constraint4 = ConstraintRelationshipHasTargetNode(
        target_node_name="o", relationship_name="s"
    )
    constraint5 = ConstraintRelationshipHasLabel(
        relationship_name="r", label="MyRelationship"
    )
    constraint6 = ConstraintRelationshipHasLabel(
        relationship_name="s", label="OtherRelationship"
    )
    constraint7 = ConstraintNodeHasLabel(node_id="n", label="Thing")
    constraint8 = ConstraintNodeHasLabel(node_id="m", label="MiddleThing")
    constraint9 = ConstraintNodeHasLabel(node_id="o", label="OtherThing")
    assert constraint1 in result.parsed.cypher.match_clause.constraints
    assert constraint2 in result.parsed.cypher.match_clause.constraints
    assert constraint3 in result.parsed.cypher.match_clause.constraints
    assert constraint4 in result.parsed.cypher.match_clause.constraints
    assert constraint5 in result.parsed.cypher.match_clause.constraints
    assert constraint6 in result.parsed.cypher.match_clause.constraints
    assert constraint7 in result.parsed.cypher.match_clause.constraints
    assert constraint8 in result.parsed.cypher.match_clause.constraints
    assert constraint9 in result.parsed.cypher.match_clause.constraints


def test_constraints_from_relationship_pair():
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing), "
        "(m)-[s:OtherRelationship]->(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    constraint1 = ConstraintRelationshipHasSourceNode(
        source_node_name="n", relationship_name="r"
    )
    constraint2 = ConstraintRelationshipHasTargetNode(
        target_node_name="m", relationship_name="r"
    )
    constraint3 = ConstraintRelationshipHasSourceNode(
        source_node_name="m", relationship_name="s"
    )
    constraint4 = ConstraintRelationshipHasTargetNode(
        target_node_name="o", relationship_name="s"
    )
    constraint5 = ConstraintRelationshipHasLabel(
        relationship_name="r", label="MyRelationship"
    )
    constraint6 = ConstraintRelationshipHasLabel(
        relationship_name="s", label="OtherRelationship"
    )
    constraint7 = ConstraintNodeHasLabel(node_id="n", label="Thing")
    constraint8 = ConstraintNodeHasLabel(node_id="m", label="MiddleThing")
    constraint9 = ConstraintNodeHasLabel(node_id="o", label="OtherThing")
    assert constraint1 in result.parsed.cypher.match_clause.constraints
    assert constraint2 in result.parsed.cypher.match_clause.constraints
    assert constraint3 in result.parsed.cypher.match_clause.constraints
    assert constraint4 in result.parsed.cypher.match_clause.constraints
    assert constraint5 in result.parsed.cypher.match_clause.constraints
    assert constraint6 in result.parsed.cypher.match_clause.constraints
    assert constraint7 in result.parsed.cypher.match_clause.constraints
    assert constraint8 in result.parsed.cypher.match_clause.constraints
    assert constraint9 in result.parsed.cypher.match_clause.constraints
    assert len(result.parsed.cypher.match_clause.constraints) == 9


def test_find_solution_relationship_chain_two_forks(
    fact_collection_2: FactCollection,
):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->"
        "(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_2)
    expected = [
        {
            "m": "2",
            "r": "relationship_1",
            "s": "relationship_2",
            "n": "1",
            "o": "3",
        }
    ]
    assert solutions == expected


def test_find_solution_relationship_chain_fork(
    fact_collection_3: FactCollection,
):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->"
        "(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_3)
    expected = [
        {
            "m": "2",
            "r": "relationship_1",
            "s": "relationship_2",
            "n": "1",
            "o": "3",
        },
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_2",
            "n": "4",
            "o": "3",
        },
    ]
    assert solutions == unordered(expected)


def test_find_solution_relationship_chain_fork_2(
    fact_collection_4: FactCollection,
):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->"
        "(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_4)
    expected = [
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_4",
            "n": "4",
            "o": "5",
        },
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_2",
            "n": "4",
            "o": "3",
        },
        {
            "m": "2",
            "r": "relationship_1",
            "s": "relationship_4",
            "n": "1",
            "o": "5",
        },
        {
            "m": "2",
            "r": "relationship_1",
            "s": "relationship_2",
            "n": "1",
            "o": "3",
        },
    ]
    assert solutions == unordered(expected)


def test_constraint_relationship_chain_with_node_attribute():
    cypher = (
        "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    constraint1 = ConstraintRelationshipHasSourceNode(
        source_node_name="n", relationship_name="r"
    )
    constraint2 = ConstraintRelationshipHasTargetNode(
        target_node_name="m", relationship_name="r"
    )
    constraint3 = ConstraintRelationshipHasSourceNode(
        source_node_name="m", relationship_name="s"
    )
    constraint4 = ConstraintRelationshipHasTargetNode(
        target_node_name="o", relationship_name="s"
    )
    constraint5 = ConstraintRelationshipHasLabel(
        relationship_name="r", label="MyRelationship"
    )
    constraint6 = ConstraintRelationshipHasLabel(
        relationship_name="s", label="OtherRelationship"
    )
    constraint7 = ConstraintNodeHasLabel(node_id="n", label="Thing")
    constraint8 = ConstraintNodeHasLabel(node_id="m", label="MiddleThing")
    constraint9 = ConstraintNodeHasLabel(node_id="o", label="OtherThing")
    constraint10 = ConstraintNodeHasAttributeWithValue(
        node_id="n", attribute="foo", value=Literal(2)
    )
    assert constraint1 in result.parsed.cypher.match_clause.constraints
    assert constraint2 in result.parsed.cypher.match_clause.constraints
    assert constraint3 in result.parsed.cypher.match_clause.constraints
    assert constraint4 in result.parsed.cypher.match_clause.constraints
    assert constraint5 in result.parsed.cypher.match_clause.constraints
    assert constraint6 in result.parsed.cypher.match_clause.constraints
    assert constraint7 in result.parsed.cypher.match_clause.constraints
    assert constraint8 in result.parsed.cypher.match_clause.constraints
    assert constraint9 in result.parsed.cypher.match_clause.constraints
    assert constraint10 in result.parsed.cypher.match_clause.constraints


def test_find_no_solution_relationship_chain_fork_missing_node_attribute(
    fact_collection_4: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_4)
    assert not solutions


def test_find_two_solutions_relationship_chain_fork_require_node_attribute_value(
    fact_collection_5: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_5)
    expected = [
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_4",
            "n": "4",
            "o": "5",
        },
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_2",
            "n": "4",
            "o": "3",
        },
    ]
    assert solutions == unordered(expected)


def test_find_no_solutions_relationship_chain_fork_node_attribute_value_wrong_type(
    fact_collection_5: FactCollection,
):
    cypher = (
        'MATCH (n:Thing {foo: "2"})-[r:MyRelationship]->(m:MiddleThing)-'
        "[s:OtherRelationship]->(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_5)
    assert not solutions


def test_find_two_solutions_relationship_chain_fork_red_herring_node(
    fact_collection_6: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_6)
    expected = [
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_4",
            "n": "4",
            "o": "5",
        },
        {
            "m": "2",
            "r": "relationship_3",
            "s": "relationship_2",
            "n": "4",
            "o": "3",
        },
    ]
    assert solutions == unordered(expected)


def test_find_no_solutions_relationship_chain_fork_node_attribute_value_wrong_type_red_herring_node(
    fact_collection_6: FactCollection,
):
    cypher = (
        """MATCH (n:Thing {foo: "2"})-[r:MyRelationship]->(m:MiddleThing)-"""
        """[s:OtherRelationship]->(o:OtherThing) """
        """RETURN n.foobar"""
    )
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_6)
    assert not solutions


def test_networkx_shim_initialization(networkx_graph):
    nxshim = NetworkX(networkx_graph)
    assert nxshim.graph
    assert isinstance(nxshim.graph, nx.DiGraph)


def test_nx_graph_to_fact_collection(networkx_graph):
    nxshim = NetworkX(networkx_graph)
    fact_collection = nxshim.make_fact_collection()
    assert fact_collection.facts


def test_parser_creates_with_clause():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(obj.parsed.cypher.match_clause.with_clause, WithClause)


def test_parser_creates_with_clause_object_as_series():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.match_clause.with_clause.object_as_series,
        ObjectAsSeries,
    )


def test_parser_creates_with_clause_object_as_series_members():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.match_clause.with_clause.object_as_series.object_attribute_lookup_list,
        list,
    )
    assert (
        len(
            obj.parsed.cypher.match_clause.with_clause.object_as_series.object_attribute_lookup_list
        )
        == 2
    )


def test_parser_creates_with_clause_object_as_series_members_are_alias():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.match_clause.with_clause.object_as_series.object_attribute_lookup_list[
            0
        ],
        Alias,
    )
    assert isinstance(
        obj.parsed.cypher.match_clause.with_clause.object_as_series.object_attribute_lookup_list[
            1
        ],
        Alias,
    )


def test_parser_creates_with_clause_object_alias_has_lookup():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.match_clause.with_clause.object_as_series.object_attribute_lookup_list[
            0
        ].reference,
        ObjectAttributeLookup,
    )
    assert isinstance(
        obj.parsed.cypher.match_clause.with_clause.object_as_series.object_attribute_lookup_list[
            0
        ].alias,
        str,
    )


def test_parser_creates_with_clause_object_alias_correct_value():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"""
    obj = CypherParser(query)
    assert (
        obj.parsed.cypher.match_clause.with_clause.object_as_series.object_attribute_lookup_list[
            0
        ].alias
        == "bar"
    )


def test_parser_creates_with_clause_single_element():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(obj.parsed.cypher.match_clause.with_clause, WithClause)


def test_parser_handles_collect_aggregation_in_return():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert (
        obj.parsed.cypher.return_clause.projection.lookups[0].alias
        == "whatever"
    )


def test_parser_handles_aggregation_in_return():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert (
        obj.parsed.cypher.return_clause.projection.lookups[0].alias
        == "whatever"
    )


def test_parser_handles_collect_in_aggregation_in_return():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.return_clause.projection.lookups[
            0
        ].reference.aggregation,
        Collect,
    )


def test_parser_handles_collect_in_aggregation_in_with_clause():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH COLLECT(n.foo) AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.return_clause.projection.lookups[
            0
        ].reference.aggregation,
        Collect,
    )


def test_parser_handles_collect_in_aggregation_in_with_clause_node_only():
    query = (
        """MATCH (n:Thingy) """
        """WITH COLLECT(n.foo) AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.return_clause.projection.lookups[
            0
        ].reference.aggregation,
        Collect,
    )


def test_parser_handles_collect_in_aggregation_in_return_twice():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """RETURN COLLECT(n.foobar) AS whatever, m.whatever AS bazqux"""
    )
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.return_clause.projection.lookups[
            0
        ].reference.aggregation,
        Collect,
    )
    assert isinstance(
        obj.parsed.cypher.return_clause.projection.lookups[1].reference,
        ObjectAttributeLookup,
    )


def test_parser_handles_with_where_clause_where_class():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert isinstance(obj.parsed.cypher.match_clause.where, Where)


def test_parser_handles_with_where_clause_with_class():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert isinstance(obj.parsed.cypher.match_clause.with_clause, WithClause)


def test_nodes_have_parent():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert all(
        node.parent is not None
        for node in obj.parsed.walk()
        if isinstance(node, TreeMixin)
    )


def test_child_of_parent_is_self():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert all(
        node in list(node.parent.children)
        for node in obj.parsed.walk()
        if isinstance(node, TreeMixin)
    )


def test_root_node_defined_everywhere():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert all(
        node.root is obj.parsed
        for node in obj.parsed.walk()
        if isinstance(node, TreeMixin)
    )


def test_node_in_match_clause_has_match_clause_enclosing():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    match_node = obj.parsed.cypher.match_clause
    assert all(
        node.enclosing_class(Match) is match_node
        for node in match_node.walk()
        if isinstance(node, TreeMixin)
    )


def test_error_on_no_enclosing_class():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH n.foo AS bar """
        """WHERE n.whatever = "thing" """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    with pytest.raises(ValueError):
        obj.parsed.cypher.return_clause.enclosing_class(Match)


def test_evaluate_literal_string():
    literal = Literal("thing")
    assert literal.evaluate(None) == "thing"


def test_evaluate_literal_int():
    literal = Literal(5)
    assert literal.evaluate(None) == 5


def test_evaluate_literal_float():
    literal = Literal(5.0)
    assert literal.evaluate(None) == 5.0


def test_evaluate_literal_bool():
    literal = Literal(True)
    assert literal.evaluate(None) is True


def test_evaluate_literal_none():
    literal = Literal(None)
    assert literal.evaluate(None) is None


def test_literals_are_evaluable():
    literal = Literal("thing")
    assert isinstance(literal, Evaluable)


def test_evaluate_literals_evaluate_equal_strings():
    literal1 = Literal("thing")
    literal2 = Literal("thing")
    assert Equals(literal1, literal2).evaluate(None)


def test_evaluate_literals_evaluate_equal_integer():
    literal1 = Literal(5)
    literal2 = Literal(5)
    assert Equals(literal1, literal2).evaluate(None)


def test_evaluate_literals_evaluate_not_equal_strings():
    literal1 = Literal("thing")
    literal2 = Literal("thingy")
    assert not Equals(literal1, literal2).evaluate(None)


def test_evaluate_literals_evaluate_greater_than_integer():
    literal1 = Literal(6)
    literal2 = Literal(5)
    assert GreaterThan(literal1, literal2).evaluate(None)


def test_evaluate_literals_evaluate_not_greater_than_integer():
    literal1 = Literal(6)
    literal2 = Literal(5)
    assert not GreaterThan(literal2, literal1).evaluate(None)


def test_evaluate_literals_evaluate_less_than_integer():
    literal1 = Literal(6)
    literal2 = Literal(5)
    assert not LessThan(literal1, literal2).evaluate(None)


def test_evaluate_literals_evaluate_not_less_than_integer():
    literal1 = Literal(6)
    literal2 = Literal(5)
    assert LessThan(literal2, literal1).evaluate(None)


def test_evaluate_addition_integers():
    literal1 = Literal(6)
    literal2 = Literal(5)
    assert Addition(literal1, literal2).evaluate(None) == 11


def test_evaluate_division_integers():
    literal1 = Literal(6)
    literal2 = Literal(3)
    assert Division(literal1, literal2).evaluate(None) == 2


def test_evaluating_division_returns_float():
    literal1 = Literal(6)
    literal2 = Literal(3)
    assert isinstance(Division(literal1, literal2).evaluate(None), float)


def test_evaluate_subtraction_integers():
    literal1 = Literal(6)
    literal2 = Literal(5)
    assert Subtraction(literal1, literal2).evaluate(None) == 1


def test_cannot_evaluate_addition_strings():
    literal1 = Literal("thing")
    literal2 = Literal("thing")
    with pytest.raises(Exception):
        Addition(literal1, literal2).evaluate(None)


def test_evaluate_nested_addition():
    literal1 = Literal(6)
    literal2 = Literal(5)
    literal3 = Literal(4)
    assert (
        Addition(Addition(literal1, literal2), literal3).evaluate(None) == 15
    )


def test_cannot_evaluate_addition_strings_right_side():
    literal1 = Literal(1)
    literal2 = Literal("thing")
    with pytest.raises(Exception):
        Addition(literal1, literal2).evaluate(None)


def test_evaluate_nested_subtraction():
    literal1 = Literal(6)
    literal2 = Literal(5)
    literal3 = Literal(4)
    assert (
        Subtraction(Subtraction(literal1, literal2), literal3).evaluate(None)
        == -3
    )


def test_evaluate_nested_addition_multiplication():
    literal1 = Literal(6)
    literal2 = Literal(5)
    literal3 = Literal(4)
    assert (
        Addition(Multiplication(literal1, literal2), literal3).evaluate(None)
        == 34
    )


def test_cannot_evaluate_addition_strings_left_side():
    literal1 = Literal("thing")
    literal2 = Literal(1)
    with pytest.raises(Exception):
        Addition(literal1, literal2).evaluate(None)


def test_refuse_to_divide_by_zero():
    literal1 = Literal(1)
    literal2 = Literal(0)
    with pytest.raises(WrongCypherTypeError):
        Division(literal1, literal2).evaluate(None)


def test_refuse_to_divide_by_zero_both():
    literal1 = Literal(0)
    literal2 = Literal(0)
    with pytest.raises(WrongCypherTypeError):
        Division(literal1, literal2).evaluate(None)


def test_evaluate_boolean_and_both_true():
    literal1 = Literal(True)
    literal2 = Literal(True)
    assert And(literal1, literal2).evaluate(None)


def test_evaluate_boolean_and_both_false():
    literal1 = Literal(False)
    literal2 = Literal(False)
    assert not And(literal1, literal2).evaluate(None)


def test_evaluate_boolean_and_one_false():
    literal1 = Literal(False)
    literal2 = Literal(True)
    assert not And(literal1, literal2).evaluate(None)


def test_evaluate_boolean_or_both_true():
    literal1 = Literal(True)
    literal2 = Literal(True)
    assert Or(literal1, literal2).evaluate(None)


def test_evaluate_boolean_or_one_true():
    literal1 = Literal(False)
    literal2 = Literal(True)
    assert Or(literal1, literal2).evaluate(None)


def test_evaluate_boolean_or_both_false():
    literal1 = Literal(False)
    literal2 = Literal(False)
    assert not Or(literal1, literal2).evaluate(None)


def test_evaluate_boolean_not_true():
    literal = Literal(True)
    assert not Not(literal).evaluate(None)


def test_evaluate_boolean_not_false():
    literal = Literal(False)
    assert Not(literal).evaluate(None)


def test_double_negation():
    literal = Literal(True)
    assert Not(Not(literal)).evaluate(None)


def test_evaluate_boolean_not_not_true():
    literal = Literal(False)
    assert not Not(Not(literal)).evaluate(None)


def test_evaluate_boolean_and_both_true_negated():
    literal1 = Literal(True)
    literal2 = Literal(True)
    assert not Not(And(literal1, literal2)).evaluate(None)


def test_evaluate_boolean_and_both_false_negated():
    literal1 = Literal(False)
    literal2 = Literal(False)
    assert Not(And(literal1, literal2)).evaluate(None)


def test_evaluate_boolean_and_one_false_negated():
    literal1 = Literal(False)
    literal2 = Literal(True)
    assert Not(And(literal1, literal2)).evaluate(None)


def test_evaluate_boolean_or_both_true_negated():
    literal1 = Literal(True)
    literal2 = Literal(True)
    assert not Not(Or(literal1, literal2)).evaluate(None)


def test_evaluate_boolean_or_one_true_negated():
    literal1 = Literal(False)
    literal2 = Literal(True)
    assert not Not(Or(literal1, literal2)).evaluate(None)


def test_evaluate_boolean_or_both_false_negated():
    literal1 = Literal(False)
    literal2 = Literal(False)
    assert Not(Or(literal1, literal2)).evaluate(None)


def test_evaluate_demorgan_law_both_true():
    literal1 = Literal(True)
    literal2 = Literal(True)
    assert Equals(
        Not(And(literal1, literal2)), Or(Not(literal1), Not(literal2))
    ).evaluate(None)


def test_evaluate_demorgan_law_left_true():
    literal1 = Literal(True)
    literal2 = Literal(False)
    assert Equals(
        Not(And(literal1, literal2)), Or(Not(literal1), Not(literal2))
    ).evaluate(None)


def test_evaluate_demorgan_law_right_true():
    literal1 = Literal(False)
    literal2 = Literal(True)
    assert Equals(
        Not(And(literal1, literal2)), Or(Not(literal1), Not(literal2))
    ).evaluate(None)


def test_evaluate_demorgan_law_both_false():
    literal1 = Literal(False)
    literal2 = Literal(False)
    assert Equals(
        Not(And(literal1, literal2)), Or(Not(literal1), Not(literal2))
    ).evaluate(None)


def test_enumerate_fact_types(fact_collection_6):
    facts = [fact for fact in fact_collection_6.node_has_label_facts()]
    assert len(facts) == 6


def test_query_node_has_attribute_with_value(fact_collection_6):
    query = QueryValueOfNodeAttribute(node_id="4", attribute="foo")
    value = fact_collection_6.query(query)
    assert value.evaluate(fact_collection_6) == 2


def test_query_node_has_non_existent_attribute(fact_collection_6):
    query = QueryValueOfNodeAttribute(node_id="4", attribute="bar")
    with pytest.raises(ValueError):
        fact_collection_6.query(query)


def test_query_non_existent_node_has_attribute_raises_error(fact_collection_6):
    query = QueryValueOfNodeAttribute(node_id="idontexist", attribute="foo")
    with pytest.raises(ValueError):
        fact_collection_6.query(query)


def test_object_attribute_lookup_evaluate(fact_collection_6):
    lookup = ObjectAttributeLookup(object_name="n", attribute="foo")
    assert lookup.evaluate(fact_collection_6, projection={"n": "4"}) == 2


def test_object_attribute_lookup_non_existent_object_raises_error(
    fact_collection_6,
):
    with pytest.raises(ValueError):
        ObjectAttributeLookup(object_name="n", attribute="foo").evaluate(
            fact_collection_6, projection={"n": "idontexist"}
        )


def test_object_attribute_lookup_in_addition(fact_collection_6):
    lookup = ObjectAttributeLookup(object_name="n", attribute="foo")
    literal = Literal(3)
    assert (
        Addition(lookup, literal).evaluate(
            fact_collection_6, projection={"n": "4"}
        )
        == 5
    )


def test_object_attribute_lookup_greater_than(fact_collection_6):
    lookup = ObjectAttributeLookup(object_name="n", attribute="foo")
    literal = Literal(1)
    assert GreaterThan(lookup, literal).evaluate(
        fact_collection_6, projection={"n": "4"}
    )


def test_object_attribute_lookup_greater_than_false(fact_collection_6):
    lookup = ObjectAttributeLookup(object_name="n", attribute="foo")
    literal = Literal(10)
    assert Not(GreaterThan(lookup, literal)).evaluate(
        fact_collection_6, projection={"n": "4"}
    )


def test_object_attribute_lookup_greater_than_double_negation(
    fact_collection_6,
):
    lookup = ObjectAttributeLookup(object_name="n", attribute="foo")
    literal = Literal(10)
    assert not Not(Not(GreaterThan(lookup, literal))).evaluate(
        fact_collection_6, projection={"n": "4"}
    )


def test_nonexistent_attribute_nested_evaluation_raises_error(
    fact_collection_6,
):
    lookup = ObjectAttributeLookup(object_name="n", attribute="idontexist")
    literal = Literal(10)
    with pytest.raises(ValueError):
        Not(Not(GreaterThan(lookup, literal))).evaluate(
            fact_collection_6, projection={"n": "4"}
        )


def test_collect_aggregated_variables_in_with_clause():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH COLLECT(n.foo) AS thingy, m.qux AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert obj.parsed.cypher.match_clause.with_clause.aggregated_variables == [
        "n"
    ]


def test_collect_all_variables_in_with_clause():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH COLLECT(n.foo) AS thingy, m.qux AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert sorted(
        obj.parsed.cypher.match_clause.with_clause.all_variables
    ) == ["m", "n"]


def test_collect_non_aggregated_variables_in_with_clause():
    query = (
        """MATCH (n:Thingy)-[r:Thingy]->(m) """
        """WITH COLLECT(n.foo) AS thingy, m.qux AS bar """
        """RETURN COLLECT(n.foobar) AS whatever"""
    )
    obj = CypherParser(query)
    assert (
        obj.parsed.cypher.match_clause.with_clause.non_aggregated_variables
        == ["m"]
    )


def test_unique_non_aggregated_variable_solutions_one_aggregation():
    solutions = [
        {"n": "1", "m": "2", "o": "3"},
        {"n": "2", "m": "2", "o": "3"},
        {"n": "3", "m": "2", "o": "3"},
    ]
    non_aggregated_variables = ["m", "o"]
    unique_solutions = WithClause._unique_non_aggregated_variable_solutions(
        solutions, non_aggregated_variables
    )
    assert unique_solutions == [{"m": "2", "o": "3"}]


def test_unique_non_aggregated_variable_solutions_one_aggregation_complex():
    solutions = [
        {"n": "1", "m": "2", "o": "3"},
        {"n": "2", "m": "2", "o": "3"},
        {"n": "3", "m": "2", "o": "3"},
        {"n": "4", "m": "1", "o": "3"},
        {"n": "5", "m": "1", "o": "3"},
    ]
    non_aggregated_variables = ["m", "o"]
    unique_solutions = WithClause._unique_non_aggregated_variable_solutions(
        solutions, non_aggregated_variables
    )
    assert unique_solutions == [
        {"m": "2", "o": "3"},
        {"m": "1", "o": "3"},
    ]


def test_unique_non_aggregated_variable_solutions_two_aggregations_complex():
    solutions = [
        {"n": "1", "m": "2", "o": "3"},
        {"n": "2", "m": "2", "o": "3"},
        {"n": "3", "m": "2", "o": "3"},
        {"n": "4", "m": "1", "o": "3"},
        {"n": "5", "m": "1", "o": "3"},
    ]
    non_aggregated_variables = ["m"]
    unique_solutions = WithClause._unique_non_aggregated_variable_solutions(
        solutions, non_aggregated_variables
    )
    assert unique_solutions == [
        {"m": "2"},
        {"m": "1"},
    ]


def test_transform_solutions_by_aggregations():
    solutions = [
        {"n": "1", "m": "2", "o": "3"},
        {"n": "2", "m": "2", "o": "3"},
        {"n": "3", "m": "2", "o": "3"},
        {"n": "4", "m": "1", "o": "3"},
        {"n": "5", "m": "1", "o": "3"},
    ]
    aggregated_variables = ["n"]
    non_aggregated_variables = ["m", "o"]
    transformed_solutions = WithClause._transform_solutions_by_aggregations(
        solutions, aggregated_variables, non_aggregated_variables
    )
    assert transformed_solutions == [
        {
            "n": [
                "1",
                "2",
                "3",
            ],
            "m": "2",
            "o": "3",
        },
        {
            "n": [
                "4",
                "5",
            ],
            "m": "1",
            "o": "3",
        },
    ]


def test_transform_solutions_in_with_clause(
    fact_collection_6: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]"
        "->(o:OtherThing) "
        "WITH COLLECT(o) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    aggregated_results = result.\
    parsed.\
    cypher.\
    match_clause.\
    with_clause.\
    transform_solutions_by_aggregations(
        fact_collection_6
    )
    expected_results = [
        {
            "n": "4",
            "m": "2",
            "o": ["5", "3"],
        }
    ]

    assert aggregated_results == expected_results


def test_transform_solutions_in_with_clause_no_solutions(
    fact_collection_6: FactCollection,
):
    cypher = (
        "MATCH (n:Thing {foo: 37})-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "WITH COLLECT(o) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    aggregated_results = result.parsed.cypher.match_clause.with_clause.\
        transform_solutions_by_aggregations(
            fact_collection_6
    )
    expected_results = []

    assert aggregated_results == expected_results


def test_transform_solutions_in_with_clause_multiple_solutions(
    fact_collection_6: FactCollection,
):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->"
        "(o:OtherThing) "
        "WITH COLLECT(o) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    aggregated_results = result.parsed.cypher.match_clause.with_clause.\
        transform_solutions_by_aggregations(
            fact_collection_6
    )
    expected_results = [
        {"n": "4", "m": "2", "o": ["5", "3"]},
        {"n": "1", "m": "2", "o": ["5", "3"]},
    ]

    assert aggregated_results == expected_results


def test_apply_substitutions_to_one_projection(
    fact_collection_7: FactCollection,
):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "WITH COLLECT(o.oattr) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)

    projection = {"n": "4", "m": "2", "o": ["5", "3"]}

    out = (
        result.parsed.cypher.match_clause.with_clause._evaluate_one_projection(
            fact_collection_7,
            projection=projection,
        )
    )
    expected_result = {
        "co": Collection([Literal(5), Literal(4)]),
        "nfoo": Literal(2),
        "mbar": Literal(3),
    }
    assert out == expected_result


def test_apply_substitutions_to_projection_list(
    fact_collection_7: FactCollection,
):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "WITH COLLECT(o.oattr) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)

    solutions = [
        {"n": "4", "m": "2", "o": ["5", "3"]},
        {"n": "1", "m": "2", "o": ["5", "3"]},
    ]

    out = result.parsed.cypher.match_clause.with_clause._evaluate(
        fact_collection_7,
        projection=solutions,
    )
    expected_result = [
        {
            "co": Collection([Literal(5), Literal(4)]),
            "nfoo": Literal(2),
            "mbar": Literal(3),
        },
        {
            "co": Collection([Literal(5), Literal(4)]),
            "nfoo": Literal(42),
            "mbar": Literal(3),
        },
    ]

    assert out == expected_result


def test_distinct_evaluation_removes_duplicates():
    assert Distinct(
        Collection([Literal(1), Literal(2), Literal(2)])
    )._evaluate(None) == Collection([Literal(1), Literal(2)])


def test_distinct_evaluation_removes_nothing_if_no_duplicates():
    assert Distinct(
        Collection([Literal(1), Literal(2), Literal(3)])
    )._evaluate(None) == Collection([Literal(1), Literal(2), Literal(3)])


def test_distinct_evaluation_removes_nothing_if_different_types():
    assert Distinct(
        Collection([Literal(1), Literal(2), Literal("2")])
    )._evaluate(None) == Collection([Literal(1), Literal(2), Literal("2")])


def test_size_of_list():
    assert Size(Collection([Literal(1), Literal(2), Literal("2")]))._evaluate(
        None
    ) == Literal(3)


def test_size_of_empty_list_is_zero():
    assert Size(Collection([]))._evaluate(None) == Literal(0)


def test_size_around_distinct():
    assert Size(
        Distinct(Collection([Literal(1), Literal(2), Literal(2)]))
    )._evaluate(None) == Literal(2)


def test_parse_distinct_keyword_with_collect_no_dups(fact_collection_7):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "WITH DISTINCT COLLECT(o.oattr) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = [
        {"n": "4", "m": "2", "o": ["5", "3"]},
        {"n": "1", "m": "2", "o": ["5", "3"]},
    ]
    out = result.parsed.cypher.match_clause.with_clause._evaluate(
        fact_collection_7,
        projection=solutions,
    )
    assert out == [
        {
            "co": Collection([Literal(5), Literal(4)]),
            "nfoo": Literal(2),
            "mbar": Literal(3),
        },
        {
            "co": Collection([Literal(5), Literal(4)]),
            "nfoo": Literal(42),
            "mbar": Literal(3),
        },
    ]


def test_parse_distinct_keyword_with_collect_one_dup(fact_collection_7):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "WITH DISTINCT COLLECT(o.oattr) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN n.foobar"
    )
    result = CypherParser(cypher)
    solutions = [
        {"n": "4", "m": "2", "o": ["5", "5"]},
        {"n": "1", "m": "2", "o": ["5", "3"]},
    ]
    out = result.parsed.cypher.match_clause.with_clause._evaluate(
        fact_collection_7,
        projection=solutions,
    )
    assert out == [
        {
            "co": Collection([Literal(5)]),
            "nfoo": Literal(2),
            "mbar": Literal(3),
        },
        {
            "co": Collection([Literal(5), Literal(4)]),
            "nfoo": Literal(42),
            "mbar": Literal(3),
        },
    ]


def test_evaluate_return_after_with_clause(fact_collection_7):
    cypher = (
        "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-"
        "[s:OtherRelationship]->(o:OtherThing) "
        "WITH DISTINCT COLLECT(o.oattr) AS co, n.foo AS nfoo, m.bar AS mbar "
        "RETURN nfoo, co"
    )
    result = CypherParser(cypher)
    expected = [
        {"nfoo": Literal(2), "co": Collection([Literal(5), Literal(4)])},
        {"nfoo": Literal(42), "co": Collection([Literal(5), Literal(4)])},
    ]
    out = result.parsed.cypher.return_clause._evaluate(
        fact_collection_7, projection=None
    )
    assert out == expected


def test_tree_mixing_get_parse_object():
    query = "MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar, m.baz AS qux RETURN n.foobar"
    obj = CypherParser(query)
    assert obj.parsed.cypher.match_clause.pattern.parse_obj is obj.parsed


def test_collection_children():
    # Create a mock Evaluable object
    mock_evaluable = Collection(values=[])

    # Create a Collection instance with the mock evaluable object
    collection = Collection(values=[mock_evaluable])

    # Get the children of the collection
    children = list(collection.children)

    # Assert that the children list contains the mock evaluable object
    assert children == [mock_evaluable]


def test_distinct_children():
    # Create a mock Collection object
    mock_collection = Collection(values=[])

    # Create a Distinct instance with the mock collection
    distinct = Distinct(collection=mock_collection)

    # Get the children of the distinct instance
    children = list(distinct.children)

    # Assert that the children list contains the mock collection
    assert children == [mock_collection]


def test_size_children():
    # Create a mock Collection object
    mock_collection = Collection(values=[])

    # Create a Size instance with the mock collection
    size = Size(collection=mock_collection)

    # Get the children of the size instance
    children = list(size.children)

    # Assert that the children list contains the mock collection
    assert children == [mock_collection]


def test_distinct_collect_children():
    # Create a mock ObjectAttributeLookup object
    mock_object_attribute_lookup = ObjectAttributeLookup(
        "object_name", "attribute_name"
    )

    # Create a Collect instance with the mock object attribute lookup
    collect = Collect(object_attribute_lookup=mock_object_attribute_lookup)

    # Get the children of the collect instance
    children = list(collect.children)

    # Assert that the children list contains the mock object attribute lookup
    assert children == [mock_object_attribute_lookup]


def test_cypher_collection_children():
    # Create a mock Evaluable object
    mock_evaluable = Collection(values=[])

    # Create a Collection instance with the mock evaluable object
    collection = Collection(values=[mock_evaluable])

    # Get the children of the collection
    children = list(collection.children)

    # Assert that the children list contains the mock evaluable object
    assert children == [mock_evaluable]


def test_cypher_distinct_children():
    # Create a mock Collection object
    mock_collection = Collection(values=[])

    # Create a Distinct instance with the mock collection
    distinct = Distinct(collection=mock_collection)

    # Get the children of the distinct instance
    children = list(distinct.children)

    # Assert that the children list contains the mock collection
    assert children == [mock_collection]


def test_cypher_size_children():
    # Create a mock Collection object
    mock_collection = Collection(values=[])

    # Create a Size instance with the mock collection
    size = Size(collection=mock_collection)

    # Get the children of the size instance
    children = list(size.children)

    # Assert that the children list contains the mock collection
    assert children == [mock_collection]


def test_cypher_collect_children():
    # Create a mock ObjectAttributeLookup object
    mock_object_attribute_lookup = ObjectAttributeLookup(
        "object_name", "attribute_name"
    )

    # Create a Collect instance with the mock object attribute lookup
    collect = Collect(object_attribute_lookup=mock_object_attribute_lookup)

    # Get the children of the collect instance
    children = list(collect.children)

    # Assert that the children list contains the mock object attribute lookup
    assert children == [mock_object_attribute_lookup]


def test_aggregation_children():
    # Create a mock Evaluable object
    mock_evaluable = Collection(values=[])

    # Create an Aggregation instance with the mock evaluable object
    aggregation = Aggregation(aggregation=mock_evaluable)

    # Get the children of the aggregation instance
    children = list(aggregation.children)

    # Assert that the children list contains the mock evaluable object
    assert children == [mock_evaluable]


def test_collect_evaluate():
    # Create a mock ObjectAttributeLookup object
    mock_object_attribute_lookup = ObjectAttributeLookup(
        "object_name", "attribute_name"
    )

    # Create a Collect instance with the mock object attribute lookup
    collect = Collect(object_attribute_lookup=mock_object_attribute_lookup)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"object_name": ["instance1", "instance2"]}

    # Mock the _evaluate method of ObjectAttributeLookup
    def mock_evaluate(_, projection):
        return Literal(projection["object_name"])

    mock_object_attribute_lookup._evaluate = mock_evaluate

    # Evaluate the collect instance
    result = collect._evaluate(
        mock_fact_collection, projection=mock_projection
    )

    # Assert that the result is a Collection with the expected values
    assert isinstance(result, Collection)
    assert result.values == [Literal("instance1"), Literal("instance2")]


def test_collect_evaluate_empty_projection():
    # Create a mock ObjectAttributeLookup object
    mock_object_attribute_lookup = ObjectAttributeLookup(
        "object_name", "attribute_name"
    )

    # Create a Collect instance with the mock object attribute lookup
    collect = Collect(object_attribute_lookup=mock_object_attribute_lookup)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create an empty mock projection
    mock_projection = {"object_name": []}

    # Mock the _evaluate method of ObjectAttributeLookup
    def mock_evaluate(_, projection):
        return Literal(projection["object_name"])

    mock_object_attribute_lookup._evaluate = mock_evaluate

    # Evaluate the collect instance
    result = collect._evaluate(
        mock_fact_collection, projection=mock_projection
    )

    # Assert that the result is a Collection with no values
    assert isinstance(result, Collection)
    assert result.values == []


def test_collect_evaluate_with_projection():
    # Create a mock ObjectAttributeLookup object
    mock_object_attribute_lookup = ObjectAttributeLookup(
        "object_name", "attribute_name"
    )

    # Create a Collect instance with the mock object attribute lookup
    collect = Collect(object_attribute_lookup=mock_object_attribute_lookup)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"object_name": ["instance1", "instance2"]}

    # Mock the _evaluate method of ObjectAttributeLookup
    def mock_evaluate(_, projection):
        return Literal(projection["object_name"])

    mock_object_attribute_lookup._evaluate = mock_evaluate

    # Evaluate the collect instance
    result = collect._evaluate(
        mock_fact_collection, projection=mock_projection
    )

    # Assert that the result is a Collection with the expected values
    assert isinstance(result, Collection)
    assert result.values == [Literal("instance1"), Literal("instance2")]


def test_query_children():
    # Create a mock Collection object
    mock_collection = Collection(values=[])

    # Create a Cypher instance with the mock collection
    cypher = Cypher(cypher=mock_collection)

    # Get the children of the Cypher instance
    children = list(cypher.children)

    # Assert that the children list contains the mock collection
    assert children == [mock_collection]


def test_trigger_gather_constraints_to_match():
    query = (
        """MATCH (n:Thing {key1: "value", key2: 5})-[r]->(m:OtherThing {key3: "hithere"}) """
        """WHERE n.key = 2, n.foo = 3 """
        """RETURN n.foobar, n.baz"""
    )
    obj = CypherParser(query)
    obj.parsed.trigger_gather_constraints_to_match()
    assert len(obj.parsed.cypher.match_clause.constraints) == 18  # check this


def test_trigger_gather_constraints_to_match_no_match():
    class MockCypher(Cypher):  # pylint: disable=too-few-public-methods,missing-class-docstring
        def walk(self):
            return [Collection(values=[]), Collection(values=[])]

    # Create a mock Cypher instance
    mock_cypher = MockCypher(cypher=Collection(values=[]))

    # Trigger the gathering of constraints
    mock_cypher.trigger_gather_constraints_to_match()

    # Check that no constraints were gathered since there are no Match instances
    for node in mock_cypher.walk():
        if isinstance(node, Collection):
            assert not hasattr(node, "constraints_gathered")


def test_match_children_with_all_attributes():
    # Create mock objects for pattern, where, and with_clause
    mock_pattern = Collection(values=[])
    mock_where = Collection(values=[])
    mock_with_clause = Collection(values=[])

    # Create a Match instance with all attributes
    match = Match(
        pattern=mock_pattern, where=mock_where, with_clause=mock_with_clause
    )

    # Get the children of the match instance
    children = list(match.children)

    # Assert that the children list contains the mock pattern, where, and with_clause
    assert children == [mock_pattern, mock_where, mock_with_clause]


def test_match_children_with_pattern_only():
    # Create a mock object for pattern
    mock_pattern = Collection(values=[])

    # Create a Match instance with only the pattern attribute
    match = Match(pattern=mock_pattern, where=None, with_clause=None)

    # Get the children of the match instance
    children = list(match.children)

    # Assert that the children list contains only the mock pattern
    assert children == [mock_pattern]


def test_match_children_with_pattern_and_where():
    # Create mock objects for pattern and where
    mock_pattern = Collection(values=[])
    mock_where = Collection(values=[])

    # Create a Match instance with pattern and where attributes
    match = Match(pattern=mock_pattern, where=mock_where, with_clause=None)

    # Get the children of the match instance
    children = list(match.children)

    # Assert that the children list contains the mock pattern and where
    assert children == [mock_pattern, mock_where]


def test_match_children_with_pattern_and_with_clause():
    # Create mock objects for pattern and with_clause
    mock_pattern = Collection(values=[])
    mock_with_clause = Collection(values=[])

    # Create a Match instance with pattern and with_clause attributes
    match = Match(
        pattern=mock_pattern, where=None, with_clause=mock_with_clause
    )

    # Get the children of the match instance
    children = list(match.children)

    # Assert that the children list contains the mock pattern and with_clause
    assert children == [mock_pattern, mock_with_clause]


def test_return_children():
    # Create a mock Projection object
    mock_projection = Projection(lookups=[])

    # Create a Return instance with the mock projection
    return_clause = Return(node=mock_projection)

    # Get the children of the return instance
    children = list(return_clause.children)

    # Assert that the children list contains the mock projection
    assert children == [mock_projection]


def test_projection_children():
    # Create mock ObjectAttributeLookup objects
    mock_lookup1 = ObjectAttributeLookup("object1", "attribute1")
    mock_lookup2 = ObjectAttributeLookup("object2", "attribute2")

    # Create a Projection instance with the mock lookups
    projection = Projection(lookups=[mock_lookup1, mock_lookup2])

    # Get the children of the projection instance
    children = list(projection.children)

    # Assert that the children list contains the mock lookups
    assert children == [mock_lookup1, mock_lookup2]


def test_projection_children_empty():
    # Create a Projection instance with no lookups
    projection = Projection(lookups=[])

    # Get the children of the projection instance
    children = list(projection.children)

    # Assert that the children list is empty
    assert not children


def test_node_constraints_with_label():
    # Create a mock NodeNameLabel with a label
    mock_node_name_label = NodeNameLabel(name="node1", label="Label1")

    # Create a Node instance with the mock NodeNameLabel
    node = Node(node_name_label=mock_node_name_label)

    # Get the constraints of the node
    constraints = node.constraints

    # Assert that the constraints list contains the expected ConstraintNodeHasLabel
    assert len(constraints) == 1
    assert isinstance(constraints[0], ConstraintNodeHasLabel)
    assert constraints[0].node_id == "node1"
    assert constraints[0].label == "Label1"


def test_node_constraints_without_label():
    # Create a mock NodeNameLabel without a label
    mock_node_name_label = NodeNameLabel(name="node1", label=None)

    # Create a Node instance with the mock NodeNameLabel
    node = Node(node_name_label=mock_node_name_label)

    # Get the constraints of the node
    constraints = node.constraints

    # Assert that the constraints list is empty
    assert len(constraints) == 0


def test_evaluate_with_projection():
    # Create a mock Projection object with lookups
    mock_lookup1 = ObjectAttributeLookup("object1", "attribute1")
    mock_lookup2 = ObjectAttributeLookup("object2", "attribute2")
    mock_projection = Projection(lookups=[mock_lookup1, mock_lookup2])

    # Create a Return instance with the mock projection
    return_clause = Return(node=mock_projection)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection_data = [
        {"object1": "value1", "object2": "value2"},
        {"object1": "value3", "object2": "value4"},
    ]

    # Mock the _evaluate method of the parent match_clause.with_clause
    class MockWithClause:  # pylint: disable=too-few-public-methods,missing-class-docstring
        def _evaluate(self, *args, **kwargs):  # pylint: disable=unused-argument
            return mock_projection_data

    class MockMatchClause:  # pylint: disable=too-few-public-methods,missing-class-docstring
        with_clause = MockWithClause()

    class MockParent:  # pylint: disable=too-few-public-methods,missing-class-docstring
        match_clause = MockMatchClause()

    return_clause.parent = MockParent()

    # Evaluate the return instance
    result = return_clause._evaluate(mock_fact_collection, projection=None)

    # Assert that the result is as expected
    expected_result = [
        {"object1": "value1", "object2": "value2"},
        {"object1": "value3", "object2": "value4"},
    ]
    assert result == expected_result


def test_evaluate_with_given_projection():
    # Create a mock Projection object with lookups
    mock_lookup1 = ObjectAttributeLookup("object1", "attribute1")
    mock_lookup2 = ObjectAttributeLookup("object2", "attribute2")
    mock_projection = Projection(lookups=[mock_lookup1, mock_lookup2])

    # Create a Return instance with the mock projection
    return_clause = Return(node=mock_projection)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a given projection
    given_projection = [
        {"object1": "value1", "object2": "value2"},
        {"object1": "value3", "object2": "value4"},
    ]

    # Evaluate the return instance with the given projection
    result = return_clause._evaluate(
        mock_fact_collection, projection=given_projection
    )

    # Assert that the result is as expected
    expected_result = [
        {"object1": "value1", "object2": "value2"},
        {"object1": "value3", "object2": "value4"},
    ]
    assert result == expected_result


def test_node_name_label_children_with_label():
    # Create a NodeNameLabel instance with a name and label
    node_name_label = NodeNameLabel(name="node1", label="Label1")

    # Get the children of the NodeNameLabel instance
    children = list(node_name_label.children)

    # Assert that the children list contains the name and label
    assert children == ["node1", "Label1"]


def test_node_name_label_children_without_label():
    # Create a NodeNameLabel instance with a name and no label
    node_name_label = NodeNameLabel(name="node1", label=None)

    # Get the children of the NodeNameLabel instance
    children = list(node_name_label.children)

    # Assert that the children list contains only the name
    assert children == ["node1"]


@pytest.mark.skip
def test_node_children_with_node_name_label_and_mappings():
    # Create a mock NodeNameLabel
    mock_node_name_label = NodeNameLabel(name="node1", label="Label1")

    # Create mock Mapping objects
    mock_mapping1 = Mapping(key="key1", value="value1")
    mock_mapping2 = Mapping(key="key2", value="value2")

    # Create a Node instance with the mock NodeNameLabel and mappings
    node = Node(
        node_name_label=mock_node_name_label,
        mapping_list=[mock_mapping1, mock_mapping2],
    )

    # Get the children of the node instance
    children = list(node.children)

    # Assert that the children list contains the mock NodeNameLabel and mappings
    assert children == [mock_node_name_label, mock_mapping1, mock_mapping2]


def test_node_children_with_node_name_label_only():
    # Create a mock NodeNameLabel
    mock_node_name_label = NodeNameLabel(name="node1", label="Label1")

    # Create a Node instance with the mock NodeNameLabel and no mappings
    node = Node(node_name_label=mock_node_name_label, mapping_list=[])

    # Get the children of the node instance
    children = list(node.children)

    # Assert that the children list contains only the mock NodeNameLabel
    assert children == [mock_node_name_label]


@pytest.mark.skip
def test_node_children_with_mappings_only():
    # Create mock Mapping objects
    mock_mapping1 = Mapping(key="key1", value="value1")
    mock_mapping2 = Mapping(key="key2", value="value2")

    # Create a Node instance with no NodeNameLabel and the mock mappings
    node = Node(
        node_name_label=None, mapping_list=[mock_mapping1, mock_mapping2]
    )

    # Get the children of the node instance
    children = list(node.children)

    # Assert that the children list contains only the mock mappings
    assert children == [mock_mapping1, mock_mapping2]


def test_node_children_empty():
    # Create a Node instance with no NodeNameLabel and no mappings
    node = Node(node_name_label=None, mapping_list=[])

    # Get the children of the node instance
    children = list(node.children)

    # Assert that the children list is empty
    assert not children


def test_relationship_children():
    # Create a mock NodeNameLabel
    mock_name_label = NodeNameLabel(name="relationship1", label="Label1")

    # Create a Relationship instance with the mock NodeNameLabel
    relationship = Relationship(name_label=mock_name_label)

    # Get the children of the relationship instance
    children = list(relationship.children)

    # Assert that the children list contains the mock NodeNameLabel
    assert children == [mock_name_label]


def test_relationship_chain_children():
    # Create mock Relationship objects
    mock_relationship1 = Relationship(
        name_label=NodeNameLabel(name="relationship1", label="Label1")
    )
    mock_relationship2 = Relationship(
        name_label=NodeNameLabel(name="relationship2", label="Label2")
    )

    # Create a RelationshipChain instance with the mock relationships
    relationship_chain = RelationshipChain(
        steps=[mock_relationship1, mock_relationship2]
    )

    # Get the children of the relationship chain instance
    children = list(relationship_chain.children)

    # Assert that the children list contains the mock relationships
    assert children == [mock_relationship1, mock_relationship2]


def test_relationship_left_right_children():
    # Create a mock Relationship object
    mock_relationship = Relationship(
        name_label=NodeNameLabel(name="relationship1", label="Label1")
    )

    # Create a RelationshipLeftRight instance with the mock relationship
    relationship_left_right = RelationshipLeftRight(
        relationship=mock_relationship
    )

    # Get the children of the relationship left-right instance
    children = list(relationship_left_right.children)

    # Assert that the children list contains the mock relationship
    assert children == [mock_relationship]


def test_relationship_right_left_children():
    # Create a mock Relationship object
    mock_relationship = Relationship(
        name_label=NodeNameLabel(name="relationship1", label="Label1")
    )

    # Create a RelationshipRightLeft instance with the mock relationship
    relationship_right_left = RelationshipRightLeft(
        relationship=mock_relationship
    )

    # Get the children of the relationship right-left instance
    children = list(relationship_right_left.children)

    # Assert that the children list contains the mock relationship
    assert children == [mock_relationship]


def test_mapping_set_children():
    # Create mock Mapping objects
    mock_mapping1 = Mapping(key="key1", value="value1")
    mock_mapping2 = Mapping(key="key2", value="value2")

    # Create a MappingSet instance with the mock mappings
    mapping_set = MappingSet(mappings=[mock_mapping1, mock_mapping2])

    # Get the children of the mapping set instance
    children = list(mapping_set.children)

    # Assert that the children list contains the mock mappings
    assert children == [mock_mapping1, mock_mapping2]


def test_mapping_set_children_empty():
    # Create a MappingSet instance with no mappings
    mapping_set = MappingSet(mappings=[])

    # Get the children of the mapping set instance
    children = list(mapping_set.children)

    # Assert that the children list is empty
    assert not children


def test_relationship_chain_list_children():
    # Create mock RelationshipChain objects
    mock_relationship_chain1 = RelationshipChain(steps=[])
    mock_relationship_chain2 = RelationshipChain(steps=[])

    # Create a RelationshipChainList instance with the mock relationship chains
    relationship_chain_list = RelationshipChainList(
        relationships=[mock_relationship_chain1, mock_relationship_chain2]
    )

    # Get the children of the relationship chain list instance
    children = list(relationship_chain_list.children)

    # Assert that the children list contains the mock relationship chains
    assert children == [mock_relationship_chain1, mock_relationship_chain2]


def test_addition_children():
    # Create mock TreeMixin objects
    mock_left = Literal(value=1)
    mock_right = Literal(value=2)

    # Create an Addition instance with the mock left and right operands
    addition = Addition(left=mock_left, right=mock_right)

    # Get the children of the addition instance
    children = list(addition.children)

    # Assert that the children list contains the mock left and right operands
    assert children == [mock_left, mock_right]


def test_where_children():
    # Create a mock Predicate object
    class MockPredicate(Predicate):  # pylint: disable=missing-class-docstring
        def tree(self):
            return "Predicate Tree"

    mock_predicate = MockPredicate(
        left_side=Literal(value=1), right_side=Literal(value=2)
    )

    # Create a Where instance with the mock predicate
    where_clause = Where(predicate=mock_predicate)

    # Get the children of the where instance
    children = list(where_clause.children)

    # Assert that the children list contains the mock predicate
    assert children == [mock_predicate]


def test_aliased_name_children():
    # Create an AliasedName instance with a name
    aliased_name = AliasedName(name="alias")

    # Get the children of the AliasedName instance
    children = list(aliased_name.children)

    # Assert that the children list contains the name
    assert children == ["alias"]


def test_evaluate_equals():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=5)

    # Create an Equals instance with the mock literals
    equals = Equals(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the equals instance
    result = equals._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value True
    assert isinstance(result, Literal)
    assert result.value is True


def test_evaluate_equals_false():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create an Equals instance with the mock literals
    equals = Equals(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the equals instance
    result = equals._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value False
    assert isinstance(result, Literal)
    assert result.value is False


def test_evaluate_equals_with_projection():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=5)

    # Create an Equals instance with the mock literals
    equals = Equals(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the equals instance with the projection
    result = equals._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value True
    assert isinstance(result, Literal)
    assert result.value is True


def test_evaluate_equals_with_projection_false():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create an Equals instance with the mock literals
    equals = Equals(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the equals instance with the projection
    result = equals._evaluate(mock_fact_collection, projection=mock_projection)

    # Assert that the result is a Literal with the value False
    assert isinstance(result, Literal)
    assert result.value is False


def test_evaluate_less_than():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a LessThan instance with the mock literals
    less_than = LessThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the less_than instance
    result = less_than._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value True
    assert isinstance(result, Literal)
    assert result.value is True


def test_evaluate_less_than_false():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=5)

    # Create a LessThan instance with the mock literals
    less_than = LessThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the less_than instance
    result = less_than._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value False
    assert isinstance(result, Literal)
    assert result.value is False


def test_evaluate_less_than_with_projection():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a LessThan instance with the mock literals
    less_than = LessThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the less_than instance with the projection
    result = less_than._evaluate(
        mock_fact_collection, projection=mock_projection
    )

    # Assert that the result is a Literal with the value True
    assert isinstance(result, Literal)
    assert result.value is True


def test_evaluate_less_than_with_projection_false():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=5)

    # Create a LessThan instance with the mock literals
    less_than = LessThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the less_than instance with the projection
    result = less_than._evaluate(
        mock_fact_collection, projection=mock_projection
    )

    # Assert that the result is a Literal with the value False
    assert isinstance(result, Literal)
    assert result.value is False


def test_evaluate_greater_than():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=5)

    # Create a GreaterThan instance with the mock literals
    greater_than = GreaterThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the greater_than instance
    result = greater_than._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value True
    assert isinstance(result, Literal)
    assert result.value is True


def test_evaluate_greater_than_false():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a GreaterThan instance with the mock literals
    greater_than = GreaterThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the greater_than instance
    result = greater_than._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value False
    assert isinstance(result, Literal)
    assert result.value is False


def test_evaluate_greater_than_with_projection():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=5)

    # Create a GreaterThan instance with the mock literals
    greater_than = GreaterThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the greater_than instance with the projection
    result = greater_than._evaluate(
        mock_fact_collection, projection=mock_projection
    )

    # Assert that the result is a Literal with the value True
    assert isinstance(result, Literal)
    assert result.value is True


def test_evaluate_greater_than_with_projection_false():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a GreaterThan instance with the mock literals
    greater_than = GreaterThan(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the greater_than instance with the projection
    result = greater_than._evaluate(
        mock_fact_collection, projection=mock_projection
    )

    # Assert that the result is a Literal with the value False
    assert isinstance(result, Literal)
    assert result.value is False


def test_evaluate_subtraction():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=5)

    # Create a Subtraction instance with the mock literals
    subtraction = Subtraction(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the subtraction instance
    result = subtraction._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value 5
    assert isinstance(result, Literal)
    assert result.value == 5


def test_evaluate_subtraction_negative_result():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a Subtraction instance with the mock literals
    subtraction = Subtraction(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the subtraction instance
    result = subtraction._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value -5
    assert isinstance(result, Literal)
    assert result.value == -5


def test_evaluate_subtraction_with_projection():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=5)

    # Create a Subtraction instance with the mock literals
    subtraction = Subtraction(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the subtraction instance with the projection
    result = subtraction._evaluate(
        mock_fact_collection, projection=mock_projection
    )

    # Assert that the result is a Literal with the value 5
    assert isinstance(result, Literal)
    assert result.value == 5


def test_evaluate_subtraction_with_projection_negative_result():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a Subtraction instance with the mock literals
    subtraction = Subtraction(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the subtraction instance with the projection
    result = subtraction._evaluate(
        mock_fact_collection, projection=mock_projection
    )

    # Assert that the result is a Literal with the value -5
    assert isinstance(result, Literal)
    assert result.value == -5


def test_evaluate_multiplication():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a Multiplication instance with the mock literals
    multiplication = Multiplication(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the multiplication instance
    result = multiplication._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value 50
    assert isinstance(result, Literal)
    assert result.value == 50


def test_evaluate_multiplication_with_projection():
    # Create mock Literal objects
    mock_left = Literal(value=5)
    mock_right = Literal(value=10)

    # Create a Multiplication instance with the mock literals
    multiplication = Multiplication(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the multiplication instance with the projection
    result = multiplication._evaluate(
        mock_fact_collection, projection=mock_projection
    )

    # Assert that the result is a Literal with the value 50
    assert isinstance(result, Literal)
    assert result.value == 50


def test_evaluate_multiplication_negative():
    # Create mock Literal objects
    mock_left = Literal(value=-5)
    mock_right = Literal(value=10)

    # Create a Multiplication instance with the mock literals
    multiplication = Multiplication(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the multiplication instance
    result = multiplication._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value -50
    assert isinstance(result, Literal)
    assert result.value == -50


def test_evaluate_multiplication_zero():
    # Create mock Literal objects
    mock_left = Literal(value=0)
    mock_right = Literal(value=10)

    # Create a Multiplication instance with the mock literals
    multiplication = Multiplication(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the multiplication instance
    result = multiplication._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value 0
    assert isinstance(result, Literal)
    assert result.value == 0


def test_evaluate_multiplication_with_projection_negative():
    # Create mock Literal objects
    mock_left = Literal(value=-5)
    mock_right = Literal(value=10)

    # Create a Multiplication instance with the mock literals
    multiplication = Multiplication(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the multiplication instance with the projection
    result = multiplication._evaluate(
        mock_fact_collection, projection=mock_projection
    )

    # Assert that the result is a Literal with the value -50
    assert isinstance(result, Literal)
    assert result.value == -50


def test_evaluate_division():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=2)

    # Create a Division instance with the mock literals
    division = Division(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the division instance
    result = division._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value 5
    assert isinstance(result, Literal)
    assert result.value == 5


def test_evaluate_division_with_projection():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=2)

    # Create a Division instance with the mock literals
    division = Division(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the division instance with the projection
    result = division._evaluate(
        mock_fact_collection, projection=mock_projection
    )  # pylint: disable=protected-access

    # Assert that the result is a Literal with the value 5
    assert isinstance(result, Literal)
    assert result.value == 5


def test_evaluate_division_by_zero():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=0)

    # Create a Division instance with the mock literals
    division = Division(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # # Evaluate the division instance and expect an exception
    # with pytest.raises(ZeroDivisionError):
    with pytest.raises(WrongCypherTypeError):
        division._evaluate(mock_fact_collection)  # pylint: disable=protected-access


def test_evaluate_division_negative():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=-2)

    # Create a Division instance with the mock literals
    division = Division(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Evaluate the division instance
    result = division._evaluate(mock_fact_collection)

    # Assert that the result is a Literal with the value -5
    assert isinstance(result, Literal)
    assert result.value == -5


def test_evaluate_division_with_projection_negative():
    # Create mock Literal objects
    mock_left = Literal(value=10)
    mock_right = Literal(value=-2)

    # Create a Division instance with the mock literals
    division = Division(left_side=mock_left, right_side=mock_right)

    # Create a mock FactCollection
    mock_fact_collection = FactCollection([])

    # Create a mock projection
    mock_projection = {"key": "value"}

    # Evaluate the division instance with the projection
    result = division._evaluate(
        mock_fact_collection, projection=mock_projection
    )

    # Assert that the result is a Literal with the value -5
    assert isinstance(result, Literal)
    assert result.value == -5


def test_is_true_eq_same_predicate():
    predicate = Mock()
    constraint1 = IsTrue(predicate)
    constraint2 = IsTrue(predicate)
    assert constraint1 == constraint2


def test_is_true_eq_different_predicate():
    predicate1 = Mock()
    predicate2 = Mock()
    constraint1 = IsTrue(predicate1)
    constraint2 = IsTrue(predicate2)
    assert constraint1 != constraint2


def test_is_true_eq_different_type():
    predicate = Mock()
    constraint = IsTrue(predicate)
    assert constraint != "not a constraint"
