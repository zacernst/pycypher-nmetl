# pylint: disable=missing-function-docstring,redefined-outer-name,too-many-lines

from unittest.mock import patch

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
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from pycypher.node_classes import Alias  # pylint: disable=unused-import
from pycypher.node_classes import (
    Addition,
    And,
    Collect,
    Cypher,
    Division,
    Equals,
    Evaluable,
    GreaterThan,
    LessThan,
    Literal,
    Match,
    Multiplication,
    Not,
    ObjectAsSeries,
    ObjectAttributeLookup,
    Or,
    Query,
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


@pytest.fixture
def fact_collection_0():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasAttributeWithValue("1", "key", Literal(2))
    fact3 = FactNodeRelatedToNode("1", "2", "MyRelationship")
    fact4 = FactNodeHasLabel("2", "OtherThing")
    fact5 = FactNodeHasAttributeWithValue("2", "key", Literal(5))
    fact6 = FactRelationshipHasLabel("relationship_123", "MyRelationship")
    fact7 = FactRelationshipHasSourceNode("relationship_123", "1")
    fact8 = FactRelationshipHasTargetNode("relationship_123", "2")
    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_1():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "Thing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_2():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "MiddleThing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact4 = FactRelationshipHasLabel("relationship_1", "MyRelationship")
    fact5 = FactRelationshipHasLabel("relationship_2", "OtherRelationship")
    fact6 = FactRelationshipHasSourceNode("relationship_1", "1")
    fact7 = FactRelationshipHasTargetNode("relationship_1", "2")
    fact8 = FactRelationshipHasSourceNode("relationship_2", "2")
    fact9 = FactRelationshipHasTargetNode("relationship_2", "3")
    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
            fact9,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_3():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "MiddleThing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact4 = FactRelationshipHasLabel("relationship_1", "MyRelationship")
    fact5 = FactRelationshipHasLabel("relationship_2", "OtherRelationship")
    fact6 = FactRelationshipHasSourceNode("relationship_1", "1")
    fact7 = FactRelationshipHasTargetNode("relationship_1", "2")
    fact8 = FactRelationshipHasSourceNode("relationship_2", "2")
    fact9 = FactRelationshipHasTargetNode("relationship_2", "3")
    fact10 = FactNodeHasLabel("4", "Thing")
    fact11 = FactRelationshipHasLabel("relationship_3", "MyRelationship")
    fact12 = FactRelationshipHasSourceNode("relationship_3", "4")
    fact13 = FactRelationshipHasTargetNode("relationship_3", "2")

    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
            fact9,
            fact10,
            fact11,
            fact12,
            fact13,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_4():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "MiddleThing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact4 = FactRelationshipHasLabel("relationship_1", "MyRelationship")
    fact5 = FactRelationshipHasLabel("relationship_2", "OtherRelationship")
    fact6 = FactRelationshipHasSourceNode("relationship_1", "1")
    fact7 = FactRelationshipHasTargetNode("relationship_1", "2")
    fact8 = FactRelationshipHasSourceNode("relationship_2", "2")
    fact9 = FactRelationshipHasTargetNode("relationship_2", "3")

    fact10 = FactNodeHasLabel("4", "Thing")
    fact11 = FactRelationshipHasLabel("relationship_3", "MyRelationship")
    fact12 = FactRelationshipHasSourceNode("relationship_3", "4")
    fact13 = FactRelationshipHasTargetNode("relationship_3", "2")

    fact14 = FactNodeHasLabel("5", "OtherThing")
    fact15 = FactRelationshipHasLabel("relationship_4", "OtherRelationship")
    fact16 = FactRelationshipHasSourceNode("relationship_4", "2")
    fact17 = FactRelationshipHasTargetNode("relationship_4", "5")

    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
            fact9,
            fact10,
            fact11,
            fact12,
            fact13,
            fact14,
            fact15,
            fact16,
            fact17,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_5():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "MiddleThing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact4 = FactRelationshipHasLabel("relationship_1", "MyRelationship")
    fact5 = FactRelationshipHasLabel("relationship_2", "OtherRelationship")
    fact6 = FactRelationshipHasSourceNode("relationship_1", "1")
    fact7 = FactRelationshipHasTargetNode("relationship_1", "2")
    fact8 = FactRelationshipHasSourceNode("relationship_2", "2")
    fact9 = FactRelationshipHasTargetNode("relationship_2", "3")

    fact10 = FactNodeHasLabel("4", "Thing")
    fact11 = FactRelationshipHasLabel("relationship_3", "MyRelationship")
    fact12 = FactRelationshipHasSourceNode("relationship_3", "4")
    fact13 = FactRelationshipHasTargetNode("relationship_3", "2")

    fact14 = FactNodeHasLabel("5", "OtherThing")
    fact15 = FactRelationshipHasLabel("relationship_4", "OtherRelationship")
    fact16 = FactRelationshipHasSourceNode("relationship_4", "2")
    fact17 = FactRelationshipHasTargetNode("relationship_4", "5")

    fact18 = FactNodeHasAttributeWithValue("4", "foo", Literal(2))

    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
            fact9,
            fact10,
            fact11,
            fact12,
            fact13,
            fact14,
            fact15,
            fact16,
            fact17,
            fact18,
        ]
    )

    return fact_collection


@pytest.fixture
def fact_collection_6():  # pylint: disable=too-many-locals
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasLabel("2", "MiddleThing")
    fact3 = FactNodeHasLabel("3", "OtherThing")
    fact4 = FactRelationshipHasLabel("relationship_1", "MyRelationship")
    fact5 = FactRelationshipHasLabel("relationship_2", "OtherRelationship")
    fact6 = FactRelationshipHasSourceNode("relationship_1", "1")
    fact7 = FactRelationshipHasTargetNode("relationship_1", "2")
    fact8 = FactRelationshipHasSourceNode("relationship_2", "2")
    fact9 = FactRelationshipHasTargetNode("relationship_2", "3")

    fact10 = FactNodeHasLabel("4", "Thing")
    fact11 = FactRelationshipHasLabel("relationship_3", "MyRelationship")
    fact12 = FactRelationshipHasSourceNode("relationship_3", "4")
    fact13 = FactRelationshipHasTargetNode("relationship_3", "2")

    fact14 = FactNodeHasLabel("5", "OtherThing")
    fact15 = FactRelationshipHasLabel("relationship_4", "OtherRelationship")
    fact16 = FactRelationshipHasSourceNode("relationship_4", "2")
    fact17 = FactRelationshipHasTargetNode("relationship_4", "5")

    fact18 = FactNodeHasAttributeWithValue("4", "foo", Literal(2))

    fact19 = FactNodeHasLabel("6", "Irrelevant")

    fact_collection = FactCollection(
        [
            fact1,
            fact2,
            fact3,
            fact4,
            fact5,
            fact6,
            fact7,
            fact8,
            fact9,
            fact10,
            fact11,
            fact12,
            fact13,
            fact14,
            fact15,
            fact16,
            fact17,
            fact18,
            fact19,
        ]
    )

    return fact_collection


@pytest.fixture
def networkx_graph():
    edge_dictionary = {
        "a": ["b", "c", "d", "e"],
        "b": ["a", "e"],
        "c": ["a", "d"],
        "d": ["a", "c"],
        "e": ["a", "b"],
        "f": ["g"],
        "g": ["f"],
    }

    graph = nx.DiGraph(edge_dictionary)

    graph.nodes["a"]["name"] = "Alice"
    graph.nodes["b"]["name"] = "Bob"
    graph.nodes["c"]["name"] = "Charlie"
    graph.nodes["d"]["name"] = "David"
    graph.nodes["e"]["name"] = "Eve"
    graph.nodes["f"]["name"] = "Frank"
    graph.nodes["g"]["name"] = "Grace"

    graph.nodes["a"]["age"] = 25
    graph.nodes["b"]["age"] = 30
    graph.nodes["c"]["age"] = 35
    graph.nodes["d"]["age"] = 40
    graph.nodes["e"]["age"] = 45
    graph.nodes["f"]["age"] = 50
    graph.nodes["g"]["age"] = 55

    graph.nodes["a"]["category"] = "Person"
    graph.nodes["b"]["category"] = "Person"
    graph.nodes["c"]["category"] = "Person"
    graph.nodes["d"]["category"] = "Person"
    graph.nodes["e"]["category"] = "Person"
    graph.nodes["f"]["category"] = "Person"
    graph.nodes["g"]["category"] = "Person"

    return graph


@pytest.fixture
def number_of_facts(fact_collection_0: FactCollection) -> int:
    return len(fact_collection_0.facts)


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
    cypher = "MATCH (n:Thing {key: 2})-[r:NoRelationshipLikeMeExists]->(m:OtherThing {key: 5}) RETURN n.foobar"
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
    cypher = "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->(o:OtherThing) RETURN n.foobar"
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
    cypher = "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing), (m)-[s:OtherRelationship]->(o:OtherThing) RETURN n.foobar"
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
    cypher = "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->(o:OtherThing) RETURN n.foobar"
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
    cypher = "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->(o:OtherThing) RETURN n.foobar"
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
    cypher = "MATCH (n:Thing)-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->(o:OtherThing) RETURN n.foobar"
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
    cypher = "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->(o:OtherThing) RETURN n.foobar"
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
    cypher = "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->(o:OtherThing) RETURN n.foobar"
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_4)
    assert not solutions


def test_find_two_solutions_relationship_chain_fork_require_node_attribute_value(
    fact_collection_5: FactCollection,
):
    cypher = "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->(o:OtherThing) RETURN n.foobar"
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
    cypher = 'MATCH (n:Thing {foo: "2"})-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->(o:OtherThing) RETURN n.foobar'
    result = CypherParser(cypher)
    solutions = result.parsed.cypher.match_clause.solutions(fact_collection_5)
    assert not solutions


def test_find_two_solutions_relationship_chain_fork_red_herring_node(
    fact_collection_6: FactCollection,
):
    cypher = "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->(o:OtherThing) RETURN n.foobar"
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
    cypher = 'MATCH (n:Thing {foo: "2"})-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->(o:OtherThing) RETURN n.foobar'
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
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    assert (
        obj.parsed.cypher.return_clause.projection.lookups[0].alias
        == "whatever"
    )


def test_parser_handles_aggregation_in_return():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    assert (
        obj.parsed.cypher.return_clause.projection.lookups[0].alias
        == "whatever"
    )


def test_parser_handles_collect_in_aggregation_in_return():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.return_clause.projection.lookups[
            0
        ].reference.aggregation,
        Collect,
    )


def test_parser_handles_collect_in_aggregation_in_with_clause():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH COLLECT(n.foo) AS bar RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.return_clause.projection.lookups[
            0
        ].reference.aggregation,
        Collect,
    )


def test_parser_handles_collect_in_aggregation_in_with_clause_node_only():
    query = """MATCH (n:Thingy) WITH COLLECT(n.foo) AS bar WHERE n.whatever = "thing" RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.return_clause.projection.lookups[
            0
        ].reference.aggregation,
        Collect,
    )


def test_parser_handles_collect_in_aggregation_in_return_twice():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar RETURN COLLECT(n.foobar) AS whatever, m.whatever AS bazqux"""
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
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar WHERE n.whatever = "thing" RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    assert isinstance(obj.parsed.cypher.match_clause.where, Where)


def test_parser_handles_with_where_clause_with_class():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar WHERE n.whatever = "thing" RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    assert isinstance(obj.parsed.cypher.match_clause.with_clause, WithClause)


def test_nodes_have_parent():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar WHERE n.whatever = "thing" RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    assert all(
        node.parent is not None
        for node in obj.parsed.walk()
        if isinstance(node, TreeMixin)
    )


def test_child_of_parent_is_self():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar WHERE n.whatever = "thing" RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    assert all(
        node in list(node.parent.children)
        for node in obj.parsed.walk()
        if isinstance(node, TreeMixin)
    )


def test_root_node_defined_everywhere():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar WHERE n.whatever = "thing" RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    assert all(
        node.root is obj.parsed
        for node in obj.parsed.walk()
        if isinstance(node, TreeMixin)
    )


def test_node_in_match_clause_has_match_clause_enclosing():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar WHERE n.whatever = "thing" RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    match_node = obj.parsed.cypher.match_clause
    assert all(
        node.enclosing_class(Match) is match_node
        for node in match_node.walk()
        if isinstance(node, TreeMixin)
    )


def test_error_on_no_enclosing_class():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH n.foo AS bar WHERE n.whatever = "thing" RETURN COLLECT(n.foobar) AS whatever"""
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
    lookup = ObjectAttributeLookup(object_name="4", attribute="foo")
    assert lookup.evaluate(fact_collection_6) == 2


def test_object_attribute_lookup_non_existent_object_raises_error(
    fact_collection_6,
):
    with pytest.raises(ValueError):
        ObjectAttributeLookup(
            object_name="idontexist", attribute="foo"
        ).evaluate(fact_collection_6)


def test_object_attribute_lookup_in_addition(fact_collection_6):
    lookup = ObjectAttributeLookup(object_name="4", attribute="foo")
    literal = Literal(3)
    assert Addition(lookup, literal).evaluate(fact_collection_6) == 5


def test_object_attribute_lookup_greater_tan(fact_collection_6):
    lookup = ObjectAttributeLookup(object_name="4", attribute="foo")
    literal = Literal(1)
    assert GreaterThan(lookup, literal).evaluate(fact_collection_6)


def test_object_attribute_lookup_greater_than_false(fact_collection_6):
    lookup = ObjectAttributeLookup(object_name="4", attribute="foo")
    literal = Literal(10)
    assert Not(GreaterThan(lookup, literal)).evaluate(fact_collection_6)


def test_object_attribute_lookup_greater_than_double_negation(
    fact_collection_6,
):
    lookup = ObjectAttributeLookup(object_name="4", attribute="foo")
    literal = Literal(10)
    assert not Not(Not(GreaterThan(lookup, literal))).evaluate(
        fact_collection_6
    )


def test_nonexistent_attribute_nested_evaluation_raises_error(
    fact_collection_6,
):
    lookup = ObjectAttributeLookup(object_name="4", attribute="idontexist")
    literal = Literal(10)
    with pytest.raises(ValueError):
        Not(Not(GreaterThan(lookup, literal))).evaluate(fact_collection_6)


def test_collect_aggregated_variables_in_with_clause():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH COLLECT(n.foo) AS thingy, m.qux AS bar RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    assert obj.parsed.cypher.match_clause.with_clause.aggregated_variables == [
        "n"
    ]


def test_collect_all_variables_in_with_clause():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH COLLECT(n.foo) AS thingy, m.qux AS bar RETURN COLLECT(n.foobar) AS whatever"""
    obj = CypherParser(query)
    assert sorted(
        obj.parsed.cypher.match_clause.with_clause.all_variables
    ) == ["m", "n"]


def test_collect_non_aggregated_variables_in_with_clause():
    query = """MATCH (n:Thingy)-[r:Thingy]->(m) WITH COLLECT(n.foo) AS thingy, m.qux AS bar RETURN COLLECT(n.foobar) AS whatever"""
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
    # TODO: Start by testing the WithClause.transform... method
    #       This test is not right yet.
    cypher = "MATCH (n:Thing {foo: 2})-[r:MyRelationship]->(m:MiddleThing)-[s:OtherRelationship]->(o:OtherThing) WITH COLLECT(o) AS co, n AS n, m AS m  RETURN n.foobar"
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
