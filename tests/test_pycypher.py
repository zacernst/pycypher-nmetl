import pytest

from pycypher import CypherParser
from pycypher.fact import (
    FactCollection,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactNodeRelatedToNode,
)
from pycypher.node_classes import (
    Alias,
    Cypher,
    Equals,
    Literal,
    ObjectAttributeLookup,
    Query,
    Where,
)


@pytest.fixture
def fact_collection():
    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasAttributeWithValue("1", "key", 2)
    fact3 = FactNodeRelatedToNode("1", "2", "MyRelationship")
    fact4 = FactNodeHasLabel("2", "OtherThing")
    fact5 = FactNodeHasAttributeWithValue("2", "key", 5)
    fact_collection = FactCollection([fact1, fact2, fact3, fact4, fact5])

    return fact_collection


def test_fact_collection_has_facts(fact_collection: FactCollection):
    assert len(fact_collection) == 5


def test_fact_collection_del_item(fact_collection: FactCollection):
    del fact_collection[0]
    assert len(fact_collection) == 4


def test_fact_collection_set_item(fact_collection: FactCollection):
    fact = FactNodeHasLabel("3", "Thing")
    fact_collection[0] = fact
    assert len(fact_collection) == 5
    assert fact_collection[0] == fact


def test_fact_collection_get_item(fact_collection: FactCollection):
    fact = fact_collection[0]
    assert isinstance(fact, FactNodeHasLabel)


def test_fact_collection_insert(fact_collection: FactCollection):
    fact = FactNodeHasLabel("3", "Thing")
    fact_collection.insert(0, fact)
    assert len(fact_collection) == 6
    assert fact_collection[0] == fact


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
    constraints = result.parsed.aggregated_constraints
    assert len(constraints) == 1


def test_aggregate_constraints_node_and_mapping():
    cypher = "MATCH (m:Thing {key: 2}) RETURN m.foobar"
    result = CypherParser(cypher)
    constraints = result.parsed.aggregated_constraints
    assert len(constraints) == 2


def test_parse_anonymous_node_no_label_no_mapping():
    cypher = "MATCH () RETURN m.foobar"
    result = CypherParser(cypher)
