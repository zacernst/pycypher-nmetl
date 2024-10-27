from unittest.mock import patch

import pytest

from pycypher.cypher import (
    Alias,
    Cypher,
    CypherParser,
    Equals,
    Literal,
    ObjectAttributeLookup,
    Query,
    Where,
    NodeHasLabel,
    NodeHasAttributeWithValue,
    NodeRelatedToNode,
    FactCollection,
)


@pytest.fixture
def fact_collection():
    fact1 = NodeHasLabel("1", "Thing")
    fact2 = NodeHasAttributeWithValue("1", "key", 2)
    fact3 = NodeRelatedToNode("1", "2", "MyRelationship")
    fact4 = NodeHasLabel("2", "OtherThing")
    fact5 = NodeHasAttributeWithValue("2", "key", 5)
    fact_collection = FactCollection([fact1, fact2, fact3, fact4, fact5])

    return fact_collection


def test_fact_collection_has_facts(fact_collection: FactCollection):
    assert len(fact_collection) == 5


@pytest.mark.cypher
def test_can_parse_simple_cypher():
    obj = CypherParser("MATCH (n) RETURN n.foo")
    assert isinstance(obj, CypherParser)


@pytest.mark.cypher
def test_parser_builds_cypher_object():
    obj = CypherParser("MATCH (n) RETURN n.foo")
    assert isinstance(obj.parsed, Cypher)


@pytest.mark.cypher
def test_parser_creates_simple_node_object():
    obj = CypherParser("MATCH (n) RETURN n.foo")
    assert isinstance(obj.parsed.cypher, Query)


@pytest.mark.cypher
def test_parser_parses_complicated_query():
    query = (
        """MATCH (n:Thing {key1: "value", key2: 5})-[r]->(m:OtherThing {key3: "hithere"}) """
        """WHERE n.key = 2, n.foo = 3 """
        """RETURN n.foobar, n.baz"""
    )
    obj = CypherParser(query)
    assert isinstance(obj.parsed, Cypher)


@pytest.mark.cypher
def test_parser_handles_node_label():
    query = """MATCH (n:Thingy)-[r]->(m) RETURN n.foobar"""
    obj = CypherParser(query)
    assert (
        obj.parsed.cypher.match_clause.pattern.relationships[0]
        .steps[0]
        .node_name_label.label
        == "Thingy"
    )


@pytest.mark.cypher
def test_parser_handles_where_clause():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(obj.parsed.cypher.match_clause.where, Where)


@pytest.mark.cypher
def test_parser_handles_where_clause_predicate():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(obj.parsed.cypher.match_clause.where.predicate, Equals)


@pytest.mark.cypher
def test_parser_handles_where_clause_predicate_lookup():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.match_clause.where.predicate.left_side,
        ObjectAttributeLookup,
    )


@pytest.mark.cypher
def test_parser_handles_where_clause_predicate_literal():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.match_clause.where.predicate.right_side, Literal
    )


@pytest.mark.cypher
def test_parser_generates_alias_in_return_statement():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar AS myfoobar"""
    obj = CypherParser(query)
    assert isinstance(
        obj.parsed.cypher.return_clause.projection.lookups[0], Alias
    )


@pytest.mark.cypher
def test_parser_generates_alias_with_correct_name_in_return_statement():
    query = """MATCH (n:Thingy) WHERE n.foo = 5 RETURN n.foobar AS myfoobar"""
    obj = CypherParser(query)
    assert (
        obj.parsed.cypher.return_clause.projection.lookups[0].alias
        == "myfoobar"
    )
