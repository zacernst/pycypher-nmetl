from pycypher.cypher_parser import CypherParser
from pycypher.fact import (
    FactCollection,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactNodeRelatedToNode,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)

fact1 = FactNodeHasLabel("1", "Thing")
fact2 = FactNodeHasAttributeWithValue("1", "key", 2)
fact3 = FactNodeRelatedToNode("1", "2", "MyRelationship")
fact4 = FactNodeHasLabel("2", "OtherThing")
fact5 = FactNodeHasAttributeWithValue("2", "key", 5)
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

# cypher = "MATCH (n:Thing) RETURN n.foobar"
# result = CypherParser(cypher)
# solutions = result.solutions(fact_collection)

# Variable "Thing" hasn't been assigned to a domain


cypher = "MATCH (n:Thing)-[r:MyRelationship]->(m:OtherThing) RETURN n.foobar"
result = CypherParser(cypher)
solutions = result.solutions(fact_collection)
expected = [{"n": "1", "m": "2", "r": "relationship_123"}]
assert solutions == expected
