"""testing"""

from __future__ import annotations

from pycypher.cypher_parser import CypherParser
from pycypher.reactive_fact_collection import ReactiveFactCollection

if __name__ == "__main__":  # pragma: no cover # typing:ignore
    test_fact_collection = ReactiveFactCollection(facts=[])

    @test_fact_collection.trigger(
        "MATCH (n:Thingy)-[r:Relationship]->(m:Foobar) WITH n.foo AS nfoo RETURN nfoo"
    )
    def my_function(nfoo):  # typing: ignore
        print(f"Function called with {nfoo}")

    result = CypherParser(
        "MATCH (n:Thingy)-[r:Relationship]->(m:Foobar) WITH n.foo AS nfoo RETURN nfoo"
    )
    result.parsed.print_tree()
