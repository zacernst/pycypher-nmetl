"""testing"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import md5
from typing import Any, Callable, Dict, List, Type

from pycypher.cypher_parser import CypherParser
from pycypher.fact import FactCollection
from pycypher.node_classes import Cypher


@dataclass
class CypherTrigger:
    """hi"""

    function: Callable
    cypher_string: str
    cypher: Cypher
    call_counter: int = 0
    error_counter: int = 0


class ReactiveFactCollection(FactCollection):
    """Adds trigger capabilities to the FactCollection class"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trigger_dict: Dict[str, CypherTrigger] = {}

    def trigger(self, cypher: str) -> Any:
        """Register the Cypher query."""
        cypher_parser = CypherParser(cypher)

        def inner_decorator(f: Callable) -> Any:
            def wrapped(*args: List[Any], **kwargs: Dict[Any, Any]):
                return f(*args, **kwargs)

            return wrapped

        self.trigger_dict[md5(cypher.encode()).hexdigest()] = CypherTrigger(
            function=inner_decorator,
            cypher_string=cypher,
            cypher=cypher_parser,
        )

        return inner_decorator

    @classmethod
    def init(
        cls, fact_collection_cls: Type, *args, **kwargs
    ) -> ReactiveFactCollection:
        """Initialise the class with a FactCollection class"""
        new_class = type(
            "ReactiveFactCollection",
            (cls,),
            {"fact_collection_cls": fact_collection_cls},
        )
        return new_class(*args, **kwargs)


if __name__ == "__main__":  # pragma: no cover # typing:ignore

    @test_fact_collection.trigger(
        "MATCH (n:Thingy)-[r:Relationship]->(m:Foobar) WITH n.foo AS nfoo RETURN nfoo"
    )
    def my_function(nfoo):  # pylint: disable=unused-argument
        print(f"Function called with {nfoo}")

    result = CypherParser(
        "MATCH (n:Thingy)-[r:Relationship]->(m:Foobar) WITH n.foo AS nfoo RETURN nfoo"
    )
    result.parsed.print_tree()
