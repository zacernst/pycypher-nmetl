from typing import Any, Dict, List

from pycypher.cypher_parser import CypherParser


def cypher_condition(cypher: str) -> Any:
    def inner_decorator(f: Any) -> Any:
        cypher_parser = CypherParser(cypher)
        import pdb

        pdb.set_trace()

        def wrapped(*args: List[Any], **kwargs: Dict[Any, Any]):
            return f(*args, **kwargs)

        return wrapped

    return inner_decorator
