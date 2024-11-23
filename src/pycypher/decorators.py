from typing import Any, Dict, List

from pycypher.exceptions import UnexpectedCypherStructureError
from pycypher.logger import LOGGER
from pycypher.parser import CypherParser


def cypher_condition(cypher: str) -> Any:
    def inner_decorator(f: Any) -> Any:
        LOGGER.info(f"Wrapping function {f.__name__}...")
        cypher_parser = CypherParser(cypher)
        try:
            assert cypher_parser.parsed.cypher.match_clause is not None  # type: ignore
        except Exception as e:
            LOGGER.info(
                "Error in expected structure of `cypher_condition` argument"
            )
            raise UnexpectedCypherStructureError(e)

        def wrapped(*args: List[Any], **kwargs: Dict[Any, Any]):
            return f(*args, **kwargs)

        return wrapped

    return inner_decorator
