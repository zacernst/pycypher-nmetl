"""Experimental. Decorators for registering trigger functions."""

from typing import Any, Dict, List


def cypher_condition(cypher: str) -> Any:
    """A decorator for registering a trigger function with a Cypher condition."""

    def inner_decorator(f: Any) -> Any:
        def wrapped(*args: List[Any], **kwargs: Dict[Any, Any]):
            return f(*args, **kwargs)

        return wrapped

    return inner_decorator
