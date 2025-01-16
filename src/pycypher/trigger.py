"""testing"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import md5
from typing import Iterable, List, Any, Callable, Dict, Generator, Optional, Set

from pycypher.cypher_parser import CypherParser
from pycypher.fact import AtomicFact, FactCollection
from pycypher.solver import Constraint


@dataclass
class CypherTrigger:
    """
    We check the ``Fact`` and ``Constraint`` objects to see if they
    indicate that the trigger should be fired.
    """

    def __init__(
        self,
        function: Optional[Callable] = None,
        cypher_string: Optional[str] = None,
    ):
        self.function = function
        self.cypher_string = cypher_string
        self.cypher = CypherParser(cypher_string)
        self.call_counter = 0
        self.error_counter = 0

        self.constraints: Set[Constraint] = set()

        self._gather_constraints()

    def _gather_constraints(self):
        for node in self.cypher.walk():
            if hasattr(node, "constraints"):
                self.constraints = self.constraints | set(node.constraints)


class Goldberg:
    """Holds the triggers and fact collection and makes everything go."""

    def __init__(
        self,
        trigger_dict: Optional[Dict[str, CypherTrigger]] = None,
        fact_collection: Optional[FactCollection] = None,
    ):
        self.trigger_dict = trigger_dict or {}
        self.fact_collection = fact_collection or FactCollection(facts=[])

    def attach_fact_collection(self, fact_collection: FactCollection) -> None:
        """Attach a ``FactCollection`` to the machine."""
        if not isinstance(fact_collection, FactCollection):
            raise ValueError(
                f"Expected a FactCollection, got {type(fact_collection)}"
            )
        self.fact_collection = fact_collection

    def walk_constraints(self) -> Generator[Constraint, None, None]:
        """Yield all the triggers' constraints."""
        for trigger in self.trigger_dict.values():
            yield from trigger.constraints
    
    @property
    def constraints(self) -> List[Constraint]:
        """Return all the constraints from the triggers."""
        constraints = list(i for i in self.walk_constraints())
        return constraints
    
    def facts_matching_constraints(self, fact_generator: Iterable) -> Generator[AtomicFact, None, None]:
        """Yield all the facts that match the constraints."""
        for fact in fact_generator:
            for constraint in self.constraints:
                if fact + constraint:
                    yield fact

    def __iadd__(
        self, other: CypherTrigger | FactCollection | AtomicFact
    ) -> Goldberg:
        """Add a CypherTrigger, FactCollection, or Fact to the machine."""
        if isinstance(other, CypherTrigger):
            self.register_trigger(other)
        elif isinstance(other, FactCollection):
            self.attach_fact_collection(other)
        elif isinstance(other, AtomicFact):
            self.fact_collection.append(other)
        else:
            raise ValueError(
                f"Expected a CypherTrigger, FactCollection, or Fact, got {type(other)}"
            )
        return self

    def trigger(self, cypher: str) -> None:
        """Register the Cypher query."""

        def inner_decorator(f: Callable) -> Any:
            return f

        trigger = CypherTrigger(function=inner_decorator, cypher_string=cypher)
        self.trigger_dict[md5(trigger.cypher_string.encode()).hexdigest()] = (
            trigger
        )

        return inner_decorator

    def cypher_trigger(self, arg1):
        """Decorator that registers a trigger with a Cypher string and a function."""

        def decorator(func):
            trigger = CypherTrigger(function=func, cypher_string=arg1)
            # self.trigger_dict[
            #     md5(trigger.cypher_string.encode()).hexdigest()
            # ] = trigger

            self.register_trigger(trigger)

            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)
                return result

            return wrapper

        return decorator

    def register_trigger(self, cypher_trigger: CypherTrigger) -> None:
        """
        Register a CypherTrigger with the machine.
        """
        self.trigger_dict[
            md5(cypher_trigger.cypher_string.encode()).hexdigest()
        ] = CypherTrigger(
            function=cypher_trigger.function,
            cypher_string=cypher_trigger.cypher_string,
        )


if __name__ == "__main__":  # pragma: no cover # typing:ignore
    pass
