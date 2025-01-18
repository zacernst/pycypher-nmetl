"""testing"""

from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass
from hashlib import md5
from typing import (
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Protocol,
    Set,
    TypeVar,
)

from pycypher.core.cypher_parser import CypherParser
from pycypher.etl.fact import AtomicFact, FactCollection
from pycypher.etl.solver import Constraint

Variable = TypeVar("Variable")
Attribute = TypeVar("Attribute")


class VariableAttribute(Protocol[Variable, Attribute]):
    """Protocol to be used in triggered functions as return signature."""

    def __getitem__(self, *args, **kwargs) -> None: ...

    def __setitem__(self, *args, **kwargs) -> None: ...


@dataclass
class CypherTrigger:  # pylint: disable=too-many-instance-attributes
    """
    We check the ``Fact`` and ``Constraint`` objects to see if they
    indicate that the trigger should be fired.
    """

    def __init__(
        self,
        function: Optional[Callable] = None,
        cypher_string: Optional[str] = None,
        variable_set: Optional[str] = None,
        attribute_set: Optional[str] = None,
        parameter_names: Optional[List[str]] = None,
    ):
        self.function = function
        self.cypher_string = cypher_string
        try:
            self.cypher = CypherParser(cypher_string)
        except Exception as e:
            raise ValueError(f"Error parsing Cypher string: {e}") from e
        self.call_counter = 0
        self.error_counter = 0
        self.variable_set: Optional[str] = variable_set
        self.attribute_set: Optional[str] = attribute_set
        self.parameter_names = parameter_names

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

    def facts_matching_constraints(
        self, fact_generator: Iterable
    ) -> Generator[AtomicFact, None, None]:
        """Yield all the facts that match the constraints."""
        for fact in fact_generator:
            for constraint in self.constraints:
                if sub := fact + constraint:
                    yield fact, constraint, sub

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

    def cypher_trigger(self, arg1):
        """Decorator that registers a trigger with a Cypher string and a function."""

        def decorator(func):
            @functools.wraps
            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)
                return result

            variable_attribute_annotation = inspect.signature(
                func
            ).return_annotation
            if variable_attribute_annotation is inspect.Signature.empty:
                raise ValueError("Function must have a return annotation.")

            variable_attribute_args = variable_attribute_annotation.__args__
            if len(variable_attribute_args) != 2:
                raise ValueError(
                    "Function must have a return annotation with two arguments."
                )
            variable_name = variable_attribute_args[0].__forward_arg__
            attribute_name = variable_attribute_args[1].__forward_arg__

            parameters = inspect.signature(func).parameters
            parameter_names = list(parameters.keys())
            if not parameter_names:
                raise ValueError(
                    "CypherTrigger functions require at least one parameter."
                )

            trigger = CypherTrigger(
                function=func,
                cypher_string=arg1,
                variable_set=variable_name,
                attribute_set=attribute_name,
                parameter_names=parameter_names,
            )

            # Check that parameters are in the Return statement of the Cypher string
            for param in parameter_names:
                if (
                    param
                    not in trigger.cypher.parse_tree.cypher.return_clause.variables
                ):
                    raise ValueError(
                        f"Parameter {param} not found in Cypher string"
                    )

            self.register_trigger(trigger)

            return wrapper

        return decorator

    def register_trigger(self, cypher_trigger: CypherTrigger) -> None:
        """
        Register a CypherTrigger with the machine.
        """
        self.trigger_dict[
            md5(cypher_trigger.cypher_string.encode()).hexdigest()
        ] = cypher_trigger


if __name__ == "__main__":  # pragma: no cover # typing:ignore
    pass
