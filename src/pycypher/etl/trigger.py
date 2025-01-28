"""testing"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, Optional, Protocol, Set, TypeVar

from pycypher.core.cypher_parser import CypherParser
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
    
    def __repr__(self) -> str:
        return f"CypherTrigger(constraints: {self.constraints})"

    def _gather_constraints(self):
        for node in self.cypher.walk():
            if hasattr(node, "constraints"):
                self.constraints = self.constraints | set(node.constraints)


if __name__ == "__main__":  # pragma: no cover # typing:ignore
    pass
