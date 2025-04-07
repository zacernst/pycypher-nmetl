"""
Trigger Module Documentation (trigger.py)
=====================================

The ``trigger.py`` module within the ``nmetl`` library defines the core classes and protocols
for creating and managing triggers in the ETL pipeline. Triggers are functions that are executed
when certain conditions are met in the data processing pipeline.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Callable,
    List,
    Optional,
    Protocol,
    Set,
    TypeVar,
    runtime_checkable,
)

from pycypher.cypher_parser import CypherParser
from pycypher.solver import Constraint

Variable = TypeVar("Variable")
Attribute = TypeVar("Attribute")
SourceVariable = TypeVar("SourceVariable")
TargetVariable = TypeVar("TargetVariable")


@runtime_checkable
class VariableAttribute(Protocol[Variable, Attribute]):
    """
    Protocol to be used in triggered functions as return signature.

    This protocol is used to type-hint functions that return a value to be assigned
    as an attribute of a variable in the Cypher query.

    Type Parameters:
        Variable: The type of the variable in the Cypher query.
        Attribute: The type of the attribute to be assigned.
    """

    def __getitem__(self, *args, **kwargs) -> None: ...

    def __setitem__(self, *args, **kwargs) -> None: ...


@runtime_checkable
class NodeRelationship(Protocol[SourceVariable, Attribute, TargetVariable]):
    """
    Protocol to be used in triggered functions that create relationships.

    This protocol is used to type-hint functions that return a value to be used
    as a relationship between two nodes in the Cypher query.

    Type Parameters:
        SourceVariable: The type of the source node variable in the Cypher query.
        Attribute: The type of the relationship attribute.
        TargetVariable: The type of the target node variable in the Cypher query.
    """

    def __getitem__(self, *args, **kwargs) -> None: ...

    def __setitem__(self, *args, **kwargs) -> None: ...


@dataclass
class AttributeMetadata:
    """
    Metadata about an attribute in the ETL pipeline.

    This class stores metadata about attributes, including the function that
    generates the attribute, the attribute name, and a description.
    """

    function_name: Optional[str]
    attribute_name: Optional[str]
    description: Optional[str]


@dataclass
class RelationshipMetadata:
    """Metadata about the relationship."""

    name: Optional[str]
    description: Optional[str]


@dataclass
class CypherTrigger(ABC):  # pylint: disable=too-many-instance-attributes
    """
    We check the ``Fact`` and ``Constraint`` objects to see if they
    indicate that the trigger should be fired.
    """

    def __init__(
        self,
        function: Optional[Callable] = None,
        cypher_string: Optional[str] = None,
        # variable_set: Optional[str] = None,
        # attribute_set: Optional[str] = None,
        session: Optional["Session"] = None,
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
        self.session = session
        # self.variable_set: Optional[str] = variable_set
        # self.attribute_set: Optional[str] = attribute_set
        self.parameter_names = parameter_names

        self.constraints: Set[Constraint] = set()

        self._gather_constraints()

    def __repr__(self) -> str:
        return f"CypherTrigger(constraints: {self.constraints})"

    def _gather_constraints(self):
        for node in self.cypher.walk():
            if hasattr(node, "constraints"):
                self.constraints = self.constraints | set(node.constraints)

    @abstractmethod
    def __hash__(self):
        pass


class NodeRelationshipTrigger(CypherTrigger):
    """
    Trigger for creating relationships between nodes in the graph.

    This trigger is used to create relationships between nodes based on
    the results of a Cypher query and a function that processes those results.
    """

    def __init__(
        self,
        function: Optional[Callable] = None,
        cypher_string: Optional[str] = None,
        source_variable: Optional[str] = None,
        target_variable: Optional[str] = None,
        relationship_name: Optional[str] = None,
        session: Optional["Session"] = None,
        parameter_names: Optional[List[str]] = None,
    ):
        super().__init__(
            function=function,
            cypher_string=cypher_string,
            session=session,
            parameter_names=parameter_names,
        )
        self.source_variable: Optional[str] = source_variable
        self.target_variable: Optional[str] = target_variable
        self.relationship_name: Optional[str] = relationship_name
        self.is_relationship_trigger = True
        self.is_attribute_trigger = False

    def __hash__(self):
        return hash(
            self.cypher_string
            + self.function.__name__
            + self.source_variable
            + self.target_variable
            + self.relationship_name
        )


class VariableAttributeTrigger(CypherTrigger):
    """
    Trigger for setting attributes on variables in the graph.

    This trigger is used to set attributes on variables based on
    the results of a Cypher query and a function that processes those results.
    """

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        function: Optional[Callable] = None,
        cypher_string: Optional[str] = None,
        variable_set: Optional[str] = None,
        attribute_set: Optional[str] = None,
        session: Optional["Session"] = None,
        parameter_names: Optional[List[str]] = None,
    ):
        super().__init__(
            function=function,
            cypher_string=cypher_string,
            session=session,
            parameter_names=parameter_names,
        )
        self.variable_set: Optional[str] = variable_set
        self.attribute_set: Optional[str] = attribute_set
        self.is_relationship_trigger = False
        self.is_attribute_trigger = True

        if function.__doc__:
            attribute_metadata = AttributeMetadata(
                attribute_name=attribute_set,
                function_name=function.__name__,
                description=function.__doc__,
            )

            self.session.attribute_metadata_dict[
                self.attribute_set  # should be name of attribute
            ] = attribute_metadata

    def __hash__(self):
        return hash(
            self.cypher_string
            + self.function.__name__
            + self.variable_set
            + self.attribute_set
        )


if __name__ == "__main__":  # pragma: no cover # typing:ignore
    pass
