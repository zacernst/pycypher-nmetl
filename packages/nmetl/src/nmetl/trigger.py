"""
Trigger Module Documentation (trigger.py)
=====================================

"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, List, Protocol, TypeVar, runtime_checkable

from pycypher.cypher_parser import CypherParser
from shared.logger import LOGGER

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

    def __getitem__(self, *args, **kwargs) -> None:
        """
        Protocol method for indexing operations.

        This is a placeholder method required by the Protocol and is not meant to be called directly.

        Args:
            *args: Variable positional arguments.
            **kwargs: Variable keyword arguments.

        Returns:
            None
        """
        ...

    def __setitem__(self, *args, **kwargs) -> None:
        """
        Protocol method for item assignment operations.

        This is a placeholder method required by the Protocol and is not meant to be called directly.

        Args:
            *args: Variable positional arguments.
            **kwargs: Variable keyword arguments.

        Returns:
            None
        """
        ...


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

    def __getitem__(self, *args, **kwargs) -> None:
        """
        Protocol method for indexing operations.

        This is a placeholder method required by the Protocol and is not meant to be called directly.

        Args:
            *args: Variable positional arguments.
            **kwargs: Variable keyword arguments.

        Returns:
            None
        """
        ...

    def __setitem__(self, *args, **kwargs) -> None:
        """
        Protocol method for item assignment operations.

        This is a placeholder method required by the Protocol and is not meant to be called directly.

        Args:
            *args: Variable positional arguments.
            **kwargs: Variable keyword arguments.

        Returns:
            None
        """
        ...


# @dataclass
# class AttributeMetadata:
#     """
#     Metadata about an attribute in the ETL pipeline.
#
#     This class stores metadata about attributes, including the function that
#     generates the attribute, the attribute name, and a description.
#     """
#
#     function_name: Optional[str]
#     attribute_name: Optional[str]
#     description: Optional[str]
#
#
# @dataclass
# class RelationshipMetadata:
#     """Metadata about the relationship."""
#
#     name: Optional[str]
#     description: Optional[str]


@dataclass
class CypherTrigger(ABC):  # pylint: disable=too-many-instance-attributes
    """Abstract base class for Cypher-based triggers in the ETL pipeline.

    Triggers are reactive components that execute functions when specific
    patterns are matched in the graph data. They monitor facts and constraints
    to determine when to fire and execute associated functions.

    Attributes:
        function: The function to execute when the trigger fires.
        cypher_string: Cypher query defining trigger conditions.
        cypher: Parsed CypherParser instance.
        call_counter: Number of times this trigger has been called.
        error_counter: Number of errors encountered during execution.
        parameter_names: Names of parameters for the trigger function.
    """

    def __init__(
        self,
        function: Callable,
        cypher_string: str,
        # variable_set: Optional[str] = None,
        # attribute_set: Optional[str] = None,
        # session: Optional["Session"] = None,
        parameter_names: List[str],
    ):
        """
        Initialize a CypherTrigger instance.

        Args:
            function (Optional[Callable]): The function to be called when the trigger fires. Defaults to None.
            cypher_string (Optional[str]): The Cypher query string that defines the trigger conditions. Defaults to None.
            session (Optional[Session]): The session this trigger belongs to. Defaults to None.
            parameter_names (Optional[List[str]]): Names of parameters for the function. Defaults to None.

        Raises:
            ValueError: If there is an error parsing the Cypher string.
        """
        self.function = function
        self.cypher_string = cypher_string
        try:
            self.cypher = CypherParser(cypher_string)
        except Exception as e:
            raise ValueError(f"Error parsing Cypher string: {e}") from e
        self.call_counter = 0
        self.error_counter = 0
        # self.session = session
        # self.variable_set: Optional[str] = variable_set
        # self.attribute_set: Optional[str] = attribute_set
        self.parameter_names = parameter_names

    def __getstate__(self):
        """Get state for pickling/serialization.

        Returns:
            Dictionary containing the trigger's state.
        """
        state = self.__dict__.copy()
        return state

    def __repr__(self) -> str:
        """
        Return a string representation of the CypherTrigger instance.

        Returns:
            str: A string representation showing the constraints of this trigger.
        """
        return self.__class__.__name__

    @abstractmethod
    def __hash__(self):
        """
        Generate a hash value for this CypherTrigger instance.

        This is an abstract method that must be implemented by subclasses.

        Returns:
            int: A hash value for this instance.

        Raises:
            NotImplementedError: If not implemented by a subclass.
        """
        pass


class NodeRelationshipTrigger(CypherTrigger):
    """Trigger for creating relationships between nodes in the graph.

    This trigger type is used to create relationships between nodes based on
    the results of a Cypher query and a function that processes those results.
    When the trigger fires, it creates a relationship with the specified name
    between the source and target nodes.

    Attributes:
        source_variable: Name of the source node variable in the Cypher query.
        target_variable: Name of the target node variable in the Cypher query.
        relationship_name: Name of the relationship type to create.
        is_relationship_trigger: Always True for this trigger type.
        is_attribute_trigger: Always False for this trigger type.
    """

    def __init__(
        self,
        function: Callable,
        cypher_string: str,
        source_variable: str,
        target_variable: str,
        relationship_name: str,
        parameter_names: List[str] = [],
    ):
        """
        Initialize a NodeRelationshipTrigger instance.

        Args:
            function (Optional[Callable]): The function to be called when the trigger fires. Defaults to None.
            cypher_string (Optional[str]): The Cypher query string that defines the trigger conditions. Defaults to None.
            source_variable (Optional[str]): The name of the source node variable in the Cypher query. Defaults to None.
            target_variable (Optional[str]): The name of the target node variable in the Cypher query. Defaults to None.
            relationship_name (Optional[str]): The name of the relationship to create. Defaults to None.
            session (Optional[Session]): The session this trigger belongs to. Defaults to None.
            parameter_names (Optional[List[str]]): Names of parameters for the function. Defaults to None.
        """

        self.source_variable: str = source_variable
        self.target_variable: str = target_variable
        self.relationship_name: str = relationship_name
        LOGGER.debug("Creating NodeRelationshipTrigger: %s", self)
        LOGGER.debug("Source variable: %s", self.source_variable)
        LOGGER.debug("Target variable: %s", self.target_variable)
        LOGGER.debug("Relationship name: %s", self.relationship_name)
        self.is_relationship_trigger = True
        self.is_attribute_trigger = False
        super().__init__(
            function=function,
            cypher_string=cypher_string,
            parameter_names=parameter_names,
        )

    def __hash__(self):
        """
        Generate a hash value for this NodeRelationshipTrigger instance.

        The hash is based on the Cypher string, function name, source variable,
        target variable, and relationship name.

        Returns:
            int: A hash value for this instance.
        """
        return hash(
            self.cypher_string
            + self.function.__name__
            + self.source_variable
            + self.target_variable
            + self.relationship_name
        )


class VariableAttributeTrigger(CypherTrigger):
    """Trigger for setting attributes on variables in the graph.

    This trigger type is used to set attributes on graph nodes based on
    the results of a Cypher query and a function that processes those results.
    When the trigger fires, it sets the specified attribute on the target variable.

    Attributes:
        variable_set: Name of the variable to set the attribute on.
        attribute_set: Name of the attribute to set.
        is_relationship_trigger: Always False for this trigger type.
        is_attribute_trigger: Always True for this trigger type.
    """

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        function: Callable,
        cypher_string: str,
        variable_set: str,
        attribute_set: str,
        parameter_names: List[str] = [],
    ) -> None:
        """
        Initialize a VariableAttributeTrigger instance.

        Args:
            function (Optional[Callable]): The function to be called when the trigger fires. Defaults to None.
            cypher_string (Optional[str]): The Cypher query string that defines the trigger conditions. Defaults to None.
            variable_set (Optional[str]): The name of the variable to set the attribute on. Defaults to None.
            attribute_set (Optional[str]): The name of the attribute to set. Defaults to None.
            session (Optional[Session]): The session this trigger belongs to. Defaults to None.
            parameter_names (Optional[List[str]]): Names of parameters for the function. Defaults to None.
        """
        super().__init__(
            function=function,
            cypher_string=cypher_string,
            parameter_names=parameter_names,
        )
        self.variable_set: str = variable_set
        self.attribute_set: str = attribute_set
        self.is_relationship_trigger = False
        self.is_attribute_trigger = True

    def __hash__(self) -> int:
        """
        Generate a hash value for this VariableAttributeTrigger instance.

        The hash is based on the Cypher string, function name, variable set,
        and attribute set.

        Returns:
            int: A hash value for this instance.
        """
        return hash(
            self.cypher_string
            + self.function.__name__
            + self.variable_set
            + self.attribute_set
        )


if __name__ == "__main__":  # pragma: no cover # typing:ignore
    pass
