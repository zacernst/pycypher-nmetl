"""
This module defines various constraint classes used to represent different
types of constraints in a graph database context.

Classes:

- Constraint: A base class for all constraints.
- IsTrue: A constraint that checks if a given predicate is true.
- ConstraintNodeHasLabel: A constraint that ensures a node has a specific label.
- ConstraintRelationshipHasSourceNode: A constraint that ensures a relationship has a
  specific source node.
- ConstraintRelationshipHasTargetNode: A constraint that ensures a relationship has a
  specific target node.
- ConstraintRelationshipHasLabel: A constraint that ensures a relationship has a specific label.
- ConstraintNodeHasAttributeWithValue: A constraint that ensures a node has a specific
  attribute with a given value.
"""

from __future__ import annotations

from typing import Any


class Constraint:
    """
    A base class used to represent a Constraint.

    This class currently does not have any attributes or methods.

    Attributes
    ----------
    None

    Methods
    -------
    None
    """


class IsTrue(Constraint):
    """
    A constraint that checks if a given predicate is true.

    Attributes:
        predicate (Predicate): The predicate to be evaluated.

    Methods:
        __repr__() -> str: Returns a string representation of the IsTrue instance.
    """

    def __init__(self, predicate: "Predicate"):  # type: ignore
        self.predicate = predicate  # type: ignore

    def __repr__(self) -> str:
        return f"IsTrue({self.predicate})"  # type: ignore

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, IsTrue) and self.predicate == other.predicate


class ConstraintNodeHasLabel(Constraint):
    """
    A class to represent a constraint that a node must have a specific label.

    Attributes:
    -----------
    node_id : str
        The identifier of the node.
    label : str
        The label that the node must have.

    Methods:
    --------
    __repr__():
        Returns a string representation of the constraint.
    __hash__() -> int:
        Returns a hash value for the constraint.
    __eq__(other: Any) -> bool:
        Checks if this constraint is equal to another constraint.
    """

    def __init__(self, node_id: str, label: str):
        self.node_id = node_id
        self.label = label

    def __repr__(self):
        return f"HasLabel: {self.node_id} {self.label}"

    def __hash__(self) -> int:
        return hash(
            str("HasLabel") + self.node_id.__str__() + self.label.__str__()
        )  # type: ignore

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintNodeHasLabel)
            and self.node_id == other.node_id
            and self.label == other.label
        )


class ConstraintRelationshipHasSourceNode(Constraint):
    """
    A constraint that ensures a relationship has a specific source node.

    Attributes:
        source_node_name (str): The name of the source node.
        relationship_name (str): The name of the relationship.

    Methods:
        __repr__(): Returns a string representation of the constraint.
        __hash__(): Returns a hash value for the constraint.
        __eq__(other: Any): Checks equality between this constraint and another object.
    """

    def __init__(self, source_node_name: str, relationship_name: str):
        self.source_node_name = source_node_name
        self.relationship_name = relationship_name

    def __repr__(self):
        return f"RelationshipHasSourceNode: {self.relationship_name} {self.source_node_name}"

    def __hash__(self) -> int:
        return hash(
            "RelationshipHasSourceNode"
            + self.relationship_name.__str__()
            + self.source_node_name.__str__()
        )

    def __eq__(self, other: Any) -> bool:
        """
        Check if this instance is equal to another instance.

        Args:
            other (Any): The other instance to compare against.

        Returns:
            bool: True if the other instance is of type ConstraintRelationshipHasSourceNode
                  and has the same source_node_name and relationship_name, False otherwise.
        """
        return (
            isinstance(other, ConstraintRelationshipHasSourceNode)
            and self.source_node_name == other.source_node_name
            and self.relationship_name == other.relationship_name
        )


class ConstraintRelationshipHasTargetNode(Constraint):
    """
    A constraint that ensures a relationship has a specific target node.

    Attributes:
        target_node_name (str): The name of the target node.
        relationship_name (str): The name of the relationship.

    Methods:
        __repr__(): Returns a string representation of the constraint.
        __hash__(): Returns a hash value for the constraint.
        __eq__(other: Any): Checks equality between this constraint and another object.
    """

    def __init__(self, target_node_name: str, relationship_name: str):
        self.target_node_name = target_node_name
        self.relationship_name = relationship_name

    def __repr__(self):
        return f"RelationshipHasTargetNode: {self.relationship_name} {self.target_node_name}"

    def __hash__(self) -> int:
        return hash(
            "RelationshipHasTargetNode"
            + self.relationship_name.__str__()
            + self.target_node_name.__str__()
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintRelationshipHasTargetNode)
            and self.target_node_name == other.target_node_name
            and self.relationship_name == other.relationship_name
        )


class ConstraintRelationshipHasLabel(Constraint):
    """
    A constraint that specifies a relationship must have a certain label.

    Attributes:
        relationship_name (str): The name of the relationship.
        label (str): The label that the relationship must have.

    Methods:
        __repr__(): Returns a string representation of the constraint.
        __hash__(): Returns a hash value for the constraint.
        __eq__(other: Any): Checks if this constraint is equal to another constraint.
    """

    def __init__(self, relationship_name: str, label: str):
        self.relationship_name = relationship_name
        self.label = label

    def __repr__(self):
        return f"RelationshipHasLabel: {self.relationship_name} {self.label}"

    def __hash__(self) -> int:
        return hash(
            "RelationshipHasLabel"
            + self.relationship_name.__str__()
            + self.label.__str__()
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintRelationshipHasLabel)
            and self.relationship_name == other.relationship_name
            and self.label == other.label
        )


class ConstraintNodeHasAttributeWithValue(Constraint):
    """
    A constraint that checks if a node has a specific attribute with a given value.

    Attributes:
        node_id (str): The ID of the node.
        attribute (str): The attribute to check.
        value (Any): The value that the attribute should have.

    Methods:
        __repr__(): Returns a string representation of the constraint.
        __hash__(): Returns a hash value for the constraint.
        __eq__(other: Any): Checks if this constraint is equal to another constraint.
    """

    def __init__(self, node_id: str, attribute: str, value: Any):
        self.node_id = node_id
        self.attribute = attribute
        self.value = value

    def __repr__(self):
        return f"HasAttributeWithValue: [{self.node_id}] {self.attribute}: {self.value}"

    def __hash__(self) -> int:
        return hash(
            "HasAttributeWithValue"
            + self.node_id
            + self.attribute
            + str(self.value)
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintNodeHasAttributeWithValue)
            and self.node_id == other.node_id
            and self.attribute == other.attribute
            and self.value == other.value
        )  # noqa: E501
