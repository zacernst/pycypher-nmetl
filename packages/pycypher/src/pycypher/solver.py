"""
Solver Module (solver.py)
==========================

The ``solver.py`` module in the `pycypher` library defines a set of classes for
representing and evaluating constraints within a graph-like data structure. These
constraints are used to determine if certain conditions are met within the graph,
particularly in the context of triggering reactive behaviors (e.g., with
``CypherTrigger``).
"""

from __future__ import annotations

from typing import Any, Optional


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

    def __init__(self, trigger: Optional["CypherTrigger"] = None):  # type: ignore
        """
        Initialize a Constraint instance.

        Args:
            trigger (Optional[CypherTrigger]): The trigger associated with this constraint.
                Defaults to None.
        """
        self.trigger = trigger


class IsTrue(Constraint):
    """
    A constraint that checks if a given predicate is true.

    Attributes:
        predicate (Predicate): The predicate to be evaluated.

    Methods:
        __repr__() -> str: Returns a string representation of the IsTrue instance.
    """

    def __init__(self, predicate: "Predicate", **kwargs):  # type: ignore
        """
        Initialize an IsTrue constraint.

        Args:
            predicate (Predicate): The predicate to be evaluated.
            **kwargs: Additional keyword arguments passed to the parent class.
        """
        self.predicate = predicate  # type: ignore
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        """
        Return a string representation of the IsTrue constraint.

        Returns:
            str: A string representation in the format "IsTrue(predicate)".
        """
        return f"IsTrue({self.predicate})"  # type: ignore

    def __eq__(self, other: Any) -> bool:
        """
        Check if this IsTrue constraint is equal to another object.

        Two IsTrue constraints are considered equal if they have the same predicate.

        Args:
            other (Any): The object to compare with.

        Returns:
            bool: True if the objects are equal, False otherwise.
        """
        return isinstance(other, IsTrue) and self.predicate == other.predicate


class ConstraintVariableRefersToSpecificObject(Constraint):
    """
    A constraint that checks if a given predicate refers to a specific object.

    Attributes:
        predicate (Predicate): The predicate to be evaluated.

    Methods:
        __repr__() -> str: Returns a string representation of the IsTrue instance.
    """

    def __init__(self, variable: str, node_id: str, **kwargs):
        self.variable = variable
        self.node_id = node_id
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"ConstraintVariableRefersToSpecificObject: {self.variable} -> {self.node_id}"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintVariableRefersToSpecificObject)
            and self.variable == other.variable
            and self.node_id == other.node_id
        )


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

    def __init__(self, variable: str, label: str, **kwargs):
        self.variable = variable
        self.label = label
        super().__init__(**kwargs)

    def __repr__(self):
        return f"ConstraintNodeHasLabel: {self.variable} {self.label}"

    def __hash__(self) -> int:
        return hash(
            str("HasLabel") + self.variable.__str__() + self.label.__str__()
        )  # type: ignore

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintNodeHasLabel)
            and self.variable == other.variable
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

    def __init__(self, source_node_name: str, relationship_name: str, **kwargs):
        self.variable = source_node_name
        self.relationship_name = relationship_name
        super().__init__(**kwargs)

    def __repr__(self):
        return f"ConstraintRelationshipHasSourceNode: {self.relationship_name} {self.variable}"

    def __hash__(self) -> int:
        return hash(
            "RelationshipHasSourceNode"
            + self.relationship_name.__str__()
            + self.variable.__str__()
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
            and self.variable == other.variable
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

    def __init__(self, target_node_name: str, relationship_name: str, **kwargs):
        self.variable = target_node_name
        self.relationship_name = relationship_name
        super().__init__(**kwargs)

    def __repr__(self):
        return f"ConstraintRelationshipHasTargetNode: {self.relationship_name} {self.variable}"

    def __hash__(self) -> int:
        return hash(
            "RelationshipHasTargetNode"
            + self.relationship_name.__str__()
            + self.variable.__str__()
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintRelationshipHasTargetNode)
            and self.variable == other.variable
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

    def __init__(self, relationship_name: str, label: str, **kwargs):
        self.relationship_name = relationship_name
        self.label = label
        super().__init__(**kwargs)

    def __repr__(self):
        return f"ConstraintRelationshipHasLabel: {self.relationship_name} {self.label}"

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
        variable (str): The ID of the node.
        attribute (str): The attribute to check.
        value (Any): The value that the attribute should have.

    Methods:
        __repr__(): Returns a string representation of the constraint.
        __hash__(): Returns a hash value for the constraint.
        __eq__(other: Any): Checks if this constraint is equal to another constraint.
    """

    def __init__(self, variable: str, attribute: str, value: Any, **kwargs):
        self.variable = variable
        self.attribute = attribute
        self.value = value
        super().__init__(**kwargs)

    def __repr__(self):
        return f"ConstraintNodeHasAttributeWithValue: [{self.variable}] {self.attribute}: {self.value}"

    def __hash__(self) -> int:
        return hash(
            "HasAttributeWithValue"
            + self.variable
            + self.attribute
            + str(self.value)
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintNodeHasAttributeWithValue)
            and self.variable == other.variable
            and self.attribute == other.attribute
            and self.value == other.value
        )  # noqa: E501
