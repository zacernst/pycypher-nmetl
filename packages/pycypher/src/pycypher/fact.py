"""
Fact Module Documentation (fact.py)
===================================

The ``fact.py`` module within the ``pycypher`` library defines the core classes for
representing and managing "facts" within a graph-like data structure. Facts are
atomic pieces of information about nodes, relationships, and their attributes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from nmetl.session import Session
from pycypher.lineage import Lineage
from pycypher.solver import (
    Constraint,
    ConstraintNodeHasAttributeWithValue,
    ConstraintNodeHasLabel,
    ConstraintRelationshipHasLabel,
    ConstraintRelationshipHasSourceNode,
    ConstraintRelationshipHasTargetNode,
    ConstraintVariableRefersToSpecificObject,
)
from shared.logger import LOGGER

class AtomicFact:  # pylint: disable=too-few-public-methods
    """
    Abstract base class for specific types of `Fact`.

    This class serves as a base for creating various types of facts in the system.
    It is intended to be subclassed and not used directly.

    Attributes:
        None

    """

    def __init__(self, *_, session: Optional[Session] = None):  # ruff: disable=F821
        """
        Initialize an AtomicFact instance.

        Args:
            *_: Variable positional arguments (ignored).
            session (Optional[Session]): The session this fact belongs to. Defaults to None.
        """
        self.session = None
        self.lineage: Lineage | None = None

    # def __getstate__(self):
    #     state = self.__dict__.copy()
    #     return state

    # def __setstate__(self, state):
    #      self.__dict__.update(state)
    #      self.c = None


class FactNodeHasLabel(AtomicFact):
    """
    Represents a fact that a node has a specific label.

    Attributes:
        node_id (str): The ID of the node.
        label (str): The label of the node.

    """

    def __init__(self, node_id: str, label: str, **kwargs):
        """
        Initialize a FactNodeHasLabel instance.

        Args:
            node_id (str): The ID of the node.
            label (str): The label of the node.
            **kwargs: Additional keyword arguments passed to the parent class.
        """
        self.node_id = node_id
        self.label = label
        super().__init__(**kwargs)

    def __repr__(self):
        """
        Return a string representation of the FactNodeHasLabel instance.

        Returns:
            str: A string representation in the format "NodeHasLabel: node_id label".
        """
        return f"NodeHasLabel: {self.node_id} {self.label}"

    def __eq__(self, other: Any) -> bool:
        """
        Check if this FactNodeHasLabel instance is equal to another object.

        Two FactNodeHasLabel instances are considered equal if they have the same
        node_id and label.

        Args:
            other (Any): The object to compare with.

        Returns:
            bool: True if the objects are equal, False otherwise.
        """
        return (
            isinstance(other, FactNodeHasLabel)
            and self.node_id == other.node_id
            and self.label == other.label
        )

    def __add__(self, other: Constraint) -> List[Dict[str, str]] | None:
        """
        Combine this fact with a constraint to produce a solution mapping.

        This method is used to check if this fact satisfies a constraint and
        if so, returns a mapping from variable names to values.

        Args:
            other (Constraint): The constraint to check against this fact.

        Returns:
            List[Dict[str, str]] | None: A mapping from variable names to values if the
                constraint is satisfied, None otherwise.

        Raises:
            ValueError: If the other object is not a Constraint.
        """
        if not isinstance(other, Constraint):
            raise ValueError("Can only check constraints against facts")

        match other:
            case ConstraintNodeHasLabel():
                out = (
                    {other.variable: self.node_id}
                    if self.label == other.label
                    else None
                )
            case ConstraintVariableRefersToSpecificObject():
                out = (
                    {other.variable: self.node_id}
                    if self.node_id == other.node_id
                    else None
                )
            case _:
                out = None
        return out

    def __hash__(self):
        """
        Return a hash value for this FactNodeHasLabel instance.

        The hash is based on the node_id and label attributes.

        Returns:
            int: A hash value for this instance.
        """
        return hash((self.node_id, self.label))


class FactRelationshipHasLabel(AtomicFact):
    """
    Represents a fact that a relationship has a specific label.

    Attributes:
        relationship_id (str): The ID of the relationship.
        relationship_label (str): The label of the relationship.

    """

    def __init__(self, relationship_id: str, relationship_label: str, **kwargs):
        self.relationship_id = relationship_id
        self.relationship_label = relationship_label
        super().__init__(**kwargs)

    def __repr__(self):
        return f"RelationshipHasLabel: {self.relationship_id} {self.relationship_label}"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FactRelationshipHasLabel)
            and self.relationship_id == other.relationship_id
            and self.relationship_label == other.relationship_label
        )

    def __add__(self, other: Constraint):
        if not isinstance(other, Constraint):
            raise ValueError("Can only check constraints against facts")
        return (
            {other.relationship_name: self.relationship_id}
            if (
                isinstance(other, ConstraintRelationshipHasLabel)
                and self.relationship_label == other.label
            )
            else None
        )

    def __hash__(self):
        return hash((self.relationship_id, self.relationship_label))


class FactRelationshipHasAttributeWithValue(AtomicFact):
    """
    Represents a fact that a relationship has a specific attribute with a given value.

    Attributes:
        relationship_id (str): The ID of the relationship.
        attribute (str): The attribute of the relationship.
        value (Any): The value of the attribute.

    Args:
        relationship_id (str): The ID of the relationship.
        attribute (str): The attribute of the relationship.
        value (Any): The value of the attribute.
    """

    def __init__(
        self, relationship_id: str, attribute: str, value: Any, **kwargs
    ):
        self.relationship_id = relationship_id
        self.attribute = attribute
        self.value = value
        super().__init__(**kwargs)

    def __hash__(self):
        return hash((self.relationship_id, self.attribute, self.value))


class FactNodeHasAttributeWithValue(AtomicFact):
    """
    Represents a fact that a node has a specific attribute with a given value.

    Attributes:
        node_id (str): The identifier of the node.
        attribute (str): The attribute of the node.
        value (Any): The value of the attribute.
    """

    def __init__(self, node_id: str, attribute: str, value: Any, **kwargs):
        self.node_id = node_id
        self.attribute = attribute
        self.value = value
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"NodeHasAttributeWithValue: {self.node_id} {self.attribute} {self.value}"

    def __add__(self, other: Constraint) -> Dict[str, str] | None:
        """Check the ``Constraint`` against the ``Fact``. Return a mapping from the
        variable in the constraint to the value in the fact if the constraint is
        satisfied, otherwise return None.

        Args:
            other (Constraint): The constraint to check against the fact.
        """
        # VariableRefersToSpecificObject
        out = 'hithere'
        match other:
            case ConstraintVariableRefersToSpecificObject():
                out = {other.node_id: self.node_id}
            case ConstraintNodeHasAttributeWithValue():
                other_value = (
                    other.value
                    if not hasattr(other.value, "value")
                    else other.value.value
                )
                out = (
                    {other.variable: self.node_id}
                    if (
                        isinstance(other, ConstraintNodeHasAttributeWithValue)
                        and self.attribute == other.attribute
                        and self.value == other_value
                    )
                    else None
                )
            case ConstraintNodeHasLabel():  # Too chatty!
                # Need the attributes in the triggers return projection

                # node_label = self.session.fact_collection.query(
                #     QueryNodeLabel(self.node_id)
                # )
                node_label = self.node_id.split(':')[0]
                LOGGER.debug('In match clause. node_label: %s', node_label)

                out = (
                    {other.variable: self.node_id}
                    if node_label
                    == other.label  # and self.attribute is in the trigger's arguments
                    else None
                )
            case ConstraintRelationshipHasSourceNode():
                out = None
            case ConstraintRelationshipHasTargetNode():
                out = None
            case ConstraintRelationshipHasLabel():
                out = None
            case _:
                raise ValueError(
                    f"Expected a ``Constraint``, but got {other.__class__.__name__}."
                )
        if out == 'hithere':
            LOGGER.error('hithere: %s', other) 
        return out

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FactNodeHasAttributeWithValue)
            and (self.node_id == other.node_id)
            and (self.attribute == other.attribute)
            and (self.value == other.value)
        )

    def __hash__(self):
        return hash((self.node_id, self.attribute, self.value))


class FactNodeRelatedToNode(AtomicFact):
    """
    Represents a fact that one node is related to another node with a specific relationship label.

    Attributes:
        node1_id (str): The ID of the first node.
        node2_id (str): The ID of the second node.
        relationship_label (str): The label of the relationship between the two nodes.

    """

    def __init__(
        self, node1_id: str, node2_id: str, relationship_label: str, **kwargs
    ):
        self.node1_id = node1_id
        self.node2_id = node2_id
        self.relationship_label = relationship_label
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"NodeRelatedToNode: {self.node1_id} {self.relationship_label} {self.node2_id}"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FactNodeRelatedToNode)
            and self.node1_id == other.node1_id
            and self.node2_id == other.node2_id
            and self.relationship_label == other.relationship_label
        )


class FactRelationshipHasSourceNode(AtomicFact):
    """
    Represents a fact that a relationship has a source node.

    Attributes:
        relationship_id (str): The ID of the relationship.
        source_node_id (str): The ID of the source node.

    """

    def __init__(self, relationship_id: str, source_node_id: str, **kwargs):
        self.relationship_id = relationship_id
        self.source_node_id = source_node_id
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"RelationshipHasSourceNode: {self.relationship_id} {self.source_node_id}"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FactRelationshipHasSourceNode)
            and self.relationship_id == other.relationship_id
            and self.source_node_id == other.source_node_id
        )

    def __add__(self, other: Constraint):
        if not isinstance(other, Constraint):
            raise ValueError("Can only check constraints against facts")

    def __hash__(self):
        return hash((self.relationship_id, self.source_node_id))


class FactRelationshipHasTargetNode(AtomicFact):
    """
    Represents a fact that a relationship has a target node.

    Attributes:
        relationship_id (str): The ID of the relationship.
        target_node_id (str): The ID of the target node.

    """

    def __init__(self, relationship_id: str, target_node_id: str, **kwargs):
        self.relationship_id = relationship_id
        self.target_node_id = target_node_id
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"RelationshipHasTargetNode: {self.relationship_id} {self.target_node_id}"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FactRelationshipHasTargetNode)
            and self.relationship_id == other.relationship_id
            and self.target_node_id == other.target_node_id
        )

    def __add__(self, other: Constraint):
        if not isinstance(other, Constraint):
            raise ValueError("Can only check constraints against facts")

    def __hash__(self):
        return hash((self.relationship_id, self.target_node_id))
