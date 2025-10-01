"""
Fact Module Documentation (fact.py)
===================================

The ``fact.py`` module within the ``pycypher`` library defines the core classes for
representing and managing "facts" within a graph-like data structure. Facts are
atomic pieces of information about nodes, relationships, and their attributes.
"""

from __future__ import annotations

from typing import Any, Optional

from pycypher.lineage import Lineage
from pycypher.solutions import Projection, ProjectionList
from shared.logger import LOGGER


class AtomicFact:  # pylint: disable=too-few-public-methods
    """
    Abstract base class for specific types of `Fact`.

    This class serves as a base for creating various types of facts in the system.
    It is intended to be subclassed and not used directly.

    Attributes:
        None

    """

    def __init__(
        self,
        *_,
        **__,
    ):  # ruff: disable=F821
        """
        Initialize an AtomicFact instance.

        Args:
            *_: Variable positional arguments (ignored).
        """
        self.lineage: Optional[Lineage | None] = None
        # self.parent_projection: Optional[Projection | ProjectionList] = (
        #     parent_projection
        # )


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

    def __init__(
        self, relationship_id: str, relationship_label: str, **kwargs
    ) -> None:
        """Initialize a FactRelationshipHasLabel instance.

        Args:
            relationship_id: The ID of the relationship.
            relationship_label: The label of the relationship.
            **kwargs: Additional keyword arguments passed to parent class.
        """
        self.relationship_id = relationship_id
        self.relationship_label = relationship_label
        super().__init__(**kwargs)

    def __repr__(self):
        """Return string representation of the relationship label fact.

        Returns:
            String representation in format "RelationshipHasLabel: id label".
        """
        return f"RelationshipHasLabel: {self.relationship_id} {self.relationship_label}"

    def __eq__(self, other: Any) -> bool:
        """Check equality with another FactRelationshipHasLabel.

        Args:
            other: Object to compare with.

        Returns:
            True if both objects have same relationship_id and relationship_label.
        """
        return (
            isinstance(other, FactRelationshipHasLabel)
            and self.relationship_id == other.relationship_id
            and self.relationship_label == other.relationship_label
        )

    def __hash__(self):
        """Return hash value based on relationship_id and relationship_label.

        Returns:
            Hash value for this instance.
        """
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
    ) -> None:
        """Initialize a FactRelationshipHasAttributeWithValue instance.

        Args:
            relationship_id: The ID of the relationship.
            attribute: The attribute name.
            value: The attribute value.
            **kwargs: Additional keyword arguments passed to parent class.
        """
        self.relationship_id = relationship_id
        self.attribute = attribute
        self.value = value
        super().__init__(**kwargs)

    def __hash__(self):
        """Return hash value based on relationship_id, attribute, and value.

        Returns:
            Hash value for this instance.
        """
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
        """Initialize a FactNodeHasAttributeWithValue instance.

        Args:
            node_id: The identifier of the node.
            attribute: The attribute name.
            value: The attribute value.
            **kwargs: Additional keyword arguments passed to parent class.
        """
        self.node_id = node_id
        self.attribute = attribute
        self.value = value
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        """Return string representation of the node attribute fact.

        Returns:
            String representation in format "NodeHasAttributeWithValue: id attr value".
        """
        return f"NodeHasAttributeWithValue: {self.node_id} {self.attribute} {self.value}"

    def __eq__(self, other: Any) -> bool:
        """Check equality with another FactNodeHasAttributeWithValue.

        Args:
            other: Object to compare with.

        Returns:
            True if both objects have same node_id, attribute, and value.
        """
        return (
            isinstance(other, FactNodeHasAttributeWithValue)
            and (self.node_id == other.node_id)
            and (self.attribute == other.attribute)
            and (self.value == other.value)
        )

    def __hash__(self):
        """Return hash value based on node_id, attribute, and value.

        Returns:
            Hash value for this instance.
        """
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
        LOGGER.debug(
            "FactRelationshipHasSourceNode: %s %s",
            relationship_id,
            source_node_id,
        )
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"RelationshipHasSourceNode: {self.relationship_id} {self.source_node_id}"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FactRelationshipHasSourceNode)
            and self.relationship_id == other.relationship_id
            and self.source_node_id == other.source_node_id
        )

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

    def __hash__(self) -> int:
        return hash((self.relationship_id, self.target_node_id))
