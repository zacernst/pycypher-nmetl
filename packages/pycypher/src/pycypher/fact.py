"""
Fact Module Documentation (fact.py)
==================================

The ``fact.py`` module within the ``pycypher`` library defines the core classes for
representing and managing "facts" within a graph-like data structure. Facts are
atomic pieces of information about nodes, relationships, and their attributes.
"""

from __future__ import annotations

import collections
from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, List, Optional

from nmetl.helpers import decode, encode
from nmetl.logger import LOGGER

try:
    import etcd3
except ModuleNotFoundError:
    LOGGER.warning("etcd3-py not installed, etcd3 support disabled")

from pycypher.query import (
    NullResult,
    Query,
    QueryNodeLabel,
    QueryValueOfNodeAttribute,
)
from pycypher.solver import (
    Constraint,
    ConstraintNodeHasAttributeWithValue,
    ConstraintNodeHasLabel,
    ConstraintRelationshipHasLabel,
    ConstraintRelationshipHasSourceNode,
    ConstraintRelationshipHasTargetNode,
    ConstraintVariableRefersToSpecificObject,
)


class AtomicFact:  # pylint: disable=too-few-public-methods
    """
    Abstract base class for specific types of `Fact`.

    This class serves as a base for creating various types of facts in the system.
    It is intended to be subclassed and not used directly.

    Attributes:
        None

    """

    def __init__(self, *_, session: Optional["Session"] = None):  # type: ignore
        """
        Initialize an AtomicFact instance.

        Args:
            *_: Variable positional arguments (ignored).
            session (Optional[Session]): The session this fact belongs to. Defaults to None.
        """
        self.session = session


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

    def __init__(
        self, relationship_id: str, relationship_label: str, **kwargs
    ):
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

                if hasattr(
                    self.session, "fact_collection"
                ):  # only missing if unit tests
                    node_label = self.session.fact_collection.query(
                        QueryNodeLabel(self.node_id)
                    )

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


class FactCollection(ABC):
    """
    A collection of AtomicFact objects with various utility methods for
    querying and manipulating the facts.

    Attributes:
        facts (List[AtomicFact]): A list of AtomicFact objects.

    """

    def __init__(
        self,
        facts: Optional[List[AtomicFact]] = None,
        session: Optional["Session"] = None,  # type: ignore
    ):
        """
        Initialize a FactCollection instance.

        Args:
            facts (Optional[List[AtomicFact]]): A list of AtomicFact instances. Defaults to an empty list if None is provided.
            session (Optional[Session]): The session this fact collection belongs to. Defaults to None.
        """
        self.facts: List[AtomicFact] = facts or []
        self.session: Optional["Session"] = session  # type: ignore

    def __iter__(self) -> Generator[AtomicFact]:
        """
        Iterate over the facts in this collection.

        Yields:
            AtomicFact: Each fact in the collection.
        """
        yield from self.keys()

    def __repr__(self) -> str:
        """
        Return a string representation of the FactCollection instance.

        Returns:
            str: A string representation showing the number of facts in the collection.
        """
        return f"FactCollection: {len(self.facts)}"

    @abstractmethod
    def __len__(self):
        """
        Get the number of facts in the collection.

        Returns:
            int: The number of facts in the collection.
        """
        pass

    @abstractmethod
    def keys(self):
        """ABC"""
        pass

    @abstractmethod
    def append(self, fact: AtomicFact) -> FactCollection:
        """
        Append an AtomicFact to the facts list.

        Args:
            value (AtomicFact): The AtomicFact object to be appended.

        Returns:
            None
        """
        fact.session = self.session
        if fact not in self:
            self.facts.append(fact)
        return self

    def __iadd__(self, other: AtomicFact) -> FactCollection:
        """Let us use ``+=`` to add facts to the collection."""
        self.append(other)
        return self

    def relationship_has_source_node_facts(self):
        """
        Generator method that yields facts of type FactRelationshipHasSourceNode.

        This method iterates over the `facts` attribute of the instance and yields
        each fact that is an instance of the FactRelationshipHasSourceNode class.

        Yields:
            FactRelationshipHasSourceNode: Facts that are instances of
                FactRelationshipHasSourceNode.
        """
        for fact in self.facts:
            if isinstance(fact, FactRelationshipHasSourceNode):
                yield fact

    def relationship_has_target_node_facts(self):
        """
        Generator method that yields facts of type FactRelationshipHasTargetNode.

        Iterates over the `facts` attribute of the instance and yields each fact
        that is an instance of FactRelationshipHasTargetNode.

        Yields:
            FactRelationshipHasTargetNode: Facts that are instances of
                FactRelationshipHasTargetNode.
        """
        for fact in self.facts:
            if isinstance(fact, FactRelationshipHasTargetNode):
                yield fact

    def node_has_label_facts(self):
        """
        Generator function that yields facts of type `FactNodeHasLabel`.

        Iterates over the `facts` attribute and yields each fact that is an instance
        of `FactNodeHasLabel`.

        Yields:
            FactNodeHasLabel: Facts that are instances of `FactNodeHasLabel`.
        """
        for fact in self.facts:
            if isinstance(fact, FactNodeHasLabel):
                yield fact

    def node_with_id_exists(self, node_id: str) -> bool:
        """
        Check if a node with a specific ID exists in the fact collection.

        Args:
            node_id (str): The ID of the node to check for.

        Returns:
            bool: True if a node with the specified ID exists in the fact collection,
                False otherwise.
        """
        return any(
            isinstance(fact, FactNodeHasLabel) and fact.node_id == node_id
            for fact in self.facts
        )

    def node_has_attribute_with_value_facts(self):
        """
        Generator method that yields facts of type FactNodeHasAttributeWithValue.

        Iterates over the list of facts and yields each fact that is an instance
        of FactNodeHasAttributeWithValue.

        Yields:
            FactNodeHasAttributeWithValue: Facts that are instances of
                FactNodeHasAttributeWithValue.
        """
        for fact in self.facts:
            if isinstance(fact, FactNodeHasAttributeWithValue):
                yield fact

    def relationship_has_attribute_with_value_facts(self):
        """
        Generator function that yields facts of type FactRelationshipHasAttributeWithValue.

        Iterates over the `facts` attribute and yields each fact that is an instance of
        FactRelationshipHasAttributeWithValue.

        Yields:
            FactRelationshipHasAttributeWithValue: Facts that are instances of
                FactRelationshipHasAttributeWithValue.
        """
        for fact in self.facts:
            if isinstance(fact, FactRelationshipHasAttributeWithValue):
                yield fact

    def query(self, query: Query) -> Any:
        """
        Executes a query to retrieve information based on the type of the query.

        Args:
            query (Query): The query object containing the parameters for the query.

        Returns:
            Any: The result of the query. The type of the result depends on the query type.

        Raises:
            ValueError: If the query is of type QueryValueOfNodeAttribute and no matching
            facts are found, or if multiple matching facts are found, or if an
            unknown error occurs.
            NotImplementedError: If the query type is not recognized.

        """
        match query:
            case QueryValueOfNodeAttribute():
                facts = [
                    fact
                    for fact in self.node_has_attribute_with_value_facts()
                    if fact.node_id == query.node_id
                    and fact.attribute == query.attribute
                ]
                if len(facts) == 1:
                    return facts[0].value
                if not facts:
                    return NullResult(query)
                if len(facts) > 1:
                    raise ValueError(
                        f"Found multiple values for {query}: {facts}"
                    )
                raise ValueError("Unknown error")
            case QueryNodeLabel():
                facts = [
                    fact
                    for fact in self.node_has_label_facts()
                    if fact.node_id == query.node_id
                ]
                if len(facts) == 1:
                    return facts[0].label
                elif not facts:
                    return NullResult(query)
                elif len(facts) > 1:
                    raise ValueError(f"Found multiple labels for {query}")
                else:
                    raise ValueError("Unknown error")
            case _:
                raise NotImplementedError(f"Unknown query type {query}")

    def is_empty(self) -> bool:
        """
        Check if the fact collection is empty.

        Returns:
            bool: True if the fact collection is empty, False otherwise.
        """
        return len(self.facts) == 0

    def node_label_attribute_inventory(self):
        """
        Return a dictionary of all the facts in the collection.

        Returns:
            dict: A dictionary of all the facts in the collection.
        """
        attributes_by_label = collections.defaultdict(set)
        relationship_labels = set()

        for fact in self.facts:
            match fact:
                case FactNodeHasAttributeWithValue():
                    label = self.query(QueryNodeLabel(node_id=fact.node_id))
                    attributes_by_label[label].add(fact.attribute)
                case FactNodeHasLabel():
                    if fact.label not in attributes_by_label:
                        attributes_by_label[fact.label] = set()
                case FactRelationshipHasLabel():
                    relationship_labels.add(fact.relationship_label)
                case _:
                    continue

        return attributes_by_label

    def attributes_for_specific_node(
        self, node_id: str, *attributes: str
    ) -> Dict[str, Any]:
        """
        Return a dictionary of all the attributes for a specific node.

        Args:
            node_id (str): The ID of the node.

        Returns:
            dict: A dictionary of all the attributes for the specified node.
        """
        row = {attribute: None for attribute in attributes}
        for fact in self.facts:
            if (
                isinstance(fact, FactNodeHasAttributeWithValue)
                and fact.node_id == node_id
                and fact.attribute in attributes
            ):
                row[fact.attribute] = fact.value
        return row

    def nodes_with_label(self, label: str) -> Generator[str]:
        """
        Return a list of all the nodes with a specific label.

        Args:
            label (str): The label of the nodes to return.

        Returns:
            list: A list of all the nodes with the specified label.
        """
        for fact in self.node_has_label_facts():
            if fact.label == label:
                yield fact.node_id

    def rows_by_node_label(self, label: str) -> Generator[Dict[str, Any]]:
        """Docstring for rows_by_node_label

        :param self: Description
        :type self:
        :param label: Description
        :type label: str
        :return: Description
        :rtype: Generator[Dict[str, Any], None, None]
        """
        inventory = list(self.node_label_attribute_inventory()[label])
        for node_id in self.nodes_with_label(label):
            yield self.attributes_for_specific_node(node_id, *inventory)


class SimpleFactCollection(FactCollection):
    """
    A collection of AtomicFact objects with various utility methods for
    querying and manipulating the facts.

    Attributes:
        facts (List[AtomicFact]): A list of AtomicFact objects.

    """

    def __init__(
        self,
        facts: Optional[List[AtomicFact]] = None,
        session: Optional["Session"] = None,  # type: ignore
    ):
        """
        Initialize a FactCollection instance.

        Args:
            facts (Optional[List[AtomicFact]]): A list of AtomicFact instances. Defaults to an empty list if None is provided.
            session (Optional[Session]): The session this fact collection belongs to. Defaults to None.
        """
        self.facts: List[AtomicFact] = facts or []
        self.session: Optional["Session"] = session  # type: ignore

    def keys(self) -> Generator[AtomicFact]:
        """
        Iterate over the facts in this collection.

        Yields:
            AtomicFact: Each fact in the collection.
        """
        yield from self.facts

    # Not sure we're actually using this
    def __getitem__(self, index: int) -> AtomicFact:
        """
        Get a fact by index.

        Args:
            index (int): The index of the fact to retrieve.

        Returns:
            AtomicFact: The fact at the specified index.

        Raises:
            IndexError: If the index is out of range.
        """
        return self.facts[index]

    def __setitem__(self, index: int, value: AtomicFact):
        """
        Set a fact at a specific index.

        Args:
            index (int): The index at which to set the fact.
            value (AtomicFact): The fact to set at the specified index.

        Raises:
            IndexError: If the index is out of range.
        """
        self.facts[index] = value

    def __delitem__(self, index: int):
        """
        Delete a fact at a specific index.

        Args:
            index (int): The index of the fact to delete.

        Raises:
            IndexError: If the index is out of range.
        """
        del self.facts[index]

    def __len__(self):
        """
        Get the number of facts in the collection.

        Returns:
            int: The number of facts in the collection.
        """
        return len(self.facts)

    def insert(self, index: int, value: AtomicFact) -> FactCollection:
        """
        Insert an AtomicFact into the facts list at the specified index.

        Args:
            index (int): The position at which to insert the value.
            value (AtomicFact): The AtomicFact object to be inserted.

        Returns:
            None
        """
        self.facts.insert(index, value)
        return self

    def append(self, fact: AtomicFact) -> FactCollection:
        """
        Append an AtomicFact to the facts list.

        Args:
            value (AtomicFact): The AtomicFact object to be appended.

        Returns:
            None
        """
        fact.session = self.session
        if fact not in self:
            self.facts.append(fact)
        return self

    def relationship_has_source_node_facts(self):
        """
        Generator method that yields facts of type FactRelationshipHasSourceNode.

        This method iterates over the `facts` attribute of the instance and yields
        each fact that is an instance of the FactRelationshipHasSourceNode class.

        Yields:
            FactRelationshipHasSourceNode: Facts that are instances of
                FactRelationshipHasSourceNode.
        """
        for fact in self.facts:
            if isinstance(fact, FactRelationshipHasSourceNode):
                yield fact

    def relationship_has_target_node_facts(self):
        """
        Generator method that yields facts of type FactRelationshipHasTargetNode.

        Iterates over the `facts` attribute of the instance and yields each fact
        that is an instance of FactRelationshipHasTargetNode.

        Yields:
            FactRelationshipHasTargetNode: Facts that are instances of
                FactRelationshipHasTargetNode.
        """
        for fact in self.facts:
            if isinstance(fact, FactRelationshipHasTargetNode):
                yield fact

    def node_has_label_facts(self):
        """
        Generator function that yields facts of type `FactNodeHasLabel`.

        Iterates over the `facts` attribute and yields each fact that is an instance
        of `FactNodeHasLabel`.

        Yields:
            FactNodeHasLabel: Facts that are instances of `FactNodeHasLabel`.
        """
        for fact in self.facts:
            if isinstance(fact, FactNodeHasLabel):
                yield fact

    def node_with_id_exists(self, node_id: str) -> bool:
        """
        Check if a node with a specific ID exists in the fact collection.

        Args:
            node_id (str): The ID of the node to check for.

        Returns:
            bool: True if a node with the specified ID exists in the fact collection,
                False otherwise.
        """
        return any(
            isinstance(fact, FactNodeHasLabel) and fact.node_id == node_id
            for fact in self.facts
        )

    def node_has_attribute_with_value_facts(self):
        """
        Generator method that yields facts of type FactNodeHasAttributeWithValue.

        Iterates over the list of facts and yields each fact that is an instance
        of FactNodeHasAttributeWithValue.

        Yields:
            FactNodeHasAttributeWithValue: Facts that are instances of
                FactNodeHasAttributeWithValue.
        """
        for fact in self.facts:
            if isinstance(fact, FactNodeHasAttributeWithValue):
                yield fact

    def relationship_has_attribute_with_value_facts(self):
        """
        Generator function that yields facts of type FactRelationshipHasAttributeWithValue.

        Iterates over the `facts` attribute and yields each fact that is an instance of
        FactRelationshipHasAttributeWithValue.

        Yields:
            FactRelationshipHasAttributeWithValue: Facts that are instances of
                FactRelationshipHasAttributeWithValue.
        """
        for fact in self.facts:
            if isinstance(fact, FactRelationshipHasAttributeWithValue):
                yield fact

    def query(self, query: Query) -> Any:
        """
        Executes a query to retrieve information based on the type of the query.

        Args:
            query (Query): The query object containing the parameters for the query.

        Returns:
            Any: The result of the query. The type of the result depends on the query type.

        Raises:
            ValueError: If the query is of type QueryValueOfNodeAttribute and no matching
            facts are found, or if multiple matching facts are found, or if an
            unknown error occurs.
            NotImplementedError: If the query type is not recognized.

        """
        match query:
            case QueryValueOfNodeAttribute():
                facts = [
                    fact
                    for fact in self.node_has_attribute_with_value_facts()
                    if fact.node_id == query.node_id
                    and fact.attribute == query.attribute
                ]
                if len(facts) == 1:
                    return facts[0].value
                if not facts:
                    return NullResult(query)
                if len(facts) > 1:
                    raise ValueError(
                        f"Found multiple values for {query}: {facts}"
                    )
                raise ValueError("Unknown error")
            case QueryNodeLabel():
                facts = [
                    fact
                    for fact in self.node_has_label_facts()
                    if fact.node_id == query.node_id
                ]
                if len(facts) == 1:
                    return facts[0].label
                elif not facts:
                    return NullResult(query)
                elif len(facts) > 1:
                    raise ValueError(f"Found multiple labels for {query}")
                else:
                    raise ValueError("Unknown error")
            case _:
                raise NotImplementedError(f"Unknown query type {query}")

    def is_empty(self) -> bool:
        """
        Check if the fact collection is empty.

        Returns:
            bool: True if the fact collection is empty, False otherwise.
        """
        return len(self.facts) == 0

    def node_label_attribute_inventory(self):
        """
        Return a dictionary of all the facts in the collection.

        Returns:
            dict: A dictionary of all the facts in the collection.
        """
        attributes_by_label = collections.defaultdict(set)
        relationship_labels = set()

        for fact in self.facts:
            match fact:
                case FactNodeHasAttributeWithValue():
                    label = self.query(QueryNodeLabel(node_id=fact.node_id))
                    attributes_by_label[label].add(fact.attribute)
                case FactNodeHasLabel():
                    if fact.label not in attributes_by_label:
                        attributes_by_label[fact.label] = set()
                case FactRelationshipHasLabel():
                    relationship_labels.add(fact.relationship_label)
                case _:
                    continue

        return attributes_by_label

    def attributes_for_specific_node(
        self, node_id: str, *attributes: str
    ) -> Dict[str, Any]:
        """
        Return a dictionary of all the attributes for a specific node.

        Args:
            node_id (str): The ID of the node.

        Returns:
            dict: A dictionary of all the attributes for the specified node.
        """
        row = {attribute: None for attribute in attributes}
        for fact in self.facts:
            if (
                isinstance(fact, FactNodeHasAttributeWithValue)
                and fact.node_id == node_id
                and fact.attribute in attributes
            ):
                row[fact.attribute] = fact.value
        return row

    def nodes_with_label(self, label: str) -> Generator[str]:
        """
        Return a list of all the nodes with a specific label.

        Args:
            label (str): The label of the nodes to return.

        Returns:
            list: A list of all the nodes with the specified label.
        """
        for fact in self.node_has_label_facts():
            if fact.label == label:
                yield fact.node_id

    def rows_by_node_label(self, label: str) -> Generator[Dict[str, Any]]:
        """Docstring for rows_by_node_label

        :param self: Description
        :type self:
        :param label: Description
        :type label: str
        :return: Description
        :rtype: Generator[Dict[str, Any], None, None]
        """
        inventory = list(self.node_label_attribute_inventory()[label])
        for node_id in self.nodes_with_label(label):
            yield self.attributes_for_specific_node(node_id, *inventory)


class KeyValue(ABC):
    """Mixin for key-value stores"""

    def make_index_for_fact(self, fact: AtomicFact) -> str:
        """Used for the memcache index"""
        match fact:
            case FactNodeHasLabel():
                return f"node_label:{fact.node_id}:{fact.label}"
            case FactNodeHasAttributeWithValue():
                return f"node_attribute:{fact.node_id}:{fact.attribute}:{fact.value}"
            case FactRelationshipHasLabel():
                return f"relationship_label:{fact.relationship_id}"
            case FactRelationshipHasAttributeWithValue():
                return f"relationship_attribute:{fact.relationship_id}:{fact.attribute}"
            case FactRelationshipHasSourceNode():
                return f"relationship_source_node:{fact.relationship_id}:{fact.source_node_id}"
            case FactRelationshipHasTargetNode():
                return f"relationship_target_node:{fact.relationship_id}:{fact.target_node_id}"
            case _:
                raise ValueError("Unknown fact type")

    @abstractmethod
    def keys(self):
        """ABC"""
        raise NotImplementedError(
            "``keys`` method must be implemented in ``FactCollection`` class that inherits from ``KeyValue``"
        )

    @abstractmethod
    def values(self):
        """ABC"""
        raise NotImplementedError(
            "``values`` method must be implemented in ``FactCollection`` class that inherits from ``KeyValue``"
        )

    def __iterkeys__(self) -> Generator[Any]:
        yield from self.keys()

    def __itervalues__(self) -> Generator[Any]:
        yield from self.values()

    def __iteritems__(self) -> Generator[Any]:
        yield from zip(self.keys(), self.values())


class Etcd3FactCollection(FactCollection, KeyValue):
    """
    ``FactCollection`` that uses etcd version 3 as a backend.

    Attributes:
        facts (List[AtomicFact]): A list of AtomicFact objects.
        session (Session): The session object associated with the fact collection.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize a etcd3 instance.

        Args:
            *args: Variable positional arguments (ignored).
            **kwargs: Variable keyword arguments (ignored).
        """
        self.client = etcd3.Client("127.0.0.1", 2379)
        super().__init__(*args, **kwargs)

    def keys(self) -> Generator[str]:
        """
        Iterate over all keys stored in the memcached server.

        Yields:
            str: Each key stored in the memcached server.

        Raises:
            pymemcache.exceptions.MemcacheError: If there's an error communicating with the memcached server.
            pickle.PickleError: If there's an error unpickling the data.
        """
        try:
            for key_value in self.client.range(all=True).kvs:
                key = key_value.key.decode("utf-8")
                yield key
        except TypeError:  # Empty
            pass

    def __delitem__(self, key: str):
        """
        Delete a fact at a specific index.

        Args:
            index (int): The index of the fact to delete.

        Raises:
            IndexError: If the index is out of range.
        """
        self.client.delete_range(key)

    def __len__(self):
        """
        Get the number of facts in the collection.

        Returns:
            int: The number of facts in the collection.
        """
        counter = 0
        for _ in self.keys():
            counter += 1
        return counter

    def __setitem__(self, index: int, value: AtomicFact):
        """
        Set a fact at a specific index.

        Args:
            index (int): The index at which to set the fact.
            value (AtomicFact): The fact to set at the specified index.

        Raises:
            IndexError: If the index is out of range.
        """
        self.facts[index] = value

    def values(self) -> Generator[AtomicFact]:
        """
        Iterate over all values stored in the memcached server.

        Yields:
            AtomicFact: Each value stored in the memcached server.

        Raises:
            pymemcache.exceptions.MemcacheError: If there's an error communicating with the memcached server.
            pickle.PickleError: If there's an error unpickling the data.
        """
        try:
            for key_value in self.client.range(all=True).kvs:
                value = key_value.value
                yield decode(value)
        except TypeError:  # Empty
            pass

    def clear(self):
        """Erase all the keys in the etcd3"""
        self.client.delete_range(all=True)

    def insert(self, _, value):
        """Vacuously satisfy the interface"""
        self.append(value)

    def delete_fact(self, fact):
        """Delete a fact from the etcd3"""
        index = self.make_index_for_fact(fact)
        self.client.delete_range(index)

    def __contains__(self, fact: AtomicFact) -> bool:
        """
        Check if a fact is in the collection.

        Args:
            fact (AtomicFact): The fact to check for.

        Returns:
            bool: True if the fact is in the collection, False otherwise.
        """
        index = self.make_index_for_fact(fact)
        return self.client.range(index).kvs is not None

    def append(self, fact: AtomicFact) -> None:
        """
        Insert an AtomicFact into the facts list at the specified index.

        Args:
            index (int): The position at which to insert the value.
            value (AtomicFact): The AtomicFact object to be inserted.

        Returns:
            None
        """
        index = self.make_index_for_fact(fact)
        self.client.put(index, encode(fact))

    def __iter__(self) -> Generator[AtomicFact]:
        yield from self.keys()
