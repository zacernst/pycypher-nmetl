"""
Fact Module Documentation (fact.py)
==================================

The ``fact.py`` module within the ``pycypher`` library defines the core classes for
representing and managing "facts" within a graph-like data structure. Facts are
atomic pieces of information about nodes, relationships, and their attributes.
"""

from __future__ import annotations

import base64
import collections
import pickle
from typing import Any, Dict, Generator, List, Optional

import pymemcache
from nmetl.helpers import decode, encode
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
        self.session = session


class FactNodeHasLabel(AtomicFact):
    """
    Represents a fact that a node has a specific label.

    Attributes:
        node_id (str): The ID of the node.
        label (str): The label of the node.

    """

    def __init__(self, node_id: str, label: str, **kwargs):
        self.node_id = node_id
        self.label = label
        super().__init__(**kwargs)

    def __repr__(self):
        return f"NodeHasLabel: {self.node_id} {self.label}"

    def __eq__(self, other: Any):
        return (
            isinstance(other, FactNodeHasLabel)
            and self.node_id == other.node_id
            and self.label == other.label
        )

    def __add__(self, other: Constraint) -> List[Dict[str, str]] | None:
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


class FactCollection:
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
        self.facts: List[AtomicFact] = facts or []
        self.session: Optional["Session"] = session  # type: ignore

    def __iter__(self) -> Generator[AtomicFact]:
        yield from self.facts

    def __repr__(self) -> str:
        return f"FactCollection: {len(self.facts)}"

    def __getitem__(self, index: int) -> AtomicFact:
        return self.facts[index]

    def __setitem__(self, index: int, value: AtomicFact):
        self.facts[index] = value

    def __delitem__(self, index: int):
        del self.facts[index]

    def __len__(self):
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


class MemcacheFactCollection(FactCollection):
    """
    A collection of AtomicFact objects with various utility methods for
    querying and manipulating the facts.

    Attributes:
        facts (List[AtomicFact]): A list of AtomicFact objects.
        session (Session): The session object associated with the fact collection.

    """

    def __init__(self, *args, **kwargs):
        self.client = pymemcache.Client(server="localhost:11211")

    def __iter__(self) -> Generator[AtomicFact]:
        for key in self.client.stats("items"):
            yield pickle.loads(self.client.get(key))

    def make_index(self, fact: AtomicFact) -> str:
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

    def append(self, fact: AtomicFact) -> None:
        """
        Insert an AtomicFact into the facts list at the specified index.

        Args:
            index (int): The position at which to insert the value.
            value (AtomicFact): The AtomicFact object to be inserted.

        Returns:
            None
        """
        index = self.make_index(fact)
        self.client.set(index, encode(fact))
