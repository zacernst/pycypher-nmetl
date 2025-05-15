"""
Fact Module Documentation (fact.py)
===================================

The ``fact.py`` module within the ``pycypher`` library defines the core classes for
representing and managing "facts" within a graph-like data structure. Facts are
atomic pieces of information about nodes, relationships, and their attributes.
"""

from __future__ import annotations

import collections

import fdb

fdb.api_version(710)
import inspect

# from concurrent.futures import ThreadPoolExecutor
import logging
import threading
import time
from abc import ABC, abstractmethod
from multiprocessing.pool import ThreadPool
from typing import Any, Dict, Generator, List, Optional

from nmetl.config import (  # pylint: disable=no-name-in-module
    BLOOM_FILTER_ERROR_RATE,
    BLOOM_FILTER_SIZE,
    CLEAR_DB_ON_START,
    ETCD3_RETRY_DELAY,
)
from nmetl.helpers import decode, encode
from nmetl.logger import LOGGER
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
from rbloom import Bloom  # pylint: disable=no-name-in-module, import-error
from rocksdict import (  # pylint: disable=no-name-in-module, import-error
    BlockBasedIndexType,
    BlockBasedOptions,
    Options,
    Rdict,
    ReadOptions,
    WriteOptions,
)

try:
    import etcd3
except ModuleNotFoundError:
    LOGGER.warning("etcd3-py not installed, etcd3 support disabled")


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

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["session"]
        return state

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
        # self.facts: List[AtomicFact] = facts or []
        self.session: Optional["Session"] = session  # type: ignore
        self.put_counter = 0
        self.yielded_counter = 0
        self += facts or []
        if CLEAR_DB_ON_START:
            self.close()

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
        return f"FactCollection: {len(self)}"

    @abstractmethod
    def __len__(self):
        """
        Get the number of facts in the collection.

        Returns:
            int: The number of facts in the collection.
        """
        pass

    def relationship_has_label_facts(self):
        """
        Generator function that yields facts of type `FactRelationshipHasLabel`.

        Iterates over the `facts` attribute and yields each fact that is an instance
        of `FactRelationshipHasLabel`.

        Yields:
            FactRelationshipHasLabel: Facts that are instances of `FactRelationshipHasLabel`.
        """
        for fact in self:
            if isinstance(fact, FactRelationshipHasLabel):
                yield fact

    @abstractmethod
    def keys(self):
        """ABC"""
        pass

    def node_has_attribute_with_specific_value_facts(
        self, attribute: str, value: Any
    ):
        """
        Return a generator of facts that have a specific attribute and value.
        """
        for fact in self:
            if (
                isinstance(fact, FactNodeHasAttributeWithValue)
                and fact.attribute == attribute
                and fact.value == value
            ):
                yield fact

    def relevant_facts(
        self, constraints: List[Constraint]
    ) -> Generator[AtomicFact]:
        """
        Return a generator of facts that are relevant to the given constraints.

        Args:
            constraints (List[Constraint]): A list of Constraint objects.

        Yields:
            AtomicFact: Facts that are relevant to the given constraints.
        """
        relevant_facts = set()
        for constraint in constraints:
            match constraint:
                case ConstraintNodeHasLabel():
                    for fact in self.nodes_with_label_facts(constraint.label):
                        if fact in relevant_facts:
                            continue
                        yield fact
                        relevant_facts.add(fact)
                case ConstraintNodeHasAttributeWithValue():
                    for (
                        fact
                    ) in self.node_has_attribute_with_specific_value_facts(
                        constraint.attribute, constraint.value
                    ):
                        if fact in relevant_facts:
                            continue
                        yield fact
                        relevant_facts.add(fact)
                case ConstraintRelationshipHasSourceNode():
                    for fact in self.relationship_has_source_node_facts():
                        if fact in relevant_facts:
                            continue
                        yield fact
                        relevant_facts.add(fact)
                case ConstraintRelationshipHasTargetNode():
                    for fact in self.relationship_has_target_node_facts():
                        if fact in relevant_facts:
                            continue
                        yield fact
                        relevant_facts.add(fact)
                case ConstraintRelationshipHasLabel():
                    for fact in self.relationship_has_label_facts():
                        if (
                            fact in relevant_facts
                            or fact.relationship_label != constraint.label
                        ):
                            continue
                        yield fact
                        relevant_facts.add(fact)
                case ConstraintVariableRefersToSpecificObject():
                    pass
                case _:
                    raise ValueError(
                        f"Expected a ``Constraint``, but got {constraint.__class__.__name__}."
                    )

    @abstractmethod
    def append(self, fact: AtomicFact) -> FactCollection:
        """
        Append an AtomicFact to the facts list.

        Args:
            value (AtomicFact): The AtomicFact object to be appended.

        Returns:
            None
        """
        pass

    def __iadd__(self, other: AtomicFact | List[Any]) -> FactCollection:
        """Let us use ``+=`` to add facts to the collection."""
        if isinstance(other, AtomicFact):
            self.append(other)
        elif isinstance(other, list):
            for thing in other:
                FactCollection.__iadd__(self, thing)
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
        for fact in self:
            if isinstance(fact, FactRelationshipHasSourceNode):
                yield fact

    def attributes_of_label(self):
        label_attributes_dict = collections.defaultdict(set)
        for fact in self:
            if isinstance(fact, FactNodeHasAttributeWithValue):
                node_id = fact.node_id
                attribute = fact.attribute
                query = QueryNodeLabel(node_id=node_id)
                label = self.query(query)
                label_attributes_dict[label].add(attribute)
        return label_attributes_dict

    def relationship_has_target_node_facts(self):
        """
        Generator method that yields facts of type FactRelationshipHasTargetNode.

        Iterates over the `facts` attribute of the instance and yields each fact
        that is an instance of FactRelationshipHasTargetNode.

        Yields:
            FactRelationshipHasTargetNode: Facts that are instances of
                FactRelationshipHasTargetNode.
        """
        for fact in self:
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
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        LOGGER.debug("node_has_label_facts called: %s", calframe[1][3])
        ##### Look here.
        for fact in self:
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
            for fact in self
        )

    def node_label_attribute_inventory(self):
        """
        Return a dictionary of all the facts in the collection.

        Returns:
            dict: A dictionary of all the facts in the collection.
        """
        attributes_by_label = collections.defaultdict(set)
        relationship_labels = set()

        for fact in self:
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

    def node_has_attribute_with_value_facts(self):
        """
        Generator method that yields facts of type FactNodeHasAttributeWithValue.

        Iterates over the list of facts and yields each fact that is an instance
        of FactNodeHasAttributeWithValue.

        Yields:
            FactNodeHasAttributeWithValue: Facts that are instances of
                FactNodeHasAttributeWithValue.
        """
        for fact in self:
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
        for fact in self:
            if isinstance(fact, FactRelationshipHasAttributeWithValue):
                yield fact

    def is_empty(self) -> bool:
        """
        Check if the fact collection is empty.

        Returns:
            bool: True if the fact collection is empty, False otherwise.
        """
        return len(self) == 0

    def query_value_of_node_attribute(self, query: QueryValueOfNodeAttribute):
        """
        Query the value of a node's attribute.

        Args:
            query (QueryValueOfNodeAttribute): Query object containing the node_id
                and attribute to look up.

        Returns:
            Any: The value of the requested attribute if found.
            NullResult: If no matching attribute is found.

        Raises:
            ValueError: If multiple values are found for the same attribute.
        """
        print("Query value of node attribute called...")
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
            raise ValueError(f"Found multiple values for {query}: {facts}")
        raise ValueError("Unknown error")

    def query_node_label(self, query: QueryNodeLabel):
        """Given a query for a node label, return the label if it exists.

        If no label exists, return a NullResult. If multiple labels
        exist, raise a ValueError.

        Args:
            query: The query to execute.

        Returns:
            The label of the node, or a NullResult if no label exists.

        Raises:
            ValueError: If multiple labels exist for the node.
        """
        facts = [
            fact
            for fact in self.node_has_label_facts()
            if fact.node_id == query.node_id
        ]
        if len(facts) == 1:
            return facts[0].label
        if not facts:
            return NullResult(query)
        if len(facts) > 1:
            raise ValueError(f"Found multiple labels for {query}")
        raise ValueError("Unknown error")

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
        for fact in self:
            if (
                isinstance(fact, FactNodeHasAttributeWithValue)
                and fact.node_id == node_id
                and fact.attribute in attributes
            ):
                row[fact.attribute] = fact.value
        return row

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
                out = self.query_value_of_node_attribute(query)
                return out
            case QueryNodeLabel():
                out = self.query_node_label(query)
                return out
            case _:
                raise NotImplementedError(f"Unknown query type {query}")

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

    def attributes_of_node_with_label(self, label: str) -> Generator[str]:
        """
        Return a list of all the nodes with a specific label.

        Args:
            label (str): The label of the nodes to return.

        Returns:
            list: A list of all the nodes with the specified label.
        """
        attributes_of_label_dict = self.attributes_of_label()
        for node_id in self.nodes_with_label(label):
            row_dict = {
                attribute: None for attribute in attributes_of_label_dict[label]
            }
            for attribute in attributes_of_label_dict[label]:
                attribute_value = self.attributes_for_specific_node(
                    node_id, attribute
                )
                row_dict.update(attribute_value)
            row_dict["__label__"] = label
            row_dict["__node_id__"] = node_id
            yield row_dict

    def node_has_label_facts(self):
        """
        Generator function that yields facts of type `FactNodeHasLabel`.

        Iterates over the `facts` attribute and yields each fact that is an instance
        of `FactNodeHasLabel`.

        Yields:
            FactNodeHasLabel: Facts that are instances of `FactNodeHasLabel`.
        """
        for fact in self._prefix_read_values(b"node_label:"):
            if isinstance(fact, FactNodeHasLabel):
                yield fact

    def nodes_with_label_facts(self, label: str) -> Generator[FactNodeHasLabel]:
        """
        Return a list of all the nodes with a specific label.

        Args:
            label (str): The label of the nodes to return.

        Returns:
            list: A list of all the nodes with the specified label.
        """
        for fact in self.node_has_label_facts():
            if fact.label == label:
                yield fact

    def rows_by_node_label(self, label: str) -> Generator[Dict[str, Any]]:
        """Docstring for rows_by_node_label

        :param self: Description
        :type self:
        :param label: Description
        :type label: str
        :return: Description
        :rtype: Generator[Dict[str, Any], None, None]
        """
        inventory = self.session.get_all_attributes_for_label(label)
        for node_id in self.nodes_with_label(label):
            yield self.attributes_for_specific_node(node_id, *inventory)

    @abstractmethod
    def close(self):
        """When cleanup is necessary for the class"""
        pass


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
        super().__init__(facts, session)

    def keys(self) -> Generator[AtomicFact]:
        """
        Iterate over the facts in this collection.

        Yields:
            AtomicFact: Each fact in the collection.
        """
        yield from self.facts

    def close(self):
        """Vacuously satisfy the interface"""
        pass

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
        for fact in self:
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

    def is_empty(self) -> bool:
        """
        Check if the fact collection is empty.

        Returns:
            bool: True if the fact collection is empty, False otherwise.
        """
        return len(self.facts) == 0

    def node_label_attribute_inventory_bak(self):
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
        inventory = list(self.session.get_all_attributes_for_label(label))
        for node_id in self.nodes_with_label(label):
            yield self.attributes_for_specific_node(node_id, *inventory)


class KeyValue(ABC):
    """Mixin for key-value stores"""

    def make_index_for_fact(self, fact: AtomicFact) -> str:
        """Used for the memcache index"""
        match fact:
            case FactNodeHasLabel():
                return f"node_label:{fact.label}::{fact.node_id}:{fact.label}"
            case FactNodeHasAttributeWithValue():
                return f"node_attribute:{fact.node_id}:{fact.attribute}:{fact.value}"
            case FactRelationshipHasLabel():
                return f"relationship_label:{fact.relationship_id}:{fact.relationship_label}"
            case FactRelationshipHasAttributeWithValue():
                return f"relationship_attribute:{fact.relationship_id}:{fact.attribute}:{fact.value}"
            case FactRelationshipHasSourceNode():
                return f"relationship_source_node:{fact.relationship_id}:{fact.source_node_id}"
            case FactRelationshipHasTargetNode():
                return f"relationship_target_node:{fact.relationship_id}:{fact.target_node_id}"
            case FactNodeRelatedToNode():
                return f"node_relationship:{fact.node1_id}:{fact.node2_id}:{fact.relationship_label}"
            case _:
                raise ValueError(f"Unknown fact type {fact}")

    def make_item_lookup(self, fact: AtomicFact) -> str:
        match fact:
            case FactNodeHasLabel():
                return f"node_label:{fact.label}::{fact.node_id}:{fact.label}"
            case FactNodeHasAttributeWithValue():
                return f"node_attribute:{fact.node_id}:{fact.attribute}:{fact.value}"
            case FactRelationshipHasLabel():
                return f"relationship_label:{fact.relationship_id}:{fact.relationship_label}"
            case FactRelationshipHasAttributeWithValue():
                return f"relationship_attribute:{fact.relationship_id}:{fact.attribute}:{fact.value}"
            case FactRelationshipHasSourceNode():
                return f"relationship_source_node:{fact.relationship_id}:{fact.source_node_id}"
            case FactRelationshipHasTargetNode():
                return f"relationship_target_node:{fact.relationship_id}:{fact.target_node_id}"
            case FactNodeRelatedToNode():
                return f"node_relationship:{fact.node1_id}:{fact.node2_id}:{fact.relationship_label}"
            case _:
                raise ValueError(f"Unknown fact type {fact}")

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
        self.bloom = Bloom(BLOOM_FILTER_SIZE, BLOOM_FILTER_ERROR_RATE)
        self.bloom_filter_diversions = 0
        self.cache_hits = 0
        self.secondary_cache = []
        self.secondary_cache_max_size = 1
        self.transaction = self.client.Txn()
        self.put_counter = 0
        super().__init__(*args, **kwargs)

    def query_value_of_node_attribute_bak(
        self, query: QueryValueOfNodeAttribute
    ):
        """
        Query the value of a node's attribute.

        Args:
            query (QueryValueOfNodeAttribute): Query object containing the node_id
                and attribute to look up.

        Returns:
            Any: The value of the requested attribute if found.
            NullResult: If no matching attribute is found.

        Raises:
            ValueError: If multiple values are found for the same attribute.
        """
        # node_attribute:Tract::01013952900:tract_fips:01013952900
        prefix = f"node_attribute:{query.node_id}:{query.attribute}:"
        matches = self.client.range(prefix, prefix + self.LAST_KEY).kvs or []
        if len(matches) == 1:
            return (
                matches[0].value.value
                if hasattr(matches[0].value, "value")
                else matches[0].value
            )
        if not matches:
            return NullResult(query)
        if len(matches) > 1:
            raise ValueError(f"Found multiple values for {query}: {matches}")
        raise ValueError("Unknown error")

    def keys(self) -> Generator[str]:
        """
        Iterate over all keys stored in the memcached server.

        Yields:
            str: Each key stored in the memcached server.
        """
        for key_value in self.client.range(all=True).kvs:
            key = key_value.key.decode("utf-8")
            yield key

    def __delitem__(self, key: str):
        """
        Delete a fact at a specific index.

        Args:
            index (int): The index of the fact to delete.

        Raises:
            IndexError: If the index is out of range.
        """
        self.client.delete_range(key=key)

    def __len__(self):
        """
        Get the number of facts in the collection.

        Returns:
            int: The number of facts in the collection.
        """
        num_keys = self.client.range(all=True).count
        return num_keys

    def values(self) -> Generator[AtomicFact]:
        """
        Iterate over all values stored in the memcached server.

        Yields:
            AtomicFact: Each value stored in the memcached server.

        Raises:
            pymemcache.exceptions.MemcacheError: If there's an error communicating with the memcached server.
            pickle.PickleError: If there's an error unpickling the data.
        """
        for key_value in self.client.range(all=True).kvs:
            value = key_value.value
            yield decode(value)

    def close(self):
        """Erase all the keys in the etcd3"""
        LOGGER.info("Clearing etcd3")
        self.client.delete_range(all=True)
        # time.sleep(1)

    def insert(self, _, value):
        """Vacuously satisfy the interface"""
        self.append(value)

    def delete_fact(self, fact):
        """Delete a fact from the etcd3"""
        index = self.make_index_for_fact(fact)
        self.client.delete_range(key=index)

    def __contains__(self, fact: AtomicFact) -> bool:
        """
        Check if a fact is in the collection.

        Args:
            fact (AtomicFact): The fact to check for.

        Returns:
            bool: True if the fact is in the collection, False otherwise.
        """
        index = self.make_index_for_fact(fact)

        # Try to divert from etcd3 with a Bloom filter
        if index not in self.bloom:
            self.bloom_filter_diversions += 1
            LOGGER.debug(
                "Bloom filter diversion: %s, cache hits: %s",
                self.bloom_filter_diversions,
                self.cache_hits,
            )
            return False

        if key_value_list := self.client.range(index, index + "\0").kvs:
            key_value = key_value_list[0]
            LOGGER.debug("Cache hit: %s", self.cache_hits)
            self.cache_hits += 1
            return decode(key_value.value) == fact

        return False

    def query_node_label(self, query: QueryNodeLabel):
        """Given a query for a node label, return the label if it exists.

        If no label exists, return a NullResult. If multiple labels
        exist, raise a ValueError.

        Args:
            query: The query to execute.

        Returns:
            The label of the node, or a NullResult if no label exists.

        Raises:
            ValueError: If multiple labels exist for the node.
        """

        facts = [
            fact
            for fact in self.node_has_label_facts()
            if fact.node_id == query.node_id
        ]
        if len(facts) == 1:
            return facts[0].label
        if not facts:
            return NullResult(query)
        if len(facts) > 1:
            raise ValueError(f"Found multiple labels for {query}")
        raise ValueError("Unknown error")

    def query_value_of_node_attribute(self, query: QueryValueOfNodeAttribute):
        """
        Query the value of a node's attribute.

        Args:
            query (QueryValueOfNodeAttribute): Query object containing the node_id
                and attribute to look up.

        Returns:
            Any: The value of the requested attribute if found.
            NullResult: If no matching attribute is found.

        Raises:
            ValueError: If multiple values are found for the same attribute.

        """
        prefix = f"node_attribute:{query.node_id}:{query.attribute}:"

        key_value_list = (
            self.client.range(prefix, prefix + "\0").kvs or []
        )  # Could be None
        if len(key_value_list) == 1:
            key_value = key_value_list[0]
            fact = decode(key_value.value)
            return fact
        else:
            return NullResult(query)

    def append(self, fact: AtomicFact) -> None:
        """
        Insert an AtomicFact into the facts list at the specified index.

        Args:
            index (int): The position at which to insert the value.
            value (AtomicFact): The AtomicFact object to be inserted.

        Returns:
            None
        """
        self.put_counter += 1
        if self.put_counter % 1000 == 0:
            LOGGER.debug("Put counter: %s", self.put_counter)
        index = self.make_index_for_fact(fact)
        self.secondary_cache.append(
            (
                index,
                fact,
            )
        )
        retry_counter = 0
        while 1:
            try:
                self.client.put(index, encode(fact))
                break
            except Exception as e:  # pylint: disable=broad-exception-caught
                LOGGER.debug("Error writing to etcd3: %s", e)
                retry_counter += 1
                if retry_counter > 10:
                    raise e
                time.sleep(ETCD3_RETRY_DELAY)
                continue
            break
        if len(self.secondary_cache) <= self.secondary_cache_max_size:
            return
        LOGGER.debug("Flushing cache")
        transaction = self.client.Txn()
        cached_indexes = []
        for index, cached_fact in self.secondary_cache:
            if index in cached_indexes:
                LOGGER.warning("Duplicate index %s", index)
                continue
            transaction.success(transaction.put(index, encode(cached_fact)))
            cached_indexes.append(index)
            self.bloom.add(index)
        transaction.commit()
        transaction.clear()
        self.secondary_cache = []

    def __iter__(self) -> Generator[AtomicFact]:
        yield from self.values()


class RocksDBFactCollection(FactCollection, KeyValue):
    """
    ``FactCollection`` that uses RocksDB as a backend.

    Attributes:
        session (Session): The session object associated with the fact collection.
    """

    def __init__(self, *args, db_path: str = "rocksdb", **kwargs):
        """
        Initialize a RocksDB-backed FactCollection.

        Args:
            *args: Variable positional arguments (ignored).
            **kwargs: Variable keyword arguments (ignored).
        """
        self.db_path = db_path
        self.write_options = WriteOptions()
        self.write_options.sync = False
        self.options = Options()
        self.options.set_unordered_write(False)
        self.options.create_if_missing(True)
        self.options.set_max_background_jobs(10)
        self.options.set_max_write_buffer_number(10)
        self.options.set_write_buffer_size(16 * 1024 * 1024 * 1024)

        self.write_options = WriteOptions()
        self.write_options.disable_wal = True

        self.db = Rdict(self.db_path, self.options)
        self.db.set_write_options(self.write_options)
        block_opts = BlockBasedOptions()
        block_opts.set_index_type(BlockBasedIndexType.hash_search())
        block_opts.set_bloom_filter(20, False)
        self.options.set_block_based_table_factory(block_opts)
        self.iter = self.db.iter(ReadOptions())
        self.LAST_KEY = "\xff"  # pylint: disable=invalid-name
        self.diverted_counter = 0
        self.diversion_miss_counter = 0
        super().__init__(*args, **kwargs)

    def _prefix_read_items(self, prefix: str) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Args:
            start_key (str): The starting key of the range.
            end_key (str): The ending key of the range.

        Yields:
            Any: The values associated with the keys in the range.
        """
        for key in self._prefix_read_keys(prefix):
            value = decode(self.db.get(key))
            if value is None:
                break
            yield key, value

    def _prefix_read_keys(self, prefix: str) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Args:
            start_key (str): The starting key of the range.
            end_key (str): The ending key of the range.

        Yields:
            Any: The values associated with the keys in the range.
        """
        if not isinstance(prefix, str):
            prefix = str(prefix, encoding="utf8")
        for key in self.db.keys(from_key=prefix):
            if not key.startswith(prefix):
                return
            yield key

    def _prefix_read_values(self, prefix: str) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Args:
            prefix (str): The prefix of the keys to read.

        Yields:
            Any: The values associated with the keys in the range.
        """
        for key in self._prefix_read_keys(prefix):
            value = decode(self.db.get(key))
            if value is None:
                break
            yield value

    def range_read(self, start_key, end_key) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Args:
            start_key (str): The starting key of the range.
            end_key (str): The ending key of the range.

        Yields:
            Any: The values associated with the keys in the range.
        """
        for key in self.db.keys(from_key=start_key):
            if key > end_key:
                break
            value = self.db.get(key)
            if value is None:
                break
            self.yielded_counter += 1
            yield decode(value)

    def keys(self) -> Generator[str]:
        """
        Yields:
            str: Each key
        """
        yield from self._prefix_read_keys("")

    def values(self) -> Generator[AtomicFact]:
        """
        Iterate over all values stored in the memcached server.
        """
        yield from self._prefix_read_values("")

    def node_has_attribute_with_specific_value_facts(
        self, attribute: str, value: Any
    ):
        """
        Return a generator of facts that have a specific attribute and value.
        """
        for fact in self.range_read("node_attribute:", "node_attribute:\xff"):
            if (
                isinstance(fact, FactNodeHasAttributeWithValue)
                and fact.attribute == attribute
                and fact.value == value
            ):
                yield fact

    def __delitem__(self, key: str):
        """
        Delete a fact.

        Raises:
            IndexError: If the index is out of range.
        """
        del self.db[key]

    def __len__(self):
        """
        Get the number of facts in the collection.

        Returns:
            int: The number of facts in the collection.
        """
        counter = 0
        for k in self.db.keys():
            LOGGER.debug("counting: %s", k)
            counter += 1
        return counter

    def close(self):
        """Erase all the keys in the db"""
        LOGGER.warning("Deleting RocksDB")
        self.db.delete_range("\x00", "\xff")

    def __contains__(self, fact: AtomicFact) -> bool:
        """
        Check if a fact is in the collection.

        Args:
            fact (AtomicFact): The fact to check for.

        Returns:
            bool: True if the fact is in the collection, False otherwise.
        """
        # curframe = inspect.currentframe()
        # calframe = inspect.getouterframes(curframe, 2)
        # LOGGER.debug("__contains__ called: %s", calframe[1][3])
        index = self.make_index_for_fact(fact)
        if self.db.key_may_exist(index):
            value = self.db.get(index)
            if value is None:
                self.diversion_miss_counter += 1
            return decode(value) == fact if value is not None else False
        self.diverted_counter += 1
        return False

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
        for attribute in attributes:
            prefix = f"node_attribute:{node_id}:{attribute}:"
            for fact in self._prefix_read_values(prefix):
                row[fact.attribute] = fact.value
                break
        return row

    def query_node_label(self, query: QueryNodeLabel):
        """Given a query for a node label, return the label if it exists.

        If no label exists, return a NullResult. If multiple labels
        exist, raise a ValueError.

        Args:
            query: The query to execute.

        Returns:
            The label of the node, or a NullResult if no label exists.

        Raises:
            ValueError: If multiple labels exist for the node.

        case FactNodeHasLabel():
            return f"node_label:{fact.node_id}:{fact.label}"
        case FactNodeHasAttributeWithValue():
            return f"node_attribute:{fact.node_id}:{fact.attribute}:{fact.value}"
        case FactRelationshipHasLabel():
            return f"relationship_label:{fact.relationship_id}:{fact.relationship_label}"
        case FactRelationshipHasAttributeWithValue():
            return f"relationship_attribute:{fact.relationship_id}:{fact.attribute}:{fact.value}"
        case FactRelationshipHasSourceNode():
            return f"relationship_source_node:{fact.relationship_id}:{fact.source_node_id}"
        case FactRelationshipHasTargetNode():
            return f"relationship_target_node:{fact.relationship_id}:{fact.target_node_id}"
        case FactNodeRelatedToNode():
            return f"node_relationship:{fact.node1_id}:{fact.node2_id}:{fact.relationship_label}"
        """
        if self.session is None:
            LOGGER.warning("Session is not set. Reverting to brute-force.")
            return FactCollection.query_node_label(self, query)
        prefix = "node_label:"
        labels = self.session.get_all_known_labels()
        for label in labels:
            prefix = f"node_label:{label}::{query.node_id}"
            result = NullResult(query)
            for fact in self._prefix_read_values(prefix):
                if (
                    isinstance(fact, FactNodeHasLabel)
                    and fact.node_id == query.node_id
                ):
                    result = fact.label
                    break
        return result

    def append(self, fact: AtomicFact) -> None:
        """
        Insert an AtomicFact.

        Args:
            value (AtomicFact): The AtomicFact object to be inserted.

        Returns:
            None
        """
        self.put_counter += 1
        index = self.make_index_for_fact(fact)
        self.db.put(index, encode(fact), write_opt=self.write_options)
        self.db.flush()

    def __iter__(self) -> Generator[AtomicFact]:
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        LOGGER.debug("__iter__ called: %s", calframe[1][3])

        yield from self.values()
        LOGGER.debug("Done iterating values by brute force.")

    def __repr__(self):
        return "Rocks"

    def query_value_of_node_attribute(self, query: QueryValueOfNodeAttribute):
        """
        Query the value of a node's attribute.

        Args:
            query (QueryValueOfNodeAttribute): Query object containing the node_id
                and attribute to look up.

        Returns:
            Any: The value of the requested attribute if found.
            NullResult: If no matching attribute is found.

        Raises:
            ValueError: If multiple values are found for the same attribute.

        """
        prefix = f"node_attribute:{query.node_id}:{query.attribute}:"
        result = list(self._prefix_read_values(prefix))
        if len(result) == 1:
            fact = result[0]
            return fact.value
        if len(result) > 1:
            raise ValueError(f"Found multiple values for {query}: {result}")
        return NullResult(query)

    def nodes_with_label(self, label: str) -> Generator[str]:
        """
        Return a list of all the nodes with a specific label.

        Args:
            label (str): The label of the nodes to return.

        Returns:
            list: A list of all the nodes with the specified label.
        """
        prefix = f"node_label:{label}::"
        for fact in self._prefix_read_values(prefix):
            if isinstance(fact, FactNodeHasLabel) and fact.label == label:
                yield fact.node_id

    def nodes_with_label_facts(self, label: str) -> Generator[str]:
        """
        Return a list of all the nodes with a specific label.

        Args:
            label (str): The label of the nodes to return.

        Returns:
            list: A list of all the nodes with the specified label.
        """
        prefix = f"node_label:{label}::"
        yield from self._prefix_read_values(prefix)

    def relationship_has_source_node_facts(self):
        """
        Generator method that yields facts of type FactRelationshipHasSourceNode.

        This method iterates over the `facts` attribute of the instance and yields
        each fact that is an instance of the FactRelationshipHasSourceNode class.

        Yields:
            FactRelationshipHasSourceNode: Facts that are instances of
                FactRelationshipHasSourceNode.
        """
        prefix = "relationship_source_node:"
        yield from self._prefix_read_values(prefix)

    def relationship_has_target_node_facts(self):
        """
        Generator method that yields facts of type FactRelationshipHasSourceNode.

        This method iterates over the `facts` attribute of the instance and yields
        each fact that is an instance of the FactRelationshipHasSourceNode class.

        Yields:
            FactRelationshipHasTargetNode: Facts that are instances of
                FactRelationshipHasTargetNode.
        """
        prefix = "relationship_target_node:"
        yield from self.range_read(prefix, prefix + self.LAST_KEY)


def ensure_bytes(value: Any, **kwargs) -> bytes:
    """Convert a value to bytes if it is not already."""
    if isinstance(value, bytes):
        return value
    return bytes(value, **kwargs)


def write_fact(db, index, fact, timing_histogram=None):
    LOGGER.debug("Writing to FoundationDB: %s", index)
    db[index] = encode(fact, to_bytes=True)

    return True


class FoundationDBFactCollection(FactCollection, KeyValue):
    """
    ``FactCollection`` that uses FoundationDB as a backend.

    Attributes:
        session (Session): The session object associated with the fact collection.
    """

    def __init__(self, *args, sync_writes: Optional[bool] = False, **kwargs):
        """
        Initialize a RocksDB-backed FactCollection.

        Args:
            *args: Variable positional arguments (ignored).
            **kwargs: Variable keyword arguments (ignored).
        """

        self.db = fdb.open()
        self.diverted_counter = 0
        self.diversion_miss_counter = 0
        self.thread_pool = ThreadPool(1024)
        self.pending_facts = []
        self.sync_writes = sync_writes

        # self.db = Rdict(self.db_path, self.options)
        super().__init__(*args, **kwargs)

    def _prefix_read_items(
        self, prefix: str, continue_to_end: Optional[bool] = False
    ) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Yields:
            Any: The values associated with the keys in the range.
        """
        LOGGER.debug("_prefix_read_items called")
        counter = 0
        prefix = ensure_bytes(prefix, encoding="utf8")
        end_key = "\xff" if continue_to_end else prefix + b"\xff"
        for key_value_obj in self.db.get_range(
            ensure_bytes(ensure_bytes(prefix), encoding="utf8"), end_key
        ):
            value = decode(key_value_obj.value)
            key = key_value_obj.key
            key = key.decode("utf8")  # Verify this is right
            if continue_to_end or key.startswith(str(prefix, encoding="utf8")):
                counter += 1
                yield key, value
            else:
                break
        LOGGER.debug("Done with _prefix_read_items: %s: %s", prefix, counter)

    def make_index_for_fact(self, fact: AtomicFact) -> bytes:
        """Used for the memcache index"""
        # Call the superclass's version of the method and convert to bytes
        index = super().make_index_for_fact(fact)
        return ensure_bytes(index, encoding="utf8")

    def _prefix_read_keys(
        self, prefix: str, continue_to_end: Optional[bool] = False
    ) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Args:
            start_key (str): The starting key of the range.
            end_key (str): The ending key of the range.

        Yields:
            Any: The values associated with the keys in the range.
        """
        LOGGER.debug("_prefix_read_keys called")
        for key, _ in self._prefix_read_items(
            ensure_bytes(prefix, encoding="utf8"),
            continue_to_end=continue_to_end,
        ):
            yield key

    def _prefix_read_values(
        self, prefix: str, continue_to_end: Optional[bool] = False
    ) -> Generator[Any, Any]:
        """
        Read a range of keys from the database.

        Args:
            prefix (str): The prefix of the keys to read.

        Yields:
            Any: The values associated with the keys in the range.
        """
        LOGGER.debug("_prefix_read_values called")
        counter = 0
        for _, value in self._prefix_read_items(
            ensure_bytes(prefix, encoding="utf8"),
            continue_to_end=continue_to_end,
        ):
            counter += 1
            yield value
        LOGGER.debug("Done with _prefix_read_values: %s", counter)

    def keys(self) -> Generator[str]:
        """
        Yields:
            str: Each key
        """
        LOGGER.debug("keys called")
        for key_value_obj in self.db.get_range(b"\x00", b"\xff"):
            key = key_value_obj.key.decode("utf8")
            yield key

    def values(self) -> Generator[AtomicFact]:
        """
        Iterate over all values stored in the memcached server.
        """
        LOGGER.debug("values called")
        yield from self

    def node_has_attribute_with_specific_value_facts(
        self, attribute: str, value: Any
    ):
        """
        Return a generator of facts that have a specific attribute and value.

        TODO: This can be refactored for efficiency. Will have to add an index key for certain facts.
        """
        LOGGER.debug("node_has_attribute_with_specific_value_facts called")
        for fact in self._prefix_read_values(
            b"node_attribute:", b"node_attribute:\xff"
        ):
            if (
                isinstance(fact, FactNodeHasAttributeWithValue)
                and fact.attribute == attribute
                and fact.value == value
            ):
                yield fact
        LOGGER.debug("done")

    def __delitem__(self, key: str):
        """
        Delete a fact.

        Raises:
            IndexError: If the index is out of range.
        """
        del self.db[ensure_bytes(key, encoding="utf8")]

    def __len__(self):
        """
        Get the number of facts in the collection.

        Returns:
            int: The number of facts in the collection.
        """
        counter = 0
        for k in self.keys():
            LOGGER.debug("counting: %s", k)
            counter += 1
        return counter

    def close(self):
        """Erase all the keys in the db"""
        LOGGER.warning("Deleting FoundationDB data")
        # self.db.clear_range(b"\x00", b"\xff")
        time.sleep(2)

    def __contains__(self, fact: AtomicFact) -> bool:
        """
        Check if a fact is in the collection.

        Args:
            fact (AtomicFact): The fact to check for.

        Returns:
            bool: True if the fact is in the collection, False otherwise.
        """
        index = self.make_index_for_fact(fact)
        value = self.db.get(index)
        return decode(value) == fact if value is not None else False
        # return False

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
        for attribute in attributes:
            prefix = ensure_bytes(
                f"node_attribute:{node_id}:{attribute}:", encoding="utf8"
            )
            for fact in self._prefix_read_values(prefix):
                row[fact.attribute] = fact.value
                break  # This shouldn't be necessary
        return row

    def node_has_specific_label_facts(self, label: str):
        """
        Generator function that yields facts of type `FactNodeHasLabel`.

        Iterates over the `facts` attribute and yields each fact that is an instance
        of `FactNodeHasLabel`.

        Yields:
            FactNodeHasLabel: Facts that are instances of `FactNodeHasLabel`.

        TODO: Also optimizew this by adding an index key on inserts.
        """
        LOGGER.debug("Node has specific label facts...")
        prefix = bytes(f"node_label:{label}::", encoding="utf8")
        for fact in self._prefix_read_values(prefix):
            if isinstance(fact, FactNodeHasLabel) and fact.label == label:
                yield fact

    def query_node_label(self, query: QueryNodeLabel):
        """Given a query for a node label, return the label if it exists.

        If no label exists, return a NullResult. If multiple labels
        exist, raise a ValueError.

        Args:
            query: The query to execute.

        Returns:
            The label of the node, or a NullResult if no label exists.

        Raises:
            ValueError: If multiple labels exist for the node.

        case FactNodeHasLabel():
            return f"node_label:{fact.node_id}:{fact.label}"
        case FactNodeHasAttributeWithValue():
            return f"node_attribute:{fact.node_id}:{fact.attribute}:{fact.value}"
        case FactRelationshipHasLabel():
            return f"relationship_label:{fact.relationship_id}:{fact.relationship_label}"
        case FactRelationshipHasAttributeWithValue():
            return f"relationship_attribute:{fact.relationship_id}:{fact.attribute}:{fact.value}"
        case FactRelationshipHasSourceNode():
            return f"relationship_source_node:{fact.relationship_id}:{fact.source_node_id}"
        case FactRelationshipHasTargetNode():
            return f"relationship_target_node:{fact.relationship_id}:{fact.target_node_id}"
        case FactNodeRelatedToNode():
            return f"node_relationship:{fact.node1_id}:{fact.node2_id}:{fact.relationship_label}"
        """
        node_id_parts = query.node_id.split("::")
        if len(node_id_parts) == 2:
            return node_id_parts[0]
        LOGGER.debug("Query node label...")
        for fact in self.node_has_label_facts():
            if (
                isinstance(fact, FactNodeHasLabel)
                and fact.node_id == query.node_id
            ):
                LOGGER.debug("Found label: %s", fact.label)
                return fact.label
        return NullResult(query)

    def append(self, fact: AtomicFact) -> None:
        """
        Insert an AtomicFact.

        Args:
            value (AtomicFact): The AtomicFact object to be inserted.

        Returns:
            None
        """
        self.put_counter += 1
        index = self.make_index_for_fact(fact)
        if self.sync_writes:
            LOGGER.debug("Using sync writes")
            apply_function = self.thread_pool.apply
        else:
            LOGGER.debug("Using async writes")
            apply_function = self.thread_pool.apply_async
        apply_function(
            write_fact,
            args=(
                self.db,
                index,
                fact,
            ),
        )

        # t = threading.Thread(target=write_fact, args=(self.db, index, fact,))
        # t.start()
        # self.db[index] = encode(fact, to_bytes=True)
        # Do we need to flush/commit/whatever

    def bak___iter__(self) -> Generator[AtomicFact]:
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        LOGGER.debug("__iter__ called: %s", calframe[1][3])

        yield from self.values()
        LOGGER.debug("Done iterating values by brute force.")

    def __repr__(self):
        return "FoundationDB"

    def query_value_of_node_attribute(self, query: QueryValueOfNodeAttribute):
        """
        Query the value of a node's attribute.

        Args:
            query (QueryValueOfNodeAttribute): Query object containing the node_id
                and attribute to look up.

        Returns:
            Any: The value of the requested attribute if found.
            NullResult: If no matching attribute is found.

        Raises:
            ValueError: If multiple values are found for the same attribute.

        """
        prefix = ensure_bytes(
            f"node_attribute:{query.node_id}:{query.attribute}:",
            encoding="utf8",
        )
        LOGGER.debug("Querying value of node attribute prefix: %s", prefix)
        result = list(self._prefix_read_values(prefix))
        if len(result) == 1:
            fact = result[0]
            return fact.value
        if len(result) > 1:
            raise ValueError(f"Found multiple values for {query}: {result}")
        return NullResult(query)

    def nodes_with_label(self, label: str) -> Generator[str]:
        """
        Return a list of all the nodes with a specific label.

        Args:
            label (str): The label of the nodes to return.

        Returns:
            list: A list of all the nodes with the specified label.
        """
        LOGGER.warning("nodes_with_label called")
        for fact in self:
            if isinstance(fact, FactNodeHasLabel) and fact.label == label:
                yield fact.node_id

    def nodes_with_label_facts(self, label: str) -> Generator[str]:
        """
        Return a list of all the nodes with a specific label.

        Args:
            label (str): The label of the nodes to return.

        Returns:
            list: A list of all the nodes with the specified label.
        """
        prefix = f"node_label:{label}::"
        yield from self._prefix_read_values(prefix)

    def relationship_has_source_node_facts(self):
        """
        Generator method that yields facts of type FactRelationshipHasSourceNode.

        This method iterates over the `facts` attribute of the instance and yields
        each fact that is an instance of the FactRelationshipHasSourceNode class.

        Yields:
            FactRelationshipHasSourceNode: Facts that are instances of
                FactRelationshipHasSourceNode.
        """
        LOGGER.debug("Relationship has source node facts called...")
        for fact in self:
            if isinstance(fact, FactRelationshipHasSourceNode):
                yield fact

    def __iter__(self):
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        LOGGER.warning("__iter__ called: %s", calframe[1][3])

        for key_value_obj in self.db.get_range(b"\x00", b"\xff"):
            key = key_value_obj.key.decode("utf8")
            value = decode(key_value_obj.value)
            self.yielded_counter += 1
            yield value
