"""
Fact Collection
===============

"""

from __future__ import annotations

import collections
import inspect
from abc import ABC, abstractmethod
import queue
import threading
from typing import Any, Dict, Generator, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from nmetl.session import Session

from nmetl.config import CLEAR_DB_ON_START  # pyrefly: ignore
from nmetl.logger import LOGGER
from pycypher.fact import (
    AtomicFact,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
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
        start_daemon_process: bool = False,
        session: Optional[Session] = None,  # type: ignore
    ):
        """
        Initialize a FactCollection instance.

        Args:
            facts (Optional[List[AtomicFact]]): A list of AtomicFact instances. Defaults to an empty list if None is provided.
            session (Optional[Session]): The session this fact collection belongs to. Defaults to None.
        """
        # self.facts: List[AtomicFact] = facts or []
        self.session: Optional[Session] = session  # type: ignore
        self.put_counter: int = 0
        self.yielded_counter: int = 0
        self.diverted_counter: int = 0
        self.diversion_miss_counter: int = 0
        self.start_daemon_process: bool = start_daemon_process
        self.daemon_queue: queue.Queue = queue.Queue()

        self += facts or []
        if CLEAR_DB_ON_START:
            self.close()

        if self.start_daemon_process:
            self.daemon_process()

    def start_daemon(self):
        """
        Kick off a process that waits for things to insert into the fact collection.
        Use threads to do this because we can't necessarily serialize the things we'd need.
        """

        def _daemon() -> None:
            while 1:
                pass
            pass

        daemon_thread: threading.Thread = threading.Thread(target=_daemon)
        daemon_thread.start()

    def __iter__(self) -> Generator[AtomicFact]:
        """
        Iterate over the facts in this collection.

        Yields:
            AtomicFact: Each fact in the collection.
        """
        yield from self.values()

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
    def keys(self) -> Generator[str]:
        """ABC"""

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
        relevant_facts: set[AtomicFact] = set()
        for constraint in constraints:
            match constraint:
                case ConstraintNodeHasLabel():
                    for fact in self.nodes_with_label_facts(constraint.label):
                        if fact in relevant_facts:
                            continue
                        yield fact
                        relevant_facts.add(fact)
                case ConstraintNodeHasAttributeWithValue():
                    LOGGER.warning(
                        "relevant_facts: ConstraintNodeHasAttributeWithValue: %s",
                        constraint,
                    )
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
                    LOGGER.warning(
                        "relevant_facts: ConstraintRelationshipHasSourceNode: %s",
                        constraint,
                    )
                    for fact in self.relationship_has_source_node_facts():
                        if fact in relevant_facts:
                            continue
                        yield fact
                        relevant_facts.add(fact)
                case ConstraintRelationshipHasTargetNode():
                    LOGGER.warning(
                        "relevant_facts: ConstraintRelationshipHasTargetNode: %s",
                        constraint,
                    )
                    for fact in self.relationship_has_target_node_facts():
                        if fact in relevant_facts:
                            continue
                        yield fact
                        relevant_facts.add(fact)
                case ConstraintRelationshipHasLabel():
                    LOGGER.warning(
                        "relevant_facts: ConstraintRelationshipHasLabel: %s",
                        constraint,
                    )
                    for fact in self.relationship_has_label_facts():
                        if (
                            fact in relevant_facts
                            or fact.relationship_label != constraint.label
                        ):
                            continue
                        yield fact
                        relevant_facts.add(fact)
                case ConstraintVariableRefersToSpecificObject():
                    LOGGER.warning(
                        "relevant_facts: ConstraintVariableRefersToSpecificObject: %s",
                        constraint,
                    )
                    pass
                case _:
                    raise ValueError(
                        f"Expected a ``Constraint``, but got {constraint.__class__.__name__}."
                    )

    @abstractmethod
    def append(self, fact: AtomicFact) -> None:
        """
        Append an AtomicFact to the facts list.

        Args:
            value (AtomicFact): The AtomicFact object to be appended.

        Returns:
            None
        """

    @abstractmethod
    def __contains__(self, fact: AtomicFact) -> bool:
        pass

    def __iadd__(self, other: AtomicFact | List[Any]) -> FactCollection:
        """Let us use ``+=`` to add facts to the collection."""
        if isinstance(other, AtomicFact):
            self.append(other)
        elif isinstance(other, list):
            for thing in other:
                FactCollection.__iadd__(self, thing)
        return self

    def relationships_with_specific_source_node_facts(
        self, source_node_id: str
    ) -> Generator[FactRelationshipHasSourceNode]:
        """
        Return a generator of facts that have a specific source node ID.
        """
        for fact in self.relationship_has_source_node_facts():
            if (
                isinstance(fact, FactRelationshipHasSourceNode)
                and fact.source_node_id == source_node_id
            ):
                yield fact

    def relationships_with_specific_target_node_facts(
        self, target_node_id: str
    ) -> Generator[FactRelationshipHasTargetNode]:
        """
        Return a generator of facts that have a specific target node ID.
        """
        for fact in self.relationship_has_target_node_facts():
            if (
                isinstance(fact, FactRelationshipHasTargetNode)
                and fact.target_node_id == target_node_id
            ):
                yield fact

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

    def query_node_label(self, query: QueryNodeLabel) -> str:
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
                attribute: None
                for attribute in attributes_of_label_dict[label]
            }
            for attribute in attributes_of_label_dict[label]:
                attribute_value = self.attributes_for_specific_node(
                    node_id, attribute
                )
                row_dict.update(attribute_value)
            row_dict["__label__"] = label
            row_dict["__node_id__"] = node_id
            yield row_dict

    def nodes_with_label_facts(
        self, label: str
    ) -> Generator[FactNodeHasLabel]:
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
