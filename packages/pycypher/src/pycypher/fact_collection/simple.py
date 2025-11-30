"""
Simple FactCollection
=====================

This is just a list of facts, used only for dev testing. If you're not developing
``pycypher``, you almost certainly have no use for this class.
"""

from __future__ import annotations

from typing import Generator, List, Optional

from pycypher.fact import (
    AtomicFact,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactNodeRelatedToNode,
    FactRelationshipHasAttributeWithValue,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from pycypher.fact_collection import FactCollection


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
    ):
        """
        Initialize a FactCollection instance.

        Args:
            facts (Optional[List[AtomicFact]]): A list of AtomicFact instances. Defaults to an empty list if None is provided.
            session (Optional[Session]): The session this fact collection belongs to. Defaults to None.
        """
        self.facts: List[AtomicFact] = facts or []
        super().__init__()
        # super().__init__(facts)

    def keys(self) -> Generator[AtomicFact]:
        """
        Iterate over the facts in this collection.

        Yields:
            AtomicFact: Each fact in the collection.
        """
        yield from self.facts

    def __contains__(self, fact: AtomicFact) -> bool:
        """
        Check if a fact is present in the collection.

        Args:
            fact (AtomicFact): The fact to check for.

        Returns:
            bool: True if the fact is present, False otherwise.
        """
        return fact in self.facts

    def __iter__(self) -> Generator[AtomicFact]:
        """
        Iterate over the facts in this collection.
        """
        yield from self.facts

    def close(self):
        """Vacuously satisfy the interface"""

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

    def append(self, fact: AtomicFact):
        """
        Append an AtomicFact to the facts list.

        Args:
            value (AtomicFact): The AtomicFact object to be appended.

        Returns:
            None
        """
        self.facts.append(fact)

    def is_empty(self) -> bool:
        """
        Check if the fact collection is empty.

        Returns:
            bool: True if the fact collection is empty, False otherwise.
        """
        return len(self.facts) == 0

    def make_index_for_fact(self, fact: AtomicFact) -> bytes:
        """
        Create an index for a given fact.

        Args:
            fact (AtomicFact): The fact for which to create an index.

        Returns:
            bytes: The index for the fact.
        """
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
