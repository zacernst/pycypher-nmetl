"""
Fact Module Documentation (fact.py)
===================================

The ``fact.py`` module within the ``pycypher`` library defines the core classes for
representing and managing "facts" within a graph-like data structure. Facts are
atomic pieces of information about nodes, relationships, and their attributes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generator

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


class KeyValue(ABC):
    """Mixin for key-value stores"""

    def make_index_for_fact(self, fact: AtomicFact) -> str | bytes:
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
        """Used for the memcache index"""
        match fact:
            case FactNodeHasLabel():
                out: str = (
                    f"node_label:{fact.label}::{fact.node_id}:{fact.label}"
                )
            case FactNodeHasAttributeWithValue():
                out: str = f"node_attribute:{fact.node_id}:{fact.attribute}:{fact.value}"
            case FactRelationshipHasLabel():
                out: str = f"relationship_label:{fact.relationship_id}:{fact.relationship_label}"
            case FactRelationshipHasAttributeWithValue():
                out: str = f"relationship_attribute:{fact.relationship_id}:{fact.attribute}:{fact.value}"
            case FactRelationshipHasSourceNode():
                out: str = f"relationship_source_node:{fact.relationship_id}:{fact.source_node_id}"
            case FactRelationshipHasTargetNode():
                out: str = f"relationship_target_node:{fact.relationship_id}:{fact.target_node_id}"
            case FactNodeRelatedToNode():
                out: str = f"node_relationship:{fact.node1_id}:{fact.node2_id}:{fact.relationship_label}"
            case _:
                raise ValueError(f"Unknown fact type {fact}")
        return out

    @abstractmethod
    def keys(self) -> Generator[str]:
        """ABC"""

    @abstractmethod
    def values(self) -> Generator[AtomicFact]:
        """ABC"""

    def __iterkeys__(self) -> Generator[Any]:
        yield from self.keys()

    def __itervalues__(self) -> Generator[Any]:
        yield from self.values()

    def __iteritems__(self) -> Generator[Any]:
        yield from zip(self.keys(), self.values())
