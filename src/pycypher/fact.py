"""
Facts are simple atomic statements that have a truth value.
"""

from __future__ import annotations

from typing import Any, Generator, List


class AtomicFact:
    """Astract base class for specific types of ``Fact``."""

    pass


class FactNodeHasLabel(AtomicFact):
    def __init__(self, node_id: str, node_label: str):
        self.node_id = node_id
        self.label = node_label

    def __repr__(self):
        return f"NodeHasLabel: {self.node_id} {self.label}"

    def __eq__(self, other: Any):
        return (
            isinstance(other, FactNodeHasLabel)
            and self.node_id == other.node_id
            and self.label == other.label
        )


class FactRelationshipHasLabel(AtomicFact):
    def __init__(self, relationship_id: str, relationship_label: str):
        self.relationship_id = relationship_id
        self.relationship_label = relationship_label

    def __repr__(self):
        return (
            f"NodeHasLabel: {self.relationship_id} {self.relationship_label}"
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FactRelationshipHasLabel)
            and self.relationship_id == other.relationship_id
            and self.relationship_label == other.relationship_label
        )


class FactNodeHasAttributeWithValue(AtomicFact):
    def __init__(self, node_id: str, attribute: str, value: Any):
        self.node_id = node_id
        self.attribute = attribute
        self.value = value

    def __hash__(self) -> int:
        return hash(
            "NodeHasAttributeWithValue"
            + self.node_id.__str__()
            + self.attribute.__str__()
            + self.value.__str__()
        )

    def __repr__(self) -> str:
        return f"NodeHasAttributeWithValue: {self.node_id} {self.attribute} {self.value}"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FactNodeHasAttributeWithValue)
            and (self.node_id == other.node_id)
            and (self.attribute == other.attribute)
            and (self.value == other.value)
        )


class FactNodeRelatedToNode(AtomicFact):
    def __init__(self, node1_id: str, node2_id: str, relationship_label: str):
        self.node1_id = node1_id
        self.node2_id = node2_id
        self.relationship_label = relationship_label

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
    def __init__(self, relationship_id: str, source_node_id: str):
        self.relationship_id = relationship_id
        self.source_node_id = source_node_id

    def __repr__(self) -> str:
        return f"RelationshipHasSourceNode: {self.relationship_id} {self.source_node_id}"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FactRelationshipHasSourceNode)
            and self.relationship_id == other.relationship_id
            and self.source_node_id == other.source_node_id
        )


class FactRelationshipHasTargetNode(AtomicFact):
    def __init__(self, relationship_id: str, target_node_id: str):
        self.relationship_id = relationship_id
        self.target_node_id = target_node_id

    def __repr__(self) -> str:
        return f"RelationshipHasTargetNode: {self.relationship_id} {self.target_node_id}"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FactRelationshipHasTargetNode)
            and self.relationship_id == other.relationship_id
            and self.target_node_id == other.target_node_id
        )


class FactCollection:
    def __init__(self, facts: List[AtomicFact]):
        self.facts: List[AtomicFact] = facts

    def __iter__(self) -> Generator[AtomicFact]:
        for fact in self.facts:
            yield fact

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

    def insert(self, index: int, value: AtomicFact):
        self.facts.insert(index, value)
