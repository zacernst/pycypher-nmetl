from __future__ import annotations

from collections.abc import MutableSequence
from typing import Any, Generator, List

from pycypher.solver import (
    Constraint,
    ConstraintNodeHasAttributeWithValue,
    ConstraintNodeHasLabel,
)


class AtomicFact:
    def satisfies(self, constraint: Constraint) -> bool:
        return self._satisfies(constraint)


class FactNodeHasLabel(AtomicFact):
    def __init__(self, node_id: str, node_label: str):
        self.node_id = node_id
        self.label = node_label

    def __repr__(self):
        return f"NodeHasLabel: {self.node_id} {self.label}"

    def __eq__(self, other):
        return (
            isinstance(other, FactNodeHasLabel)
            and self.node_id == other.node_id
            and self.label == other.label
        )

    def _satisfies(self, constraint: Constraint) -> bool:
        if not isinstance(constraint, ConstraintNodeHasLabel):
            return False
        return bool(
            constraint.node_id == self.node_id
            and constraint.label == self.label
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

    def __repr__(self):
        return f"NodeHasAttributeWithValue: {self.node_id} {self.attribute} {self.value}"

    def _satisfies(self, constraint: Constraint) -> bool:
        if not isinstance(constraint, ConstraintNodeHasAttributeWithValue):
            return False
        return bool(
            constraint.node_id == self.node_id
            and constraint.attribute == self.attribute
            and constraint.value == self.value
        )

    def __eq__(self, other):
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

    def __repr__(self):
        return f"NodeRelatedToNode: {self.node1_id} {self.relationship_label} {self.node2_id}"

    def _satisfies(self, constraint: Constraint) -> bool:
        if not isinstance(constraint, FactNodeRelatedToNode):
            return False
        return bool(
            constraint.node1_id == self.node1_id
            and constraint.node2_id == self.node2_id
            and constraint.relationship_label == self.relationship_label
        )

    def __eq__(self, other):
        return (
            isinstance(other, FactNodeRelatedToNode)
            and self.node1_id == other.node1_id
            and self.node2_id == other.node2_id
            and self.relationship_label == other.relationship_label
        )


class FactCollection(MutableSequence):
    def __init__(self, facts: List[AtomicFact]):
        self.facts = facts

    def __iter__(self) -> Generator[AtomicFact]:
        for fact in self.facts:
            yield fact

    def __repr__(self):
        return f"FactCollection: {len(self.facts)}"

    def satisfies(self, constraint: Constraint) -> bool:
        return any(fact.satisfies(constraint) for fact in self.facts)

    def __getitem__(self, index):
        return self.facts[index]

    def __setitem__(self, index, value):
        self.facts[index] = value

    def __delitem__(self, index):
        del self.facts[index]

    def __len__(self):
        return len(self.facts)

    def insert(self, index, value):
        self.facts.insert(index, value)
