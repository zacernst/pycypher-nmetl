from __future__ import annotations

from abc import abstractmethod
from typing import Any


class State:
    pass


class Constraint:
    @abstractmethod
    def eval(self, *args, **kwargs) -> Any | None:  # type: ignore
        pass


class IsTrue(Constraint):
    """
    Class to represent a constraint that merely says that a ``Predicate`` is true.
    """

    def __init__(self, predicate: "Predicate"):  # type: ignore
        self.predicate = predicate

    def eval(self, *_) -> bool | None:
        pass

    def __repr__(self) -> str:
        return f"IsTrue({self.predicate})"


class ConstraintNodeHasLabel(Constraint):
    def __init__(self, node_id: str, label: str):
        self.node_id = node_id
        self.label = label

    def __repr__(self):
        return f"HasLabel: {self.node_id} {self.label}"

    # def eval(self, state: State) -> bool | None:
    #     pass

    def __hash__(self) -> int:
        return hash("HasLabel" + self.node.__str__() + self.label.__str__())

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintNodeHasLabel)
            and self.node == other.node
            and self.label == other.label
        )


class ConstraintRelationshipHasSourceNode(Constraint):
    def __init__(self, source_node_id: str, relationship_id: str):
        self.source_node_id = source_node_id
        self.relationship_id = relationship_id

    def __repr__(self):
        return f"RelatedTo: {self.source_node_id} {self.relationship_id}"

    def __hash__(self) -> int:
        return hash(
            "RelatedTo"
            + self.source_node_id.__str__()
            + self.relationship_id.__str__()
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintRelationshipHasSourceNode)
            and self.source_node_id == other.source_node_id
            and self.relationship_id == other.relationship_id
        )


class ConstraintNodeHasAttributeWithValue(Constraint):
    def __init__(self, node_id: str, attribute: str, value: Any):
        self.node_id = node_id
        self.attribute = attribute
        self.value = value

    def __repr__(self):
        return f"HasAttributeWithValue: [{self.node_id}] {self.attribute}: {self.value}"

    def __hash__(self) -> int:
        return hash(
            "HasAttributeWithValue"
            + self.node_id
            + self.attribute
            + str(self.value)
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintNodeHasAttributeWithValue)
            and self.node_id == other.node_id
            and self.attribute == other.attribute
            and self.value == other.value
        )  # noqa: E501
