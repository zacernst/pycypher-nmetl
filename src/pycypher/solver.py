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
    def __init__(self, source_node_name: str, relationship_name: str):
        self.source_node_name = source_node_name
        self.relationship_name = relationship_name

    def __repr__(self):
        return f"RelationshipHasSourceNode: {self.relationship_name} {self.source_node_name}"

    def __hash__(self) -> int:
        return hash(
            "RelationshipHasSourceNode"
            + self.relationship_name.__str__()
            + self.source_node_name.__str__()
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintRelationshipHasSourceNode)
            and self.source_node_name == other.source_node_name
            and self.relationship_name == other.relationship_name
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
