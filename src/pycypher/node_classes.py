from __future__ import annotations


from pycypher.tree_mixin import TreeMixin
from typing import List, Generator, Any, Optional
from pycypher.logger import LOGGER
from pycypher.solver import Constraint, ConstraintNodeHasLabel, ConstraintNodeHasAttributeWithValue, IsTrue
from rich.tree import Tree

class Cypher(TreeMixin):
    def __init__(self, cypher: TreeMixin):
        self.cypher = cypher
        self.aggregated_constraints: List[Constraint] = []

    def gather_constraints(self) -> None:
        for node in self.walk():
            LOGGER.debug(f"Walking node: {node}")
            try:
                node_constraints = getattr(node, "constraints", [])
                LOGGER.debug(f"Got constraints: {node_constraints}: {len(node_constraints)}")
                self.aggregated_constraints += getattr(node, "constraints", [])
            except Exception as err:
                LOGGER.error(err)
                import pdb

                pdb.set_trace()
        LOGGER.info(f"Got constraints: {self.aggregated_constraints}")

    @property
    def children(self) -> Generator[TreeMixin]:
        yield self.cypher

    def __repr__(self) -> str:
        return f"Cypher({self.cypher})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.cypher.tree())
        return t


class Query(TreeMixin):
    def __init__(self, match_clause: Match, return_clause: Return):
        self.match_clause = match_clause
        self.return_clause = return_clause

    @property
    def children(self) -> Generator[Match | Return]:
        yield self.match_clause
        yield self.return_clause

    def __repr__(self) -> str:
        return f"Query({self.match_clause}, {self.return_clause})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.match_clause.tree())
        t.add(self.return_clause.tree())
        return t


class Predicate(TreeMixin):
    def __init__(
        self, left_side: TreeMixin, operator: TreeMixin, right_side: TreeMixin
    ):
        self.left_side = left_side
        self.operator = operator
        self.right_side = right_side

    @property
    def children(self) -> Generator[TreeMixin]:
        yield self.left_side
        yield self.right_side

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.left_side}, {self.right_side})"
        )

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class Equals(Predicate):
    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class LessThan(Predicate):
    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class GreaterThan(Predicate):
    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class ObjectAttributeLookup(TreeMixin):
    def __init__(self, object: str, attribute: str):
        self.object = object
        self.attribute = attribute

    def __repr__(self) -> str:
        return f"ObjectAttributeLookup({self.object}, {self.attribute})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.object)
        t.add(self.attribute)
        return t

    @property
    def children(self) -> Generator[str]:
        yield self.object
        yield self.attribute


class Alias(TreeMixin):
    def __init__(self, reference: str, alias: str):
        self.reference = reference
        self.alias = alias

    def __repr__(self):
        return f"Alias({self.reference}, {self.alias})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(
            self.reference.tree()
            if isinstance(self.reference, ObjectAttributeLookup)
            else self.reference
        )
        t.add(self.alias)
        return t

    @property
    def children(self):
        yield self.reference
        yield self.alias


class Match(TreeMixin):
    def __init__(self, pattern: TreeMixin, where: Optional[TreeMixin] = None):
        self.pattern = pattern
        self.where = where

    def __repr__(self) -> str:
        return (
            f"Match({self.pattern})"
            if not self.where
            else f"Match({self.pattern}, {self.where})"
        )

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.pattern.tree())
        if self.where:
            t.add(self.where.tree())
        return t

    @property
    def children(self):
        yield self.pattern
        if self.where:
            yield self.where


class Return(TreeMixin):
    def __init__(self, node: Projection):
        self.projection = node

    def __repr__(self):
        return f"Return({self.projection})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.projection.tree())
        return t

    @property
    def children(self):
        yield self.projection


class Projection(TreeMixin):
    def __init__(self, lookups: List[ObjectAttributeLookup] | None = None):
        self.lookups = lookups or []

    def __repr__(self):
        return f"Projection({self.lookups})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        for lookup in self.lookups:
            t.add(lookup.tree())
        return t

    @property
    def children(self):
        for lookup in self.lookups:
            yield lookup


class NodeNameLabel(TreeMixin):
    def __init__(self, name: str, label: Optional[str] = None):
        self.name = name
        self.label = label

    def __repr__(self):
        return f"NodeNameLabel({self.name}, {self.label})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        if self.name:
            t.add(self.name)
        if self.label:
            t.add(self.label)
        return t

    @property
    def children(self) -> Generator[str]:
        yield self.name
        if self.label:
            yield self.label


class Node(TreeMixin):
    def __init__(
        self,
        node_name_label: NodeNameLabel,
        mapping_list: Optional[List[Mapping]] = None,
    ):
        self.node_name_label = node_name_label
        self.mapping_list: List[Mapping] | List[None] = mapping_list or []

    @property
    def constraints(self):
        constraint_list: List[Constraint] = []
        if self.node_name_label.label:
            constraint_list.append(
                ConstraintNodeHasLabel(
                    self.node_name_label.name, self.node_name_label.label
                )
            )
        return constraint_list or []

    def __repr__(self):
        return f"Node({self.node_name_label}, {self.mapping_list})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        if self.node_name_label:
            t.add(self.node_name_label.tree())
        if self.mapping_list:
            t.add(self.mapping_list.tree())
        return t

    @property
    def children(self) -> Generator[NodeNameLabel | Mapping]:
        if self.node_name_label:
            yield self.node_name_label
        if self.mapping_list:
            for mapping in self.mapping_list.mappings:
                yield mapping


class Relationship(TreeMixin):
    def __init__(self, name: str | NodeNameLabel):
        self.name = name

    def __repr__(self):
        return f"Relationship({self.name})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.name.tree())
        return t

    @property
    def children(self):
        yield self.name

    @property
    def constraints(self):
        relationship_chain = self.parent.parent
        nodes = [
            (
                relationship_chain.steps[i - 1],
                relationship_chain.steps[i + 1],
            )
            for i in range(len(relationship_chain.steps))
            if relationship_chain.steps[i] is self.parent
        ]
        return []  # TODO


class Mapping(TreeMixin):  # This is not complete
    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value

    def __repr__(self):
        return f"Mapping({self.key}:{self.value})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.key)
        t.add(str(self.value))
        return t

    @property
    def constraints(self):
        return [
            ConstraintNodeHasAttributeWithValue(
                self.parent.node_name_label.name, self.key, self.value
            )
        ]

    @property
    def children(self):
        yield self.key
        yield self.value


class MappingSet(TreeMixin):
    def __init__(self, mappings: List[Mapping]):
        self.mappings: List[Mapping] = mappings

    def __repr__(self) -> str:
        return f"MappingSet({self.mappings})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        for mapping in self.mappings:
            t.add(mapping.tree())
        return t

    @property
    def children(self) -> Generator[Mapping]:
        for mapping in self.mappings:
            yield mapping


class MatchList(TreeMixin):  # Not yet being used
    def __init__(self, match_list: List[Match] | None):
        self.match_list = match_list or []

    def __repr__(self) -> str:
        return f"MatchList({self.match_list})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        for match_clause in self.match_list:
            t.add(match_clause.tree())
        return t


class RelationshipLeftRight(TreeMixin):
    def __init__(self, relationship: Relationship):
        self.relationship = relationship

    def __repr__(self) -> str:
        return f"LeftRight({self.relationship})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.relationship.tree())
        return t

    @property
    def children(self) -> Generator[Relationship]:
        yield self.relationship


class RelationshipRightLeft(TreeMixin):
    def __init__(self, relationship: Relationship):
        self.relationship = relationship

    def __repr__(self):
        return f"RightLeft({self.relationship})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.relationship.tree())
        return t

    @property
    def children(self):
        yield self.relationship


class RelationshipChain(TreeMixin):
    def __init__(self, steps: List[TreeMixin]):
        self.steps = steps

    def __repr__(self):
        return f"RelationshipChain({self.steps})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        for step in self.steps:
            t.add(step.tree())
        return t

    @property
    def children(self) -> Generator[TreeMixin]:
        for step in self.steps:
            yield step


class Where(TreeMixin):
    def __init__(self, predicate: Predicate):
        self.predicate = predicate

    def __repr__(self):
        return f"Where({self.predicate})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.predicate.tree())
        return t

    @property
    def constraints(self):
        return [IsTrue(self.predicate)]


class BinaryBoolean(TreeMixin):
    def __init__(
        self,
        left_side: Predicate | Literal,
        operator: str,
        right_side: Predicate | Literal,
    ):
        self.left_side = left_side
        self.operator = operator
        self.right_side = right_side

    def __repr__(self):
        return (
            f"{self.__class__.__name__}({self.left_side}, {self.right_side})"
        )

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class And(BinaryBoolean):
    def __init__(
        self, left_side: Predicate | Literal, right_side: Predicate | Literal
    ):
        self.left_side = left_side
        self.right_side = right_side

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class Or(BinaryBoolean):
    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class RelationshipChainList(TreeMixin):
    def __init__(self, relationships: List[RelationshipChain]):
        self.relationships = relationships

    def __repr__(self):
        return f"RelationshipChainList({self.relationships})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        for relationship in self.relationships:
            t.add(relationship.tree())
        return t

    @property
    def children(self) -> Generator[RelationshipChain]:
        for relationship in self.relationships:
            yield relationship


class Literal(TreeMixin):
    def __init__(self, value: Any):
        self.value = value

    def __repr__(self):
        return f"Literal({self.value})"

    def __hash__(self):
        return hash(self.value)

    def _tree(self):
        t = Tree(self.__class__.__name__)
        t.add(str(self.value))
        return t

    def __eq__(self, other):
        return isinstance(other, Literal) and self.value == other.value
