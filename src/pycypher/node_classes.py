"""
The AST that is generated by the parser has nodes taken from this module.
"""

from __future__ import annotations

import abc
import uuid
from typing import Any, Generator, List, Optional

from pydantic import PositiveFloat, PositiveInt, TypeAdapter
from rich.tree import Tree

from pycypher.exceptions import WrongCypherTypeError
from pycypher.fact import FactCollection
from pycypher.solver import (
    Constraint,
    ConstraintNodeHasAttributeWithValue,
    ConstraintNodeHasLabel,
    ConstraintRelationshipHasLabel,
    ConstraintRelationshipHasSourceNode,
    ConstraintRelationshipHasTargetNode,
    IsTrue,
)
from pycypher.tree_mixin import TreeMixin


class Evaluable(abc.ABC):
    """Predicates, boolean operators, and arithmetic operators are all evaluable."""

    @abc.abstractmethod
    def _evaluate(self, fact_collection: FactCollection) -> Any:
        pass

    def evaluate(self, *args):
        """Calls the `_evaluate` method and returns the value of the `Literal` object."""
        return self._evaluate(*args).value


class Cypher(TreeMixin):
    """The root node of the AST."""

    def __init__(self, cypher: TreeMixin):
        self.cypher = cypher
        self.aggregated_constraints: List[Constraint] = []

    def trigger_gather_constraints_to_match(self):
        aggregated_constraints = []
        for node in self.walk():
            if isinstance(node, Match):
                node.gather_constraints()  # node.aggregated_constraints is list of constraints
                aggregated_constraints.extend(node.aggregated_constraints)
        self.aggregated_constraints = aggregated_constraints

    @property
    def children(self) -> Generator[TreeMixin]:
        yield self.cypher

    def __repr__(self) -> str:
        return f"Cypher({self.cypher})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.cypher.tree())
        return t


class Aggregation(TreeMixin):
    """Anything that turns a list into a singleton. Collect, Add, etc."""

    def __init__(self, aggregation):
        self.aggregation = aggregation

    @property
    def children(self):
        yield self.aggregation

    def __repr__(self):
        return f"Aggregation({self.aggregation})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.aggregation.tree())
        return t


class Collect(TreeMixin):
    """The COLLECT keyword"""

    def __init__(self, object_attribute_lookup: ObjectAttributeLookup):
        self.object_attribute_lookup = object_attribute_lookup

    @property
    def children(self):
        yield self.object_attribute_lookup

    def __repr__(self):
        return f"Collect({self.object_attribute_lookup})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.object_attribute_lookup.tree())
        return t


class Query(TreeMixin):
    """The node that represents the entire query."""

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
    """
    A node of the AST that represents a unary or binary expression
    with a definite truth value.

    This class is not instantiated directly, but is subclassed by specific
    operators.
    """

    left_side_types = Any
    right_side_types = Any
    argument_types = Any

    def __init__(self, left_side: TreeMixin, right_side: TreeMixin):
        self.left_side = left_side
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

    def _type_check_binary(self, left_value, right_value):
        try:
            TypeAdapter(self.__class__.left_side_types).validate_python(
                left_value
            )
        except:
            raise WrongCypherTypeError(  # pylint: disable=raise-missing-from
                f"Expected {self.left_side_types}, got {type(left_value)}"
            )
        try:
            TypeAdapter(self.__class__.right_side_types).validate_python(
                right_value
            )
        except:
            raise WrongCypherTypeError(  # pylint: disable=raise-missing-from
                f"Expected {self.right_side_types}, got {type(right_value)}"
            )

    def _type_check_unary(self, value):
        try:
            TypeAdapter(self.__class__.argument_types).validate_python(value)
        except:
            raise WrongCypherTypeError(  # pylint: disable=raise-missing-from
                f"Expected {self.left_side_types}, got {type(value)}"
            )

    def type_check(self, *args):
        args = [arg.value if isinstance(arg, Literal) else arg for arg in args]
        if len(args) == 1:
            self._type_check_unary(args[0])
        elif len(args) == 2:
            self._type_check_binary(args[0], args[1])
        else:
            raise ValueError("Expected one or two arguments")


class BinaryBoolean(Predicate, TreeMixin):
    """Superclass for the ``And`` and ``Or`` connective classes."""

    left_side_types = bool
    right_side_types = bool

    def __init__(  # pylint: disable=super-init-not-called
        self,
        left_side: Predicate | Literal,
        right_side: Predicate | Literal,
    ):
        self.left_side = left_side
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


class Equals(BinaryBoolean, Evaluable):
    """Binary infix operator for equality."""

    left_side_types = int | float | str | bool
    right_side_types = int | float | str | bool

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(self, fact_collection: FactCollection) -> Any:
        left_value = self.left_side._evaluate(fact_collection)  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(fact_collection)  # pylint: disable=protected-access
        self.type_check(left_value, right_value)
        return Literal(left_value.value == right_value.value)


class LessThan(Predicate, Evaluable):
    """Binary infix operator for less than."""

    left_side_types = int | float
    right_side_types = int | float

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(self, fact_collection: FactCollection) -> Any:
        left_value = self.left_side._evaluate(fact_collection)  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(fact_collection)  # pylint: disable=protected-access
        self.type_check(left_value, right_value)
        return Literal(left_value.value < right_value.value)


class GreaterThan(Predicate, Evaluable):
    """Binary infix operator for greater than."""

    left_side_types = int | float
    right_side_types = int | float

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(self, fact_collection: FactCollection) -> Any:
        left_value = self.left_side._evaluate(fact_collection)  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(fact_collection)  # pylint: disable=protected-access
        self.type_check(left_value, right_value)
        return Literal(left_value.value > right_value.value)


class Addition(Predicate, Evaluable):
    """Binary infix operator for addition."""

    left_side_types = int | float
    right_side_types = int | float

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(self, fact_collection: FactCollection) -> Any:
        left_value = self.left_side._evaluate(fact_collection)  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(fact_collection)  # pylint: disable=protected-access
        self.type_check(left_value, right_value)
        return Literal(left_value.value + right_value.value)


class Subtraction(Predicate, Evaluable):
    """Binary infix operator for subtraction."""

    left_side_types = float | int
    right_side_types = float | int

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(self, fact_collection: FactCollection) -> Any:
        left_value = self.left_side._evaluate(fact_collection)  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(fact_collection)  # pylint: disable=protected-access
        self.type_check(left_value, right_value)
        return Literal(left_value.value - right_value.value)


class Multiplication(Predicate, Evaluable):
    """Binary infix operator for multiplication."""

    left_side_types = float | int
    right_side_types = float | int

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(self, fact_collection: FactCollection) -> Any:
        left_value = self.left_side._evaluate(fact_collection)  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(fact_collection)  # pylint: disable=protected-access
        self.type_check(left_value, right_value)
        return Literal(left_value.value * right_value.value)


class Division(Predicate, Evaluable):
    """Binary infix operator for division."""

    left_side_types = int | float
    right_side_types = PositiveFloat | PositiveInt

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(self, fact_collection: FactCollection) -> Any:
        left_value = self.left_side._evaluate(fact_collection)  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(fact_collection)  # pylint: disable=protected-access
        self.type_check(left_value, right_value)
        return Literal(left_value.value / right_value.value)


class ObjectAttributeLookup(TreeMixin):
    """A node that represents the value of an attribute of a node or relationship
    of the form ``node.attribute``.
    """

    def __init__(self, object_name: str, attribute: str):
        self.object = object_name
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

    def value(self, fact_collection: FactCollection) -> Any:
        """
        Need to find reference of variable from previous Match clause.
        Then look up the attribute for that object from the FactCollection.
        """
        return fact_collection.get_attribute(self.object, self.attribute)


class Alias(TreeMixin):
    """A node representing use of an ``AS`` statement in the query."""

    def __init__(self, reference: str, alias: str):
        self.reference = reference
        self.alias = alias

    def __repr__(self):
        return f"Alias({self.reference}, {self.alias})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(
            self.reference.tree()
            if isinstance(self.reference, TreeMixin)
            else self.reference
        )
        t.add(self.alias)
        return t

    @property
    def children(self):
        yield self.reference
        yield self.alias


class ObjectAs(TreeMixin):
    """Basically an alias. Might be redundant."""

    def __init__(
        self, object_attribute_lookup: ObjectAttributeLookup | str, alias: str
    ):
        if isinstance(object_attribute_lookup, str):
            object_attribute_lookup = ObjectAttributeLookup(
                object_attribute_lookup, None
            )
        self.object_attribute_lookup = object_attribute_lookup
        self.alias = alias

    def __repr__(self):
        return f"ObjectAs({self.object_attribute_lookup}, {self.alias})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.object_attribute_lookup.tree())
        t.add(self.alias)
        return t

    @property
    def children(self) -> Generator[Projection | Alias]:
        yield self.object_attribute_lookup
        yield self.alias


class ObjectAsSeries(TreeMixin):
    """Basically an alias. Might be redundant."""

    def __init__(self, object_attribute_lookup_list: List[ObjectAs]):
        self.object_attribute_lookup_list = object_attribute_lookup_list

    def __repr__(self):
        return f"ObjectAsSeries({self.object_attribute_lookup_list})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        for object_attribute_lookup in self.object_attribute_lookup_list:
            t.add(object_attribute_lookup.tree())
        return t

    @property
    def children(self) -> Generator[Projection | Alias]:
        yield self.object_attribute_lookup_list


class WithClause(TreeMixin):
    """The ``WITH`` clause of the query, which is a set of projections."""

    def __init__(self, object_as_series: ObjectAsSeries):
        self.object_as_series = object_as_series

    def __repr__(self):
        return f"WithClause({self.object_as_series})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.object_as_series.tree())
        return t

    @property
    def children(self) -> Generator[Projection]:
        yield self.object_as_series


class Match(TreeMixin):
    """The node that represents the ``MATCH`` clause of the query. This includes
    any applicable ``WHERE`` clause, but not a ``RETURN`` clause.
    """

    def __init__(
        self,
        pattern: TreeMixin,
        where: Optional[TreeMixin] = None,
        with_clause: Optional[TreeMixin] = None,
    ):
        self.pattern = pattern
        self.where = where
        self.with_clause = with_clause
        self.aggregated_constraints = None

    def __repr__(self) -> str:
        return f"Match({self.pattern}, {self.where}, {self.with_clause})"

    def gather_constraints(self) -> None:
        """Gather all the ``Constraint`` objects from inside the ``Match`` clause."""
        self.aggregated_constraints = []
        for node in self.walk():
            self.aggregated_constraints += getattr(node, "constraints", [])

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.pattern.tree())
        if self.with_clause:
            t.add(self.with_clause.tree())
        if self.where:
            t.add(self.where.tree())
        return t

    @property
    def children(self):
        yield self.pattern
        if self.where:
            yield self.where


class Return(TreeMixin):
    """Represnents the ``RETURN`` clause, which is a set of projections."""

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
    """A node variable followed by an attribute, or another evaluable expression."""

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
        yield from self.lookups


class NodeNameLabel(TreeMixin):
    """A node name, optionally followed by a label, separated by a dot."""

    def __init__(
        self, name: Optional[str] = None, label: Optional[str] = None
    ):
        self.name = name or uuid.uuid4().hex
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
    """A node in the graph, which may contain a variable name, label, or mapping."""

    def __init__(
        self,
        node_name_label: NodeNameLabel,
        mapping_list: Optional[List[Mapping]] = None,
    ):
        self.node_name_label = node_name_label
        self.mapping_list: List[Mapping] | List[None] = mapping_list or []

    @property
    def constraints(self):
        """
        Hi
        """
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
    """Relationships may contain a variable name, label, or mapping."""

    def __init__(self, name_label: NodeNameLabel):
        self.name_label = (
            name_label  # This should be ``label`` for consistency
        )
        self.name = None

    def __repr__(self):
        return f"Relationship({self.name_label})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.name_label.tree())
        return t

    @property
    def children(self):
        yield self.name_label


class Mapping(TreeMixin):  # This is not complete
    """Mappings are dictionaries of key-value pairs."""

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
        """Generates the ``Constraint`` objects that correspond to this node."""
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
    """A list of mappings."""

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
        yield from self.mappings


class MatchList(TreeMixin):  # Not yet being used
    """Just a container for a list of ``Match`` objects."""

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
    """A ``Relationship`` with the arrow pointing from left to right. Note that there
    is no semantic difference between this and ``RelationshipRightLeft``."""

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

    @property
    def constraints(self):
        """Calculates the ``Constraint`` objects for this ``Relationship``."""
        constraint_list: List[Constraint] = []
        relationship_chain: RelationshipChain = self.parent
        nodes = [
            (
                relationship_chain.steps[i - 1],
                relationship_chain.steps[i + 1],
            )
            for i in range(len(relationship_chain.steps))
            if relationship_chain.steps[i] is self
        ]
        source_node_constraint = ConstraintRelationshipHasSourceNode(
            nodes[0][0].node_name_label.name,
            self.relationship.name_label.name,
        )
        target_node_constraint = ConstraintRelationshipHasTargetNode(
            nodes[0][1].node_name_label.name,
            self.relationship.name_label.name,
        )
        relationship_label_constraint = ConstraintRelationshipHasLabel(
            self.relationship.name_label.name,
            self.relationship.name_label.label,
        )
        constraint_list.append(source_node_constraint)
        constraint_list.append(target_node_constraint)
        constraint_list.append(relationship_label_constraint)
        return constraint_list


class RelationshipRightLeft(TreeMixin):
    """A ``Relationship`` with the arrow pointing from right to left. Note that there
    is no semantic difference between this and ``RelationshipLeftRight``."""

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
    """Several ``Relationship`` nodes chained together, sharing ``Node`` objects
    between them."""

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
        yield from self.steps


class Where(TreeMixin):
    """A node that represents the ``WHERE`` clause of the query, which is a
    single ``Predicate`` object prefixed with ``WHERE``. Multiple predicates
    are joined together with ``AND`` or ``OR``, so we can always think of them
    as a single predicate."""

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


class And(BinaryBoolean, Evaluable):
    """Boolean connective for logical AND."""

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(self, fact_collection: FactCollection) -> Any:
        left_value = self.left_side._evaluate(fact_collection)  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(fact_collection)  # pylint: disable=protected-access
        self.type_check(left_value, right_value)
        return Literal(left_value.value and right_value.value)


class Or(BinaryBoolean, Evaluable):
    """Boolean connective for logical OR."""

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(self, fact_collection: FactCollection) -> Any:
        left_value = self.left_side._evaluate(fact_collection)  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(fact_collection)  # pylint: disable=protected-access
        self.type_check(left_value, right_value)
        return Literal(left_value.value or right_value.value)


class Not(Evaluable, Predicate):
    """Boolean connective for logical NOT."""

    argument_types = bool

    def __init__(  # pylint: disable=super-init-not-called
        self,
        argument: Predicate | Literal,
    ):
        self.argument = argument

    def __repr__(self):
        return f"{self.__class__.__name__}({self.argument})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.argument.tree())
        return t

    def _evaluate(self, fact_collection: FactCollection) -> Any:
        value = self.argument._evaluate(fact_collection)  # pylint: disable=protected-access
        self.type_check(value)
        return Literal(not value.value)


class RelationshipChainList(TreeMixin):
    """Container for a list of relationship chains."""

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
        yield from self.relationships


class Literal(TreeMixin, Evaluable):
    """Simply a container for a value. This is the base case for evaluation
    of predicates, etc."""

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

    def _evaluate(self, _) -> Any:
        return Literal(self.value)

    def __eq__(self, other):
        return isinstance(other, Literal) and self.value == other.value
