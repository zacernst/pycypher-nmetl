"""
This module defines various classes representing nodes and expressions in an
Abstract Syntax Tree (AST) for Cypher queries.
"""  # pylint: disable=too-many-lines

from __future__ import annotations

import abc
import copy
import uuid
from functools import partial
from typing import Any, Dict, Generator, List, Optional

from constraint import Domain, Problem
from pycypher.exceptions import WrongCypherTypeError
from pycypher.fact import (
    FactCollection,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from pycypher.logger import LOGGER
from pycypher.query import NullResult, QueryValueOfNodeAttribute
from pycypher.shims import Shim
from pycypher.solver import (
    Constraint,
    ConstraintNodeHasAttributeWithValue,
    ConstraintNodeHasLabel,
    ConstraintRelationshipHasLabel,
    ConstraintRelationshipHasSourceNode,
    ConstraintRelationshipHasTargetNode,
    ConstraintVariableRefersToSpecificObject,
    IsTrue,
)
from pycypher.tree_mixin import TreeMixin
from pydantic import (
    NegativeFloat,
    NegativeInt,
    PositiveFloat,
    PositiveInt,
    TypeAdapter,
)
from rich.tree import Tree

LOGGER.setLevel("DEBUG")


class Evaluable(abc.ABC):  # pylint: disable=too-few-public-methods
    """
    Abstract base class representing an evaluable entity.
    """

    @abc.abstractmethod
    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        pass

    def evaluate(self, *args, **kwargs):
        """Calls the `_evaluate` method and returns the value of the `Literal` object."""
        if any(isinstance(arg, NullResult) for arg in args):
            return NullResult(self)
        result = self._evaluate(*args, **kwargs)
        return getattr(result, "value", result)


class Cypher(TreeMixin):
    """
    The root node of the Abstract Syntax Tree (AST) for Cypher queries.

    Attributes:
        cypher (TreeMixin): The root node of the AST.

    """

    def __init__(self, cypher: TreeMixin):
        self.cypher = cypher

    def trigger_gather_constraints_to_match(self):
        """
        Triggers the gathering of constraints for all nodes of type 'Match' in the current context.

        This method iterates over all nodes returned by the `walk` method. For each node that is an
        instance of the `Match` class, it calls the `gather_constraints` method on that node.

        Returns:
            None
        """
        for node in self.walk():
            if isinstance(node, Match):
                node.gather_constraints()

    @property
    def children(self) -> Generator[TreeMixin]:
        """
        Generator function that yields the children of the current node.

        Yields:
            TreeMixin: The child node of the current node.
        """
        yield self.cypher

    def get_return_clause(self) -> Return:
        """
        Returns the Return clause of the Cypher query.
        """
        return self.cypher.return_clause

    def __repr__(self) -> str:
        """
        Return a string representation of the Cypher object.

        Returns:
            str: A string in the format "Cypher(<cypher>)" where <cypher> is the
                cypher attribute of the object.
        """
        return f"Cypher({self.cypher})"

    @property
    def attribute_names(self) -> List[str]:
        """
        Returns a list of attribute names from the Cypher object.

        Returns:
            List[str]: A list of attribute names.
        """
        out = [
            obj.attribute
            for obj in self.walk()
            if isinstance(obj, ObjectAttributeLookup)
        ]
        return out

    def tree(self) -> Tree:
        """
        Generates a tree representation of the current node.

        Returns:
            Tree: A tree object representing the current node and its Cypher tree.
        """
        t = Tree(self.__class__.__name__)
        t.add(self.cypher.tree())
        return t


class Aggregation(TreeMixin, Evaluable):
    """
    Represents an aggregation operation that transforms a list into a singleton value.

    Attributes:
        aggregation: The aggregation operation to be performed.

    """

    def __init__(self, aggregation):
        self.aggregation = aggregation

    @property
    def children(self):
        """
        Generator function that yields the aggregation attribute.

        Yields:
            The aggregation attribute of the instance.
        """
        yield self.aggregation

    def __repr__(self):
        """
        Return a string representation of the Aggregation object.

        Returns:
            str: A string in the format "Aggregation(<aggregation_value>)".
        """
        return f"Aggregation({self.aggregation})"

    def tree(self):
        """
        Generates a tree representation of the current node class.

        Returns:
            Tree: A tree object representing the current node class and its aggregation.
        """
        t = Tree(self.__class__.__name__)
        t.add(self.aggregation.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        """
        Evaluates the aggregation using the provided fact collection and projection.

        Args:
            fact_collection (FactCollection): The collection of facts to be used in the evaluation.
            projection (Optional[Dict[str, str | List[str]]]): An optional dictionary specifying
                the projection to be applied during the evaluation. The keys are the projection
                names, and the values are either a single string or a list of strings representing
                the projection fields.

        Returns:
            Any: The result of the aggregation evaluation.
        """
        return self.aggregation._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )


class Collection(TreeMixin, Evaluable):  # i.e. a list
    """
    A class representing a collection of evaluable items.

    Attributes:
        values (List[Evaluable]): A list of evaluable items.

    """

    def __init__(self, values: List[Evaluable]):
        self.values = values

    @property
    def children(self):
        """
        Generator that yields the values of the node.
        """

        yield from self.values

    def __repr__(self):
        """
        Return a string representation of the Collection object.
        Returns:
            str: A string that represents the Collection object, including its values.
        """

        return f"Collection({self.values})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        for value in self.values:
            t.add(value.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        """
        Evaluates the current node using the provided fact collection and projection.
        Args:
            fact_collection (FactCollection): The collection of facts to use for evaluation.
            projection (Optional[Dict[str, str | List[str]]]): An optional dictionary
                specifying the projection rules.
        Returns:
            Any: The result of the evaluation, wrapped in a Collection.
        """
        return Collection(
            [
                value._evaluate(fact_collection, projection=projection)  # pylint: disable=protected-access
                for value in self.values
            ]
        )

    def __eq__(self, other):
        return isinstance(other, Collection) and self.values == other.values


class Distinct(TreeMixin, Evaluable):
    """
    A class that represents a distinct operation on a collection, removing duplicate values.

    Attributes:
        collection (Collection): The collection to be evaluated and filtered for distinct values.

    """

    def __init__(self, collection: Collection):
        self.collection = collection

    @property
    def children(self):
        """
        Generator that yields the collection of children nodes.

        Yields:
            The collection of children nodes.
        """
        yield self.collection

    def __repr__(self):
        """
        Return a string representation of the Distinct object.

        Returns:
            str: A string in the format "Distinct(collection)" where `collection`
                 is the attribute of the object.
        """
        return f"Distinct({self.collection})"

    def tree(self):
        """
        Generates a tree representation of the current node class.

        Returns:
            Tree: A tree object representing the current node class and its collection.
        """
        t = Tree(self.__class__.__name__)
        t.add(self.collection.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        """
        Evaluate the collection and remove duplicates.

        Args:
            fact_collection (FactCollection): The collection of facts to evaluate.
            projection (Optional[Dict[str, str | List[str]]], optional): A dictionary
                specifying the projection. Defaults to None.

        Returns:
            Any: A new collection with duplicates removed.
        """
        collection = self.collection._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )
        new_values = []
        for value in collection.values:
            if value not in new_values:
                new_values.append(value)
        return Collection(new_values)


class Size(TreeMixin, Evaluable):
    """
    Represents a size operation on a collection, like a ``len`` in Python.

    Attributes:
        collection (Collection): The collection whose size is to be evaluated.

    """

    def __init__(self, collection: Collection):
        self.collection = collection

    @property
    def children(self):
        yield self.collection

    def __repr__(self):
        return f"Size({self.collection})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.collection.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        return Literal(
            len(
                self.collection._evaluate(  # pylint: disable=protected-access
                    fact_collection, projection=projection
                ).values
            )
        )


class Collect(TreeMixin, Evaluable):
    """
    A class that represents a collection of objects based on an attribute lookup.

    Attributes:
        object_attribute_lookup (ObjectAttributeLookup): The attribute lookup object
            used to collect instances.

    """

    def __init__(self, object_attribute_lookup: ObjectAttributeLookup):
        self.object_attribute_lookup = object_attribute_lookup

    @property
    def children(self):
        yield self.object_attribute_lookup

    def __repr__(self):
        """
        Return a string representation of the object.

        This method is used to provide a human-readable representation of the object,
        which can be useful for debugging and logging purposes.

        Returns:
            str: A string in the format "Collect({self.object_attribute_lookup})".
        """
        return f"Collect({self.object_attribute_lookup})"

    def tree(self):
        """
        Generates a tree representation of the current object.

        This method creates a Tree object with the name of the current class.
        It then adds the tree representation of the `object_attribute_lookup` attribute
        to the Tree object and returns it.

        Returns:
            Tree: A tree representation of the current object.
        """

        t = Tree(self.__class__.__name__)
        t.add(self.object_attribute_lookup.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        """
        Evaluates the node using the provided fact collection and projection.
        Args:
            fact_collection (FactCollection): The collection of facts to evaluate against.
            projection (Optional[Dict[str, str | List[str]]]): A dictionary specifying the
            projection of attributes.
        Returns:
            Any: The result of the evaluation.
        """

        matching_instances = projection[self.object_attribute_lookup.object]
        output = []
        for matching_instance in matching_instances:
            individuated_projection = copy.deepcopy(projection)
            individuated_projection[self.object_attribute_lookup.object] = (
                matching_instance
            )
            single_value = self.object_attribute_lookup._evaluate(  # pylint: disable=protected-access
                fact_collection, projection=individuated_projection
            )
            output.append(single_value)
        result = Collection(output)._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )
        return result


class Query(TreeMixin):
    """
    Represents a Cypher query consisting of a MATCH clause and a RETURN clause.

    Attributes:
        match_clause (Match): The MATCH clause of the query.
        return_clause (Return): The RETURN clause of the query.

    """

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
    A class representing a predicate in a tree structure.

    Attributes:
    left_side_types (Any): The expected type(s) for the left side of the predicate.
    right_side_types (Any): The expected type(s) for the right side of the predicate.
    argument_types (Any): The expected type(s) for the arguments of the predicate.
    left_side (TreeMixin): The left side of the predicate.
    right_side (TreeMixin): The right side of the predicate.
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
        return f"{self.__class__.__name__}({self.left_side}, {self.right_side})"

    def _type_check_binary(self, left_value, right_value):
        """
        Validates the types of the left and right values against the expected types
        defined in the class.
        Parameters:
        left_value: The value to be validated against the expected left side types.
        right_value: The value to be validated against the expected right side types.
        Raises:
        WrongCypherTypeError: If the type of left_value or right_value does not match
        the expected types defined in the class.
        """

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
        TypeAdapter(self.__class__.argument_types).validate_python(value)

    def type_check(self, *args):
        """
        Perform type checking on the provided arguments.
        This method processes the provided arguments, replacing instances of the
        `Literal` class with their `value` attribute. It then performs type checking
        based on the number of arguments provided.

        Args:
        args: Variable length argument list. Can be one or two arguments.

        Raises:
        ValueError: If the number of arguments is not one or two.
        """
        # Just return if anything is a NullResult
        if any(isinstance(arg, NullResult) for arg in args):
            return NullResult(self)
        args = [arg.value if isinstance(arg, Literal) else arg for arg in args]
        if len(args) == 1:
            self._type_check_unary(args[0])
        elif len(args) == 2:
            self._type_check_binary(args[0], args[1])
        else:
            raise ValueError("Expected one or two arguments")


class BinaryBoolean(Predicate, TreeMixin):
    """
    A class representing a binary boolean operation in a predicate tree.

    Attributes:
        left_side (Predicate | Literal): The left side of the binary boolean operation.
        right_side (Predicate | Literal): The right side of the binary boolean operation.

    """

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
        return f"{self.__class__.__name__}({self.left_side}, {self.right_side})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class AliasedName(TreeMixin):
    """
    A class representing an aliased name with tree structure capabilities.

    Attributes:
        name (str): The name to be aliased.

    """

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f"AliasedName({self.name})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.name)
        return t

    @property
    def children(self) -> Generator[str]:
        yield self.name


class Equals(BinaryBoolean, Evaluable):
    """Binary infix operator for equality."""

    left_side_types = int | float | str | bool
    right_side_types = int | float | str | bool

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        left_value = self.left_side.evaluate(
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        right_value = self.right_side.evaluate(
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        type_check_result = self.type_check(left_value, right_value)
        if isinstance(type_check_result, NullResult):
            return NullResult(self)
        return Literal(left_value == right_value)


class LessThan(Predicate, Evaluable):
    """Binary infix operator for less than."""

    left_side_types = int | float
    right_side_types = int | float

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        left_value = self.left_side.evaluate(
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        right_value = self.right_side.evaluate(
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        type_check_result = self.type_check(left_value, right_value)
        if isinstance(type_check_result, NullResult):
            return NullResult(self)
        return Literal(left_value < right_value)


class GreaterThan(Predicate, Evaluable):
    """Binary infix operator for greater than."""

    left_side_types = int | float
    right_side_types = int | float

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        """
        Evaluates the comparison between the left and right side expressions.

        Args:
            fact_collection (FactCollection): The collection of facts to evaluate against.
            projection (Optional[Dict[str, Union[str, List[str]]]], optional):
                A dictionary specifying the projection of fields. Defaults to None.

        Returns:
            Any: A Literal object representing the result of the comparison.
        """
        left_value = self.left_side.evaluate(
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        right_value = self.right_side.evaluate(
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        type_check_result = self.type_check(left_value, right_value)
        if isinstance(type_check_result, NullResult):
            return NullResult(self)

        return Literal(left_value > right_value)


class Subtraction(Predicate, Evaluable):
    """Binary subtraction operator"""

    left_side_types = float | int
    right_side_types = float | int

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        left_value = self.left_side.evaluate(
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        right_value = self.right_side.evaluate(
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        type_check_result = self.type_check(left_value, right_value)
        if isinstance(type_check_result, NullResult):
            return NullResult(self)
        return Literal(left_value - right_value)


class Multiplication(Predicate, Evaluable):
    """
    A class representing a multiplication operation in a predicate logic expression.

    Attributes:
        left_side_types (type): The allowed types for the left operand (float or int).
        right_side_types (type): The allowed types for the right operand (float or int).

    """

    left_side_types = float | int
    right_side_types = float | int

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        left_value = self.left_side._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        type_check_result = self.type_check(left_value, right_value)
        if isinstance(type_check_result, NullResult):
            return NullResult(self)
        return Literal(left_value.value * right_value.value)


class Division(Predicate, Evaluable):
    """
    Represents a division operation in an expression tree.

    Attributes:
        left_side_types (Union[int, float]): Allowed types for the left operand.
        right_side_types (Union[PositiveFloat, PositiveInt]): Allowed types for the right operand.

    """

    left_side_types = int | float
    right_side_types = PositiveFloat | PositiveInt | NegativeFloat | NegativeInt

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        left_value = self.left_side._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        type_check_result = self.type_check(left_value, right_value)
        if isinstance(type_check_result, NullResult):
            return NullResult(self)
        return Literal(left_value.value / right_value.value)


class ObjectAttributeLookup(TreeMixin, Evaluable):
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
        if self.object:
            t.add(self.object)
        if self.attribute:
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

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        """TODO: Need to handle the case where the attribute is `None`"""
        one_query = QueryValueOfNodeAttribute(
            node_id=projection[self.object], attribute=self.attribute
        )
        value = fact_collection.query(one_query)
        return value


class Alias(TreeMixin, Evaluable):
    """
    Represents an alias for a reference in a tree structure.

    Attributes:
        reference (str): The original reference.
        alias (str): The alias for the reference.

    """

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

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        """Get the value of the reference and assign it to the alias."""
        result = self.reference._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )
        return result

    @property
    def children(self):
        yield self.reference
        yield self.alias


class ObjectAsSeries(TreeMixin, Evaluable):
    """
    A class that represents an object as a series of attributes.

    Attributes:
        lookups (List[Alias]): A list of Alias objects representing the attributes of the object.
    """

    def __init__(self, lookups: List[Alias]):
        self.lookups = lookups

    def __repr__(self):
        return f"ObjectAsSeries({self.lookups})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        for object_attribute_lookup in self.lookups:
            t.add(object_attribute_lookup.tree())
        return t

    @property
    def children(self) -> Generator[Projection | Alias]:
        yield self.lookups

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        result = {
            obj.alias: obj._evaluate(fact_collection, projection=projection)  # pylint: disable=protected-access
            for obj in self.lookups
        }
        return result


class WithClause(TreeMixin, Evaluable):
    """
    Represents a WITH clause in a query, which is used to manage projections and aggregations.

    Attributes:
        object_as_series (ObjectAsSeries): The object representing the series of projections.

    """

    def __init__(self, lookups: ObjectAsSeries):
        self.lookups = lookups

    def __repr__(self):
        return f"WithClause({self.lookups})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.lookups.tree())
        return t

    @property
    def children(self) -> Generator[Projection]:
        """
        Generator that yields the children of the current node.

        Yields:
        Projection: The object as a series.
        """

        yield self.lookups

    @property
    def aggregated_variables(self):
        """
        Collects and returns a list of objects that are attributes of
        Aggregation instances found during a walk through the current object.
        The method performs a depth-first traversal of the current object
        and collects objects that are attributes of Aggregation instances.

        Returns:
            list: A list of objects that are attributes of Aggregation instances.
        """
        out = []
        for obj in self.walk():
            if isinstance(obj, Aggregation):
                for sub_obj in obj.walk():
                    if isinstance(sub_obj, ObjectAttributeLookup):
                        out.append(sub_obj.object)
        return out

    @property
    def all_variables(self):
        """All the variables in the WITH clause."""
        out = []
        for obj in self.walk():
            if isinstance(obj, ObjectAttributeLookup):
                out.append(obj.object)
        return out

    @property
    def non_aggregated_variables(self):
        """Variables not in a COLLECT, SUM, or other Aggregation."""
        return list(set(self.all_variables) - set(self.aggregated_variables))

    @staticmethod
    def _unique_non_aggregated_variable_solutions(
        solutions, non_aggregated_variables
    ):
        non_aggregated_variable_solutions = []
        for solution in solutions:
            non_aggregated_variable_solution = {}
            # Below raises error in test_trigger_decorator_function_relationship_function unit test
            for variable in non_aggregated_variables:
                non_aggregated_variable_solution[variable] = solution[variable]
            if (
                non_aggregated_variable_solution
                not in non_aggregated_variable_solutions
            ):
                non_aggregated_variable_solutions.append(
                    non_aggregated_variable_solution
                )
        return non_aggregated_variable_solutions

    @staticmethod
    def _transform_solutions_by_aggregations(
        solutions, aggregated_variables, non_aggregated_variables
    ):
        transformed_solutions = []
        non_aggregated_combinations = (
            WithClause._unique_non_aggregated_variable_solutions(
                solutions, non_aggregated_variables
            )
        )
        for non_aggregated_combination in non_aggregated_combinations:
            transformed_solution = non_aggregated_combination
            for solution in solutions:
                if all(
                    solution[variable] == non_aggregated_combination[variable]
                    for variable in non_aggregated_variables
                ):
                    for variable in aggregated_variables:
                        if variable not in transformed_solution:
                            transformed_solution[variable] = []
                        transformed_solution[variable].append(
                            solution[variable]
                        )
            transformed_solutions.append(transformed_solution)
        return transformed_solutions

    def transform_solutions_by_aggregations(
        self, fact_collection: FactCollection
    ):
        """
        Transform the solutions by aggregations.
        """
        return self._transform_solutions_by_aggregations(
            self.parent.solutions(fact_collection),
            self.aggregated_variables,
            self.non_aggregated_variables,
        )

    def _evaluate_one_projection(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        return self.lookups._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Any] = None,
    ):
        solutions = projection or self.transform_solutions_by_aggregations(
            fact_collection
        )
        results = [
            self._evaluate_one_projection(
                fact_collection, projection=one_projection
            )
            for one_projection in solutions
        ]
        for one_result, one_solution in zip(results, solutions):
            one_result["__match_solution__"] = one_solution

        return results


class Match(TreeMixin):
    """
    Represents a MATCH clause in a Cypher query.

    Attributes:
    pattern (TreeMixin): The pattern to match in the query.
    where (Optional[TreeMixin]): An optional WHERE clause to filter the results.
    with_clause (Optional[TreeMixin]): An optional WITH clause to chain queries.
    constraints (Optional[List[Constraint]]): A list of constraints to apply to the match.
    """

    def __init__(
        self,
        pattern: TreeMixin,
        where_clause: Optional[TreeMixin] = None,
        with_clause: Optional[TreeMixin] = None,
        constraints: Optional[List[Constraint]] = None,
    ):
        self.pattern = pattern
        self.where_clause = where_clause
        self.with_clause = with_clause
        self.constraints = constraints or []

    def __repr__(self) -> str:
        return f"Match({self.pattern}, {self.where_clause}, {self.with_clause})"

    def gather_constraints(self) -> None:
        """Gather all the ``Constraint`` objects from inside the ``Match`` clause."""
        for node in self.walk():
            self.constraints += getattr(node, "constraints", [])

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.pattern.tree())
        if self.with_clause:
            t.add(self.with_clause.tree())
        if self.where_clause:
            t.add(self.where_clause.tree())
        return t

    @property
    def children(self):
        yield self.pattern
        if self.where_clause:
            yield self.where_clause
        if self.with_clause:
            yield self.with_clause

    def solutions(
        self, fact_collection: FactCollection | Shim
    ) -> List[Dict[str, Any]]:
        """
        Generate solutions based on the given fact collection and constraints.

        Args:
            fact_collection (FactCollection | Shim):
                A collection of facts or a shim that can be converted to a fact collection.

        Returns:
            List[Dict[str, Any]]:
                A list of dictionaries representing the solutions that satisfy the constraints.
        """

        def _set_up_problem(match_clause) -> Problem:
            constraints = match_clause.constraints
            problem = Problem()
            node_domain = Domain(set())
            relationship_domain = Domain(set())
            # Get domains for nodes and relationships
            LOGGER.debug("Constraints for problem are: %s", constraints)
            for fact in fact_collection.relevant_facts(constraints):
                LOGGER.debug("fact: %s", fact)
                if isinstance(fact, FactNodeHasLabel):
                    if fact.node_id not in node_domain:
                        LOGGER.debug("fact.node_id: %s", fact.node_id)
                        node_domain.append(fact.node_id)
                elif isinstance(fact, FactRelationshipHasLabel):
                    if fact.relationship_id not in relationship_domain:
                        relationship_domain.append(fact.relationship_id)
                elif isinstance(fact, FactNodeHasAttributeWithValue):
                    if fact.node_id not in node_domain:
                        node_domain.append(fact.node_id)
                elif isinstance(
                    fact,
                    (
                        FactRelationshipHasSourceNode,
                        FactRelationshipHasTargetNode,
                    ),
                ):
                    if fact.relationship_id not in relationship_domain:
                        relationship_domain.append(fact.relationship_id)
                else:
                    pass

            LOGGER.debug("node_domain: %s", node_domain)
            LOGGER.debug("relationship_domain: %s", relationship_domain)

            # Assign variables to domains -- TODO: Ensure no duplicates!
            for constraint in constraints:
                if isinstance(constraint, ConstraintNodeHasLabel):
                    if constraint.variable not in problem._variables:  # pylint: disable=protected-access
                        problem.addVariable(constraint.variable, node_domain)
                elif (  # pylint: disable=protected-access
                    isinstance(constraint, ConstraintRelationshipHasSourceNode)
                    and constraint.relationship_name not in problem._variables
                ):
                    if constraint.variable not in problem._variables:
                        problem.addVariable(
                            constraint.relationship_name, relationship_domain
                        )
                elif (  # pylint: disable=protected-access
                    isinstance(constraint, ConstraintRelationshipHasTargetNode)
                    and constraint.relationship_name not in problem._variables
                ):
                    if constraint.variable not in problem._variables:
                        problem.addVariable(
                            constraint.relationship_name, relationship_domain
                        )
                elif (  # pylint: disable=protected-access
                    isinstance(constraint, ConstraintRelationshipHasLabel)
                    and constraint.relationship_name not in problem._variables
                ):
                    if constraint.variable not in problem._variables:
                        problem.addVariable(
                            constraint.relationship_name, relationship_domain
                        )
                elif (  # pylint: disable=protected-access
                    isinstance(constraint, ConstraintNodeHasAttributeWithValue)
                    and constraint.variable not in problem._variables
                ):
                    if constraint.variable not in problem._variables:
                        problem.addVariable(constraint.variable, node_domain)
                elif (
                    isinstance(
                        constraint, ConstraintVariableRefersToSpecificObject
                    )
                    and constraint.variable not in problem._variables  # pylint: disable=protected-access
                ):
                    if constraint.variable not in problem._variables:  # pylint: disable=protected-access
                        problem.addVariable(constraint.variable, node_domain)
                else:
                    pass

            # Add constraints to problem definition
            def _f(x, y):
                answer = (
                    FactRelationshipHasSourceNode(
                        relationship_id=x, source_node_id=y
                    )
                    in fact_collection
                )
                LOGGER.debug(
                    "answer _f: FactRelationshipHasSourceNode: %s for x: %s, y: %s",
                    answer,
                    x,
                    y,
                )
                return answer

            def _g(node_id, label=None):
                answer = (
                    FactNodeHasLabel(node_id=node_id, label=label)
                    in fact_collection
                )
                LOGGER.debug(
                    "answer _g: FactNodeHasLabel: %s for node_id: %s, label: %s",
                    answer,
                    node_id,
                    label,
                )
                return answer

            def _h(relationship_id, relationship_label=None):
                answer = (
                    FactRelationshipHasLabel(
                        relationship_id=relationship_id,
                        relationship_label=relationship_label,
                    )
                    in fact_collection
                )
                LOGGER.debug(
                    "answer _h: FactRelationshipHasLabel %s for relationship_id: %s, "
                    "relationship_label: %s",
                    answer,
                    relationship_id,
                    relationship_label,
                )
                return answer

            def _i(node_id, attribute=None, value=None):
                answer = (
                    FactNodeHasAttributeWithValue(
                        node_id=node_id, attribute=attribute, value=value
                    )
                    in fact_collection
                )
                LOGGER.debug(
                    "answer _i: FactNodeHasAttributeWithValue %s for node_id: %s, "
                    "attribute: %s, value: %s",
                    answer,
                    node_id,
                    attribute,
                    value,
                )
                return answer

            # Experimental...
            def _j(node_id, other_node_id=None):
                answer = node_id == other_node_id
                LOGGER.debug(
                    "answer _j: %s for node_id: %s",
                    answer,
                    node_id,
                )
                return answer

            for constraint in constraints:
                if isinstance(constraint, ConstraintNodeHasLabel):
                    LOGGER.debug("Adding constraint: %s", constraint)
                    problem.addConstraint(
                        partial(_g, label=constraint.label),
                        [
                            constraint.variable,
                        ],
                    )
                elif isinstance(
                    constraint, ConstraintNodeHasAttributeWithValue
                ):
                    LOGGER.debug("Adding constraint: %s", constraint)
                    problem.addConstraint(
                        partial(
                            _i,
                            attribute=constraint.attribute,
                            value=constraint.value,
                        ),
                        [
                            constraint.variable,
                        ],
                    )
                # Experimental...
                elif isinstance(
                    constraint, ConstraintVariableRefersToSpecificObject
                ):
                    LOGGER.debug("Adding constraint: %s", constraint)
                    problem.addConstraint(
                        partial(_j, other_node_id=constraint.node_id),
                        [
                            constraint.variable,
                        ],
                    )
                elif isinstance(constraint, ConstraintRelationshipHasLabel):
                    LOGGER.debug("Adding constraint: %s", constraint)
                    problem.addConstraint(
                        partial(_h, relationship_label=constraint.label),
                        [
                            constraint.relationship_name,
                        ],
                    )
                elif isinstance(
                    constraint, ConstraintRelationshipHasSourceNode
                ):
                    LOGGER.debug("Adding constraint: %s", constraint)
                    problem.addConstraint(
                        _f,
                        [
                            constraint.relationship_name,
                            constraint.variable,
                        ],
                    )
                elif isinstance(
                    constraint, ConstraintRelationshipHasTargetNode
                ):
                    LOGGER.debug("Adding constraint: %s", constraint)
                    problem.addConstraint(
                        lambda x, y: FactRelationshipHasTargetNode(
                            relationship_id=x, target_node_id=y
                        )
                        in fact_collection,
                        [
                            constraint.relationship_name,
                            constraint.variable,
                        ],
                    )
                else:
                    pass  # pragma: exclude Add more constraints if necessary
            return problem

        fact_collection = (
            fact_collection
            if isinstance(fact_collection, FactCollection)
            else fact_collection.make_fact_collection()
        )
        try:
            problem = _set_up_problem(self)
        except ValueError as e:  # pylint: disable=broad-exception-caught
            LOGGER.error("Domain not ready yet? %s", e)
            return []
        try:
            solutions = problem.getSolutions()
        except Exception as e:  # pylint: disable=broad-exception-caught
            LOGGER.error("Error getting solutions: %s", e)
            return []
        return solutions


class Return(TreeMixin):
    """
    The Return class represents a RETURN clause in a Cypher query. It is used to specify
    which projections (columns) should be returned from the query.

    Attributes:
        projection (Projection): The projection node that specifies the columns to be returned.

    """

    def __init__(self, node: Projection):
        self.projection = node
        self.variables = self.gather_variables()

    def __repr__(self):
        return f"Return({self.projection})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.projection.tree())
        return t

    @property
    def children(self):
        yield self.projection

    def gather_variables(self):
        """Get the variables from the projection"""
        _ = [_ for _ in self.walk()]  # Inefficient.
        variables = []
        for child in self.walk():
            if isinstance(child, ObjectAttributeLookup) and not isinstance(
                child.parent, Alias
            ):
                variable = child.object
                if variable not in variables:
                    variables.append(variable)
            elif isinstance(child, Alias):
                variable = child.alias
                if variable not in variables:
                    variables.append(variable)
            elif isinstance(child, AliasedName):
                variable = child.name
                if variable not in variables:
                    variables.append(variable)
        return variables

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ):
        """For now, just return a subset of the projection from the WITH clause."""
        with_clause_projections = (
            projection
            or self.parent.match_clause.with_clause._evaluate(  # pylint: disable=protected-access
                fact_collection, projection=None
            )
        )
        result = []
        for with_clause_projection in with_clause_projections:
            one_return_output = {}
            for return_lookup in (
                self.projection.lookups
            ):  # Each is a single object/attribute lookup
                if isinstance(return_lookup, ObjectAttributeLookup):
                    one_return_output[return_lookup.object] = (
                        with_clause_projection[return_lookup.object]
                    )
                elif isinstance(return_lookup, AliasedName):
                    one_return_output[return_lookup.name] = (
                        with_clause_projection[return_lookup.name]
                    )
            one_return_output["__with_clause_projection__"] = (
                with_clause_projection
            )
            result.append(one_return_output)
        return result


class Projection(TreeMixin):
    """
    A class representing a projection in a tree structure.

    Attributes:
        lookups (List[ObjectAttributeLookup]): A list of ObjectAttributeLookup instances.

    """

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

    def __init__(self, name: Optional[str] = None, label: Optional[str] = None):
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
        self.name_label = name_label  # This should be ``label`` for consistency
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
    """The all-important WHERE clause."""

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
        """Pointless?"""
        return [IsTrue(self.predicate)]

    @property
    def children(self):
        yield self.predicate


class And(BinaryBoolean, Evaluable):
    """
    Represents a logical AND operation between two boolean expressions.
    """

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        left_value = self.left_side._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        self.type_check(left_value, right_value)
        return Literal(left_value.value and right_value.value)


class Or(BinaryBoolean, Evaluable):
    """
    Represents a logical OR operation between two boolean expressions.

    """

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        left_value = self.left_side._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        self.type_check(left_value, right_value)
        return Literal(left_value.value or right_value.value)


class Not(Evaluable, Predicate):
    """
    Represents a logical NOT operation in a predicate expression.

    Inherits from:
        Evaluable: Base class for evaluable expressions.
        Predicate: Base class for predicate expressions.

    Attributes:
        argument_types (type): The expected type of the argument, which is a boolean.

    Args:
        argument (Predicate | Literal): The predicate or literal to be negated.

    """

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

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        value = self.argument._evaluate(fact_collection, projection=projection)  # pylint: disable=protected-access
        type_check_result = self.type_check(value)
        if isinstance(type_check_result, NullResult):
            return NullResult(self)
        return Literal(not value.value)


class RelationshipChainList(TreeMixin):
    """
    A class to represent a list of relationship chains.

    Attributes:
    -----------
    relationships : List[RelationshipChain]
        A list of RelationshipChain objects.

    """

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


class Addition(Evaluable, Predicate):
    """
    Represents an addition operation between two evaluable tree nodes.

    Attributes:
        left_side_types (type): The allowed types for the left operand (int or float).
        right_side_types (type): The allowed types for the right operand (int or float).
        left_side (TreeMixin): The left operand of the addition.
        right_side (TreeMixin): The right operand of the addition.

    """

    left_side_types = int | float
    right_side_types = int | float

    def __init__(self, left: TreeMixin, right: TreeMixin):  # pylint: disable=super-init-not-called
        """
        Initialize an Addition instance.

        Args:
            left (TreeMixin): The left operand of the addition.
            right (TreeMixin): The right operand of the addition.
        """
        self.left_side = left
        self.right_side = right

    def __repr__(self):
        """
        Return a string representation of the Addition instance.

        Returns:
            str: A string representation in the format "Addition(left_side, right_side)".
        """
        return f"Addition({self.left_side}, {self.right_side})"

    def tree(self):
        """
        Create a tree representation of this Addition node.

        Returns:
            Tree: A rich.tree.Tree object representing this node and its children.
        """
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    @property
    def children(self):
        """
        Get the children of this Addition node.

        Yields:
            TreeMixin: The left and right operands of the addition.
        """
        yield self.left_side
        yield self.right_side

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        """
        Evaluate this Addition node.

        Computes the sum of the left and right operands.

        Args:
            fact_collection (FactCollection): The collection of facts to use for evaluation.
            projection (Optional[Dict[str, str | List[str]]]): A mapping of variable names to values.
                Defaults to None.

        Returns:
            Any: The result of adding the left and right operands.

        Raises:
            TypeError: If the operands are not of compatible types for addition.
        """
        left_value = self.left_side._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(  # pylint: disable=protected-access
            fact_collection, projection=projection
        )  # pylint: disable=protected-access
        type_check_result = self.type_check(left_value, right_value)
        if isinstance(type_check_result, NullResult):
            return NullResult(self)
        return Literal(left_value.value + right_value.value)


class Literal(TreeMixin, Evaluable):
    """
    A class representing a literal value in a tree structure.

    Attributes:
        value (Any): The literal value.

    """

    def __init__(self, value: Any):
        self.value = value

    def __repr__(self):
        return f"Literal({self.value})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(str(self.value))
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Optional[Dict[str, str | List[str]]] = None,
    ) -> Any:
        return Literal(self.value)

    def __eq__(self, other):
        return isinstance(other, Literal) and self.value == other.value

    def __hash__(self):
        return hash(self.value)
