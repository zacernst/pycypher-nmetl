"""
This module defines various classes representing nodes and expressions in an
Abstract Syntax Tree (AST) for Cypher queries.
"""  # pylint: disable=too-many-lines

from __future__ import annotations

import abc
import collections
import copy
import itertools
import uuid
from typing import Any, Dict, Generator, List, Optional, Set

from pycypher.fact_collection import FactCollection
from pycypher.query import QueryNodeLabel, QueryValueOfNodeAttribute
from pycypher.tree_mixin import TreeMixin
from rich.tree import Tree
from shared.logger import LOGGER

LOGGER.setLevel("WARNING")

MATCH_SOLUTION_KEY: str = '__MATCH_SOLUTION_KEY__'
WITH_SOLUTION_KEY: str = '__WITH_SOLUTION_KEY__'
WHERE_SOLUTION_KEY: str = '__WHERE_SOLUTION_KEY__'


class EvaluationError(Exception):
    """
    Custom exception class for evaluation errors.
    """


class Evaluable(TreeMixin, abc.ABC):  # pylint: disable=too-few-public-methods
    """
    Abstract base class representing an evaluable entity.
    """

    @abc.abstractmethod
    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = {},
    ) -> Any:
        pass


class Cypher(TreeMixin):
    """
    The root node of the Abstract Syntax Tree (AST) for Cypher queries.

    Attributes:
        cypher (TreeMixin): The root node of the AST.

    """

    def __init__(self, cypher: Query):
        self.cypher: Query = cypher
        self._attribute_names: List[str] = []

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
        if not self._attribute_names:
            LOGGER.debug('Walking for attribute names...')
            self._attribute_names: List[str] = [
                obj.attribute
                for obj in self.walk()
                if isinstance(obj, ObjectAttributeLookup)
            ]
        else:
            LOGGER.debug('Cached attribute names...')
        LOGGER.debug('Attribute names: %s', self._attribute_names)
        return self._attribute_names

    def tree(self) -> Tree:
        """
        Generates a tree representation of the current node.

        Returns:
            Tree: A tree object representing the current node and its Cypher tree.
        """
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.cypher.tree())
        return t


class Aggregation(Evaluable, TreeMixin):
    """
    Represents an aggregation operation that transforms a list into a singleton value.

    Attributes:
        aggregation: The aggregation operation to be performed.

    """

    def __init__(self, aggregation):
        self.aggregation = aggregation

    @property
    def children(self) -> Generator[Evaluable]:
        """
        Generator function that yields the aggregation attribute.

        Yields:
            The aggregation attribute of the instance.
        """
        yield self.aggregation

    def __repr__(self) -> str:
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
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.aggregation.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = [],
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
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )


class Collection(Evaluable, TreeMixin):  # i.e. a list
    """
    A class representing a collection of evaluable items.

    Attributes:
        values (List[Evaluable]): A list of evaluable items.

    """

    def __init__(self, values: List[Evaluable]):
        self.values = values
        self.value = values # Make this consistent later...

    @property
    def children(self) -> Generator[Evaluable, Any, Any]:
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

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        for value in self.values:
            t.add(value.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = {},
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
                value._evaluate(
                    fact_collection,
                    start_entity_var_id_mapping=start_entity_var_id_mapping,
                )
                for value in self.values
            ]
        )

    def __eq__(self, other):
        return isinstance(other, Collection) and self.values == other.values


class Distinct(Evaluable, TreeMixin):
    """
    Represents a distinct operation on a collection.

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

    def __repr__(self) -> str:
        """
        Return a string representation of the Distinct object.
        """
        return f"Distinct({self.collection})"

    def tree(self):
        """
        Generates a tree representation of the current node class.

        Returns:
            Tree: A tree object representing the current node class and its collection.
        """
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.collection.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = [],
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
        collection: List[Evaluable] = self.collection._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )
        new_values: list[Evaluable] = []
        for value in collection:
            if value not in new_values:
                new_values.append(value)
        return Collection(new_values)


class Size(Evaluable, TreeMixin):
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

    def __repr__(self) -> str:
        return f"Size({self.collection})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.collection.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = {},
    ) -> Any:
        return Literal(
            len(
                self.collection._evaluate(  # pylint: disable=protected-access
                    fact_collection,
                    start_entity_var_id_mapping=start_entity_var_id_mapping,
                ).values
            )
        )


class Collect(Evaluable, TreeMixin):
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

        t: Tree = Tree(self.__class__.__name__)
        t.add(self.object_attribute_lookup.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = [],
    ) -> Collection:
        """ """
        if not isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(f"Error while evaluating Collect: {self}")
        result: Collection = Collection(
            values=[
                self.object_attribute_lookup._evaluate(
                    fact_collection, start_entity_var_id_mapping=mapping
                )
                for mapping in start_entity_var_id_mapping
            ]
        )

        return result


class Query(Evaluable, TreeMixin):
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
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.match_clause.tree())
        t.add(self.return_clause.tree())
        return t

    def _evaluate(
        self,
        fact_collection,
        start_entity_var_id_mapping: Dict[str, Any] | List[dict[str, Any]] = {},
    ) -> List[Dict[str, Any]]:
        match_clause_results: list[dict[str, Any]] = (
            self.match_clause._evaluate(
                fact_collection,
                start_entity_var_id_mapping=start_entity_var_id_mapping,
            )
        )
        return_clause_results: list[dict[str, Any]] = [
            self.return_clause._evaluate(
                fact_collection,
                start_entity_var_id_mapping=start_entity_var_id_mapping,
            )
            for start_entity_var_id_mapping in match_clause_results
        ]
        return return_clause_results


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

    def __init__(self, left_side: Evaluable, right_side: Evaluable):
        self.left_side: Evaluable = left_side
        self.right_side: Evaluable = right_side

    @property
    def children(self) -> Generator[TreeMixin]:
        if hasattr(self, "left_side"):
            yield self.left_side
            yield self.right_side

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.left_side}, {self.right_side})"


class BinaryBoolean(Predicate, TreeMixin):
    """
    A class representing a binary boolean operation in a predicate tree.

    Attributes:
        left_side (Predicate | Literal): The left side of the binary boolean operation.
        right_side (Predicate | Literal): The right side of the binary boolean operation.

    """

    def __init__(  # pylint: disable=super-init-not-called
        self,
        left_side: Evaluable,
        right_side: Evaluable,
    ) -> None:
        self.left_side = left_side
        self.right_side = right_side

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.left_side}, {self.right_side})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class AliasedName(Evaluable, TreeMixin):
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
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.name)
        return t

    @property
    def children(self) -> Generator[str]:
        yield self.name

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = {},
    ) -> Literal:
        if isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(f"Error while evaluating AliasedName: {self}")
        return Literal(start_entity_var_id_mapping[self.name])


class Equals(BinaryBoolean, Evaluable):
    """Binary infix operator for equality."""

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = {},
    ) -> Literal:
        if isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(f"Error while evaluating Equals: {self}")
        left_value = self.left_side._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        return Literal(left_value == right_value)


class LessThan(Predicate, Evaluable):
    """Binary infix operator for less than."""

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, str] | List[Dict[str, Any]] = {},
    ) -> Any:
        if isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(f"Error while evaluating LessThan: {self}")
        left_value = self.left_side._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        return Literal(left_value < right_value)


class GreaterThan(Predicate, Evaluable):
    """Binary infix operator for greater than."""

    left_side_types = int | float
    right_side_types = int | float

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, str] | List[Dict[str, Any]] = {},
    ) -> Any:
        if isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(f"Error while evaluating GreaterThan: {self}")
        left_value = self.left_side._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
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
        start_entity_var_id_mapping: Dict[str, str] | List[Dict[str, Any]] = {},
    ) -> Any:
        if isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(f"Error while evaluating Subtraction: {self}")
        left_value = self.left_side._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        return Literal(left_value - right_value)


class Multiplication(Predicate, Evaluable):
    """
    A class representing a multiplication operation in a predicate logic expression.

    Attributes:
        left_side_types (type): The allowed types for the left operand (float or int).
        right_side_types (type): The allowed types for the right operand (float or int).

    """

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, str] | List[Dict[str, Any]] = {},
    ) -> Any:
        if isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(
                f"Error while evaluating Multiplication: {self}"
            )
        left_value = self.left_side._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        return Literal(left_value * right_value)


class Division(Predicate, Evaluable):
    """
    Represents a division operation in an expression tree.

    Attributes:
        left_side_types (Union[int, float]): Allowed types for the left operand.
        right_side_types (Union[PositiveFloat, PositiveInt]): Allowed types for the right operand.

    """

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, str] | List[Dict[str, Any]] = {},
    ) -> Any:
        if isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(f"Error while evaluating Division: {self}")
        left_value = self.left_side._evaluate(  # pylint: disable=protected-access
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(  # pylint: disable=protected-access
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        return Literal(left_value.value / right_value.value)


class ObjectAttributeLookup(Evaluable, TreeMixin):
    """A node that represents the value of an attribute of a node or relationship
    of the form ``node.attribute``.
    """

    def __init__(self, object: str, attribute: str):
        self.object = object
        self.attribute = attribute

    def __repr__(self) -> str:
        return f"ObjectAttributeLookup({self.object}, {self.attribute})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        if self.object:
            t.add(self.object)
        if self.attribute:
            t.add(self.attribute)
        return t

    @property
    def children(self) -> Generator[str]:
        yield self.object
        yield self.attribute

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, str] | List[Dict[str, Any]] = {},
    ) -> Literal:
        """TODO: Need to handle the case where the attribute is `None`"""
        if isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(
                f"Error while evaluating ObjectAttributeLookup: {self}"
            )
        one_query: QueryValueOfNodeAttribute = QueryValueOfNodeAttribute(
            node_id=start_entity_var_id_mapping[self.object],
            attribute=self.attribute,
        )
        value: Any = fact_collection.query(one_query)
        return Literal(value)


class Alias(Evaluable, TreeMixin):
    """
    Represents an alias for a reference in a tree structure.

    Attributes:
        reference (str): The original reference.
        alias (str): The alias for the reference.

    """

    def __init__(self, reference: Evaluable, alias: str):
        self.reference: Evaluable = reference
        self.alias = alias

    def __repr__(self):
        return f"Alias({self.reference}, {self.alias})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
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
        start_entity_var_id_mapping: Dict[str, str] | List[Dict[str, Any]] = {},
    ) -> Dict[str, Any]:
        """Get the value of the reference and assign it to the alias."""
        result = self.reference._evaluate(  # pylint: disable=protected-access
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )
        mapping: Dict[str, Any] = {self.alias: result}
        return mapping

    @property
    def children(self):
        yield self.reference
        yield self.alias


class ObjectAsSeries(Evaluable, TreeMixin):
    """ """

    def __init__(self, lookups: List[Alias]):
        self.lookups = lookups

    def __repr__(self):
        return f"ObjectAsSeries({self.lookups})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        for object_attribute_lookup in self.lookups:
            t.add(object_attribute_lookup.tree())
        return t

    @property
    def children(self) -> Generator[Projection | Alias]:
        yield from self.lookups

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, str] | List[Dict[str, Any]] = {},
    ) -> Dict[str, Any]:
        if isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(
                f"Error while evaluating ObjectAsSeries: {self}"
            )
        result_list: List[Dict[str, Any]] = [
            lookup._evaluate(
                fact_collection,
                start_entity_var_id_mapping=start_entity_var_id_mapping,
            )
            for lookup in self.lookups
        ]
        out: dict[str, Any] = {}
        for result in result_list:
            out.update(result)
        return out


class WithClause(Evaluable, TreeMixin):
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
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.lookups.tree())
        return t

    @property
    def children(self) -> Generator[ObjectAsSeries]:
        """
        Generator that yields the children of the current node.

        Yields:
        Projection: The object as a series.
        """

        yield self.lookups

    @property
    def is_aggregation(self) -> bool:
        """
        Returns whether the WithClause contains aggregation functions.
        """
        for obj in self.walk():
            if isinstance(obj, Aggregation):
                return True
        return False

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = [],
    ) -> List[Dict[str, Any]]:
        if isinstance(start_entity_var_id_mapping, dict):
            raise EvaluationError(f"Error while evaluating WithClause: {self}")

        if self.is_aggregation:  # Skipping for now
            result: List[Dict[str, Any]] = [self._evaluate_aggregation(
                fact_collection,
                start_entity_var_id_mapping=start_entity_var_id_mapping,
            )]
            result = [one_result for one_result in result if one_result]
            if not result:
                return result
            for index, one_result in enumerate(result):
                if len(one_result) == 0 or not start_entity_var_id_mapping:
                    continue
                LOGGER.warning("result in is_aggregation block: %s ::::: %s ::::: %s", one_result, index, start_entity_var_id_mapping)
                LOGGER.warning("result: %s", result)
                try:
                    one_result[MATCH_SOLUTION_KEY] = start_entity_var_id_mapping[index]
                except Exception as e:
                    LOGGER.warning("Error in is_aggregation block: %s ::::: %s ::::: %s", one_result, index, start_entity_var_id_mapping)
                    raise e
            # result[MATCH_SOLUTION_KEY] = start_entity_var_id_mapping
            # LOGGER.debug("result in is_aggregation block: %s", result)
        elif not self.is_aggregation:
            result: List[Dict[str, Any]] = []
            for item in start_entity_var_id_mapping:
                evaluation = self._evaluate_non_aggregation(fact_collection, item)
                evaluation[MATCH_SOLUTION_KEY] = item  # Changed to ``item``
                result.append(evaluation)
        else:
            raise Exception('This should never happen')
        return result

    def _evaluate_aggregation(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = [],
    ) -> Dict[str, Any]:
        """
        If it's an aggregation, we need to feed the subclauses *all* of the solutions at once.
        """
        if not isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(f"Error while evaluating WithClause: {self}")
        projection: Dict[str, Any] = {}
        LOGGER.warning("_evaluate_aggregation: %s", start_entity_var_id_mapping)
        for lookup in self.lookups.lookups:
            value: Any = lookup._evaluate(
                fact_collection=fact_collection,
                start_entity_var_id_mapping=start_entity_var_id_mapping,
            )
            projection.update(value)
        return projection

    def _evaluate_non_aggregation(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, str] = {},
    ) -> Dict[str, Any]:
        projection: Dict[str, Any] = {}
        for lookup in self.lookups.lookups:
            value: Any = lookup._evaluate(
                fact_collection=fact_collection,
                start_entity_var_id_mapping=start_entity_var_id_mapping,
            )
            projection.update(value)
        return projection


class Match(Evaluable, TreeMixin):
    """
    Represents a MATCH clause in a Cypher query.

    Attributes:
    pattern (TreeMixin): The pattern to match in the query.
    where (Optional[TreeMixin]): An optional WHERE clause to filter the results.
    with_clause (Optional[TreeMixin]): An optional WITH clause to chain queries.
    """

    def __init__(
        self,
        pattern: RelationshipChainList,  # This part of the query is mandatory
        with_clause: Optional[WithClause] = None,
        where_clause: Optional[Where] = None,
    ) -> None:
        self.pattern = pattern
        self.with_clause = with_clause
        self.where_clause = where_clause

    def __repr__(self) -> str:
        return f"Match({self.pattern}, {self.where_clause}, {self.with_clause})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
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

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = [],
    ) -> List[Dict[str, Any]]:
        """First, evaluate the pattern;
        Then evaluate the WithClause;
        Then the WhereClause;
        Send to ReturnClause
        """
        if isinstance(start_entity_var_id_mapping, list):
            raise ValueError()
        start_entity_var_id_mapping_post_pattern: List[Dict[str, str]] = (
            self.pattern._evaluate(
                fact_collection,
                start_entity_var_id_mapping=start_entity_var_id_mapping,
            )
        )
        start_entity_var_id_mapping_post_with: List[Dict[str, Any]] = (
            self.with_clause._evaluate(
                fact_collection,
                start_entity_var_id_mapping=start_entity_var_id_mapping_post_pattern,
            )
            if self.with_clause
            else start_entity_var_id_mapping_post_pattern
        )
        out: list = (
            [
                start_entity_var_id_mapping
                for start_entity_var_id_mapping in start_entity_var_id_mapping_post_with
                if self.where_clause._evaluate(
                    fact_collection,
                    start_entity_var_id_mapping=start_entity_var_id_mapping,
                )
            ]
            if self.where_clause
            else start_entity_var_id_mapping_post_with
        )
        LOGGER.info(f'start_entity_var_id_mapping_post_pattern: {start_entity_var_id_mapping_post_pattern}')
        LOGGER.info(f'start_entity_var_id_mapping_post_with: {start_entity_var_id_mapping_post_with}')
        LOGGER.info(f'start_entity_var_id_mapping_out: {out}')
        for post_pattern, post_with, post_where in zip(start_entity_var_id_mapping_post_pattern, start_entity_var_id_mapping_post_with, out):
            LOGGER.info(f'Post pattern: {post_pattern}')
            LOGGER.info(f'Post with: {post_with}')
            LOGGER.info(f'Post out: {post_where}')
        LOGGER.debug(f'Solution at end of MATCH WITH WHERE: {out}')
        return out


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
        _: list[TreeMixin] = [_ for _ in self.walk()]  # Inefficient.
        variables: List[str] = []
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
        start_entity_var_id_mapping: Dict[str, str] = {},
    ) -> Dict[str, Any]:
        LOGGER.debug('In _evaluate of Return: %s', start_entity_var_id_mapping)
        out = self.projection._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )
        out[MATCH_SOLUTION_KEY] = start_entity_var_id_mapping.get(MATCH_SOLUTION_KEY, {})
        return out


class Projection(TreeMixin):
    """ """

    def __init__(self, lookups: List[AliasedName | Alias] = []):
        self.lookups = lookups

    def __repr__(self) -> str:
        return f"Projection({self.lookups})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        for lookup in self.lookups:
            t.add(lookup.tree())
        return t

    @property
    def children(self) -> Generator[AliasedName | Alias, None, None]:
        yield from self.lookups

    def _evaluate(
        self, fact_collection, start_entity_var_id_mapping: Dict[str, Any] = {}
    ) -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        for lookup in self.lookups:
            if not isinstance(
                lookup,
                (
                    AliasedName,
                    Alias,
                ),
            ):
                raise EvaluationError(
                    f"Error while evaluating Projection: {self}"
                )

            d.update(
                {lookup.name: start_entity_var_id_mapping[lookup.name]}
                if isinstance(lookup, AliasedName)
                else {
                    lookup.alias: start_entity_var_id_mapping[lookup.reference]
                }
            )
        return d


class NodeNameLabel(TreeMixin):
    """A node name, optionally followed by a label, separated by a dot."""

    def __init__(self, name: Optional[str] = None, label: str = "_") -> None:
        self.name: str = name or uuid.uuid4().hex
        self.label = label

    def __repr__(self) -> str:
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


class Assignment:
    def __init__(self, variable: str, assignment: str) -> None:
        self.variable: str = variable
        self.assignment: str = assignment
        self.children: List[Assignment] = []
        self.parent: Optional[Assignment] = None

    def parents(self) -> List[Assignment]:
        out: List[Assignment] = []
        parent: Assignment | None = self.parent
        while parent:
            out.append(parent)
            parent = parent.parent
        return out

    def walk(self) -> Generator[Assignment]:
        yield self
        for child in self.children:
            yield from child.walk()

    def assignments(self) -> Dict[str, str]:
        out: Dict[str, str] = {self.variable: self.assignment}
        for parent in self.parents():
            out[parent.variable] = parent.assignment
        return out

    def leaves(self) -> Generator[Assignment]:
        for node in self.walk():
            if not node.children:
                yield node

    def _leaf_assignments(self) -> Generator[Dict[str, str]]:
        for leaf in self.leaves():
            yield leaf.assignments()

    def leaf_assignments(self) -> List[Dict[str, str]]:
        return list(self._leaf_assignments())

    def set_child(self, assignment: Assignment) -> None:
        self.children.append(assignment)
        assignment.parent = self


class Node(TreeMixin):
    """A node in the graph, which may contain a variable name, label, or mapping."""

    def __init__(
        self,
        node_name_label: NodeNameLabel | None = None,
        mapping_set: MappingSet | None = None,
    ) -> None:
        self.node_name_label: NodeNameLabel = node_name_label or NodeNameLabel()
        self.mapping_set: MappingSet = mapping_set or MappingSet([])

    # Ensure node_name_label always has a variable name and label
    # And that each Node has a NodeNameLabel
    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, str] | List[Dict[str, Any]] = {},
    ) -> List[Dict[str, str]]:
        if isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(f"Error while evaluating Node: {self}")
        if self.node_name_label.name in start_entity_var_id_mapping:
            entity_id: str = start_entity_var_id_mapping[
                self.node_name_label.name
            ]
            entity_label: str = fact_collection.query_node_label(
                QueryNodeLabel(entity_id)
            )
            out: List[Dict[str, str]] = (
                [{self.node_name_label.name: entity_id}]
                if entity_label == self.node_name_label.label
                else []
            )
        else:
            out: List[Dict[str, str]] = [
                {self.node_name_label.name: entity_id}
                for entity_id in fact_collection.nodes_with_label(
                    self.node_name_label.label
                )
            ]
        # Filter ``out`` to instances that match mapping
        # TODO: Move this to MappingSet class so that Relationships can use the same code
        mapping_filtered: List[Dict[str, str]] = []
        for solution in out:
            satisfies_mapping: bool = True
            for mapping in self.mapping_set.mappings:
                query: QueryValueOfNodeAttribute = QueryValueOfNodeAttribute(
                    node_id=solution[self.node_name_label.name],
                    attribute=mapping.key,
                )
                value: Any = fact_collection.query(query)
                if value != mapping.value.value:
                    satisfies_mapping = False
            if satisfies_mapping:
                mapping_filtered.append(solution)

        out = mapping_filtered

        # TODO: Fix the following block
        # if "_" in start_entity_var_id_mapping:
        #     mapping_clone: dict[str, str] = copy.copy(start_entity_var_id_mapping)
        #     mapping_clone[self.node_name_label.name] = mapping_clone["_"]
        #     del mapping_clone["_"]
        #     out.append(
        #         self._evaluate(
        #             fact_collection, start_entity_var_id_mapping=mapping_clone
        #         )
        #     )
        return out

    def __repr__(self):
        return f"Node({self.node_name_label}, {self.mapping_set})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        if self.node_name_label:
            t.add(self.node_name_label.tree())
        if self.mapping_set:
            t.add(self.mapping_set.tree())
        return t

    @property
    def children(self) -> Generator[NodeNameLabel | Mapping]:
        if self.node_name_label:
            yield self.node_name_label
        if self.mapping_set:
            for mapping in self.mapping_set.mappings:
                yield mapping


class Relationship(TreeMixin):
    """Relationships may contain a variable name, label, or mapping."""

    def __init__(self, name_label: NodeNameLabel):
        self.name_label = name_label  # This should be ``label`` for consistency
        self.name = None

    def __repr__(self):
        return f"Relationship({self.name_label})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.name_label.tree())
        return t

    @property
    def children(self):
        yield self.name_label


class Mapping(TreeMixin):  # This is not complete
    """Mappings are dictionaries of key-value pairs."""

    def __init__(self, key: str, value: Any):
        self.key: str = key
        self.value: Literal = value

    def __repr__(self):
        return f"Mapping({self.key}:{self.value})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.key)
        t.add(str(self.value))
        return t

    @property
    def children(self):
        yield self.key
        yield self.value


class List_(TreeMixin):
    """A list."""

    def __init__(self, items: List[Any]):
        self.items = items

    def __repr__(self):
        return f"List_({self.items})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        for item in self.items:
            t.add(item.tree())
        return t

    @property
    def children(self):
        yield from self.items


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
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.relationship.tree())
        return t

    @property
    def children(self) -> Generator[Relationship]:
        yield self.relationship


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


def conflicting_vars(*dictionaries) -> Set[str]:
    vars_values: collections.defaultdict[str, set[str]] = (
        collections.defaultdict(set)
    )
    for dictionary in dictionaries:
        for key, value in dictionary.items():
            vars_values[key].add(value)
    conflicts: set[str] = {
        key for key, value in vars_values.items() if len(value) > 1
    }
    return conflicts


class RelationshipChain(TreeMixin):
    """Several ``Relationship`` nodes chained together, sharing ``Node`` objects
    between them."""

    def __init__(
        self,
        source_node: Node,
        relationship: RelationshipLeftRight | RelationshipRightLeft,
        target_node: Node,
    ):
        self.source_node: Node = source_node
        self.relationship: RelationshipLeftRight | RelationshipRightLeft = (
            relationship
        )
        self.target_node: Node = target_node

    def __repr__(self):
        return f"RelationshipChain({self.source_node}, {self.relationship}, {self.target_node})"

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, str] = {},
    ) -> List[Dict[str, str]]:
        """Assume that self.steps is exactly (Node, Relationship, Node,)."""
        left_node_var_id_mapping_list: list[dict[str, str]] = (
            self.source_node._evaluate(
                fact_collection, start_entity_var_id_mapping
            )
        )
        # If this is a degenerate chain (no target node), we're done.
        if not self.target_node:
            return left_node_var_id_mapping_list
        
        right_node_var_id_mapping_list: list[dict[str, str]] = ( self.target_node._evaluate(
                fact_collection, start_entity_var_id_mapping
            )
        )
        LOGGER.warning('start_entity_var_id_mapping: %s\nright_node_var_id_mapping_list: %s\nleft_node_var_id_mapping_list: %s', start_entity_var_id_mapping, right_node_var_id_mapping_list, left_node_var_id_mapping_list)
        # LOGGER.warning("right_node_var_id_mapping_list: %s", right_node_var_id_mapping_list)
        # LOGGER.warning("left_node_var_id_mapping_list: %s", left_node_var_id_mapping_list)
        all_solutions: List[Dict[str, str]] = []
        for left_node_mapping, right_node_mapping in itertools.product(
            left_node_var_id_mapping_list, right_node_var_id_mapping_list
        ):
            if conflicting_vars(left_node_mapping, right_node_mapping):
                continue
            combined_mapping: dict[str, str] = left_node_mapping.copy()
            combined_mapping.update(right_node_mapping)
            left_entity_id: str = combined_mapping[
                self.source_node.node_name_label.name
            ]
            right_entity_id: str = combined_mapping[
                self.target_node.node_name_label.name
            ]
            relationships_satisfying_left_entity_id: set[str] = set(
                fact.relationship_id
                for fact in fact_collection.relationships_with_specific_source_node_facts(
                    left_entity_id
                )
            )
            relationships_satisfying_right_entity_id: set[str] = set(
                fact.relationship_id
                for fact in fact_collection.relationships_with_specific_target_node_facts(
                    right_entity_id
                )
            )
            LOGGER.debug('Start entity var id mapping: %s', left_entity_id)
            LOGGER.debug('Relationships satisfying left entity id %s: %s', left_entity_id, relationships_satisfying_left_entity_id)
            LOGGER.debug('Relationships satisfying right entity id %s: %s', right_entity_id, relationships_satisfying_right_entity_id)
            relationship_solutions: set[str] = (
                relationships_satisfying_left_entity_id
                & relationships_satisfying_right_entity_id
            )
            LOGGER.warning('Relationship solutions: %s', relationship_solutions)
            for relationship_solution in relationship_solutions:
                LOGGER.debug('relationship_solution: %s, %s', relationship_solution, combined_mapping)
                combined_mapping_copy: Dict[str, str] = copy.copy(
                    combined_mapping
                )
                combined_mapping_copy[
                    self.relationship.relationship.name_label.name  # Used to be relationship.relationship
                ] = relationship_solution
                all_solutions.append(combined_mapping_copy)
                LOGGER.debug('combined_mapping_copy: %s', combined_mapping_copy)
        LOGGER.debug('all_solutions: %s', all_solutions)
        return all_solutions

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.source_node.tree())
        if self.relationship:
            t.add(self.relationship.tree())
        if self.target_node:
            t.add(self.target_node.tree())
        return t

    @property
    def children(self) -> Generator[TreeMixin]:
        yield self.source_node
        yield self.relationship
        yield self.target_node


class Where(Evaluable, TreeMixin):
    """The all-important WHERE clause."""

    def __init__(self, predicate: Evaluable):
        self.predicate = predicate

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = {},
    ) -> List[Dict[str, str]]:
        return self.predicate._evaluate(
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )

    def __repr__(self):
        return f"Where({self.predicate})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.predicate.tree())
        return t

    @property
    def children(self):
        yield self.predicate


class And(BinaryBoolean, Evaluable):
    """
    Represents a logical AND operation between two boolean expressions.
    """

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = {},
    ) -> Any:
        left_value = self.left_side._evaluate(  # pylint: disable=protected-access
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(  # pylint: disable=protected-access
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        return Literal(left_value.value and right_value.value)


class Or(BinaryBoolean, Evaluable):
    """
    Represents a logical OR operation between two boolean expressions.

    """

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = {},
    ) -> Any:
        left_value = self.left_side._evaluate(  # pylint: disable=protected-access
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(  # pylint: disable=protected-access
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
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
        argument: Evaluable,
    ):
        self.argument: Evaluable = argument

    def __repr__(self):
        return f"{self.__class__.__name__}({self.argument})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.argument.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = {},
    ) -> Any:
        return Literal(
            not self.argument._evaluate(
                fact_collection,
                start_entity_var_id_mapping=start_entity_var_id_mapping,
            ).value
        )


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
        t: Tree = Tree(self.__class__.__name__)
        for relationship in self.relationships:
            t.add(relationship.tree())
        return t

    @property
    def children(self) -> Generator[RelationshipChain]:
        yield from self.relationships

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, str] = {},
    ) -> List[Dict[str, str]]:
        evaluations: List[List[Dict[str, str]]] = [
            relationship._evaluate(fact_collection, start_entity_var_id_mapping)
            for relationship in self.relationships
        ]
        LOGGER.warning('RelationshipChainList evaluations: %s', evaluations)
        # RelationshipChainList evaluations: [[{'i': 'Tract::01001020805', 'c': 'County::01001', 'r': '36085b73ab5d48c194d0daa7d859e9a7'}]]
        solutions: List[Dict[str, str]] = []
        for substitution_combination in itertools.product(*evaluations):
            # substitution_combination = substitution_combination[0]
            LOGGER.debug("substitution_combination: %s", substitution_combination)
            if conflicting_vars(*substitution_combination):
                continue
            LOGGER.debug("After conflicting variable test... %s", substitution_combination)
            combined_substitution: Dict[str, str] = {}
            for substitution in substitution_combination:
                combined_substitution.update(substitution)
            solutions.append(combined_substitution)
        LOGGER.warning('In RelationshipChainList solutions: %s', solutions)
        return solutions


class Addition(Evaluable, Predicate):
    """
    Represents an addition operation between two evaluable tree nodes.
    """

    def __init__(self, left: Evaluable, right: Evaluable):  # pylint: disable=super-init-not-called
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

    def tree(self) -> Tree:
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
    def children(self) -> Generator[Evaluable, None, None]:
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
        start_entity_var_id_mapping: Dict[str, str] | List[Dict[str, Any]] = {},
    ) -> Any:
        """
        Evaluate this Addition node.
        """
        left_value = self.left_side._evaluate(  # pylint: disable=protected-access
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(  # pylint: disable=protected-access
            fact_collection,
            start_entity_var_id_mapping=start_entity_var_id_mapping,
        )  # pylint: disable=protected-access
        return Literal(left_value.value + right_value.value)


class Literal(Evaluable, TreeMixin):
    """
    A class representing a literal value in a tree structure.

    Attributes:
        value (Any): The literal value.

    """

    def __init__(self, value: Any):
        while isinstance(value, Literal):
            value = value.value
        self.value = value

    def __repr__(self) -> str:
        return f"Literal({self.value})"

    def __bool__(self) -> bool:
        return self.value is True

    def tree(self):
        t: Tree = Tree(self.__class__.__name__)
        t.add(str(self.value))
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any] | List[Dict[str, Any]] = {},
    ) -> Any:
        return self

    def __eq__(self, other) -> Literal:
        return Literal(isinstance(other, Literal) and self.value == other.value)
