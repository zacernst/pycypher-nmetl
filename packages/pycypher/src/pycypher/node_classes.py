"""
This module defines various classes representing nodes and expressions in an
Abstract Syntax Tree (AST) for Cypher queries.
"""  # pylint: disable=too-many-lines

from __future__ import annotations

import abc
import collections
import uuid
import itertools
from typing import Any, Dict, Generator, List, Optional, Set

from pycypher.fact_collection import FactCollection
from pycypher.query import (
    NullResult,
    QuerySourceNodeOfRelationship,
    QueryTargetNodeOfRelationship,
    QueryNodeLabel,
    QueryValueOfNodeAttribute,
)
from pycypher.solutions import Projection, ProjectionList
from pycypher.tree_mixin import TreeMixin
from rich.tree import Tree
from shared.logger import LOGGER

LOGGER.setLevel("WARNING")


def get_free_variable_substitutions(
    fact_collection: FactCollection,
    relationship_chain_list: RelationshipChainList,
    projection: Projection,
):
    free_variables: dict[str, list[Node]] = (
        relationship_chain_list.free_variables(projection=projection)
    )
    possible_variable_substitutions: dict[str, list[str]] = {}
    for variable_name, vertices in free_variables.items():
        possible_variable_substitutions[variable_name] = []
        for vertex in vertices:
            if hasattr(vertex, "name_label") and hasattr(
                vertex.name_label, "label"
            ):
                for node_with_label in fact_collection.nodes_with_label(
                    vertex.name_label.label,
                ):
                    possible_variable_substitutions[variable_name].append(
                        node_with_label,
                    )
                for (
                    relationship_with_label
                ) in fact_collection.relationships_with_label(
                    vertex.name_label.label,
                ):
                    possible_variable_substitutions[variable_name].append(
                        relationship_with_label,
                    )

    return possible_variable_substitutions


def get_all_substitutions(
    fact_collection: FactCollection, relationship_chain_list, projection_list
) -> ProjectionList:
    free_variable_substitutions = get_free_variable_substitutions(
        fact_collection,
        relationship_chain_list,
        projection_list[0],
    )

    projection_list_output: ProjectionList = ProjectionList([])

    for projection in projection_list:
        for variable, value in projection.projection.items():
            free_variable_substitutions[variable] = [value]
        for substitution_tuple in itertools.product(
            *free_variable_substitutions.values()
        ):
            variable_list: list[str] = list(free_variable_substitutions.keys())
            substitution_dict: dict[str, str] = dict(
                zip(variable_list, substitution_tuple)
            )
            substitution_projection: Projection = Projection(
                projection=substitution_dict,
            )
            projection_list_output += substitution_projection
    return projection_list_output


class EvaluationError(Exception):
    """
    Custom exception class for evaluation errors.
    """


class Evaluable(TreeMixin, abc.ABC):  # pylint: disable=too-few-public-methods
    """
    Abstract base class representing an evaluable entity.
    """

    @property
    def is_aggregation(self) -> bool:
        """
        Returns whether the WithClause contains aggregation functions.
        """
        for obj in self.walk():
            if isinstance(obj, Aggregation):
                return True
        return False


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
            LOGGER.debug("Walking for attribute names...")
            self._attribute_names = [
                obj.attribute
                for obj in self.walk()
                if isinstance(obj, ObjectAttributeLookup)
            ]
        else:
            LOGGER.debug("Cached attribute names...")
        LOGGER.debug("Attribute names: %s", self._attribute_names)
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

class Aggregation:
    pass  # mix-in class for functions that aggregate values into lists


# class Aggregation(Evaluable, TreeMixin):
#     """
#     Represents an aggregation operation that transforms a list into a singleton value.
# 
#     Attributes:
#         aggregation: The aggregation operation to be performed.
# 
#     """
# 
#     def __init__(self, aggregation):
#         self.aggregation = aggregation
# 
#     def disaggregate(self) -> Evaluable:
#         return self.aggregation.object_attribute_lookup
# 
#     @property
#     def children(self) -> Generator[Evaluable]:
#         """
#         Generator function that yields the aggregation attribute.
# 
#         Yields:
#             The aggregation attribute of the instance.
#         """
#         yield self.aggregation
# 
#     def __repr__(self) -> str:
#         """
#         Return a string representation of the Aggregation object.
# 
#         Returns:
#             str: A string in the format "Aggregation(<aggregation_value>)".
#         """
#         return f"Aggregation({self.aggregation})"
# 
#     def tree(self) -> Tree:
#         """
#         Generates a tree representation of the current node class.
# 
#         Returns:
#             Tree: A tree object representing the current node class and its aggregation.
#         """
#         t: Tree = Tree(self.__class__.__name__)
#         t.add(self.aggregation.tree())
#         return t
# 
#     def _evaluate(
#         self,
#         fact_collection: FactCollection,
#         start_entity_var_id_mapping: Dict[str, Any]
#         | List[Dict[str, Any]] = [],
#     ) -> Any:
#         """
#         Evaluates the aggregation using the provided fact collection and projection.
# 
#         Args:
#             fact_collection (FactCollection): The collection of facts to be used in the evaluation.
#             projection (Optional[Dict[str, str | List[str]]]): An optional dictionary specifying
#                 the projection to be applied during the evaluation. The keys are the projection
#                 names, and the values are either a single string or a list of strings representing
#                 the projection fields.
# 
#         Returns:
#             Any: The result of the aggregation evaluation.
#         """
#         return self.aggregation._evaluate(  # pylint: disable=protected-access
#             fact_collection,
#             start_entity_var_id_mapping=start_entity_var_id_mapping,
#         )


class Collection(Evaluable, TreeMixin):  # i.e. a list
    """
    A class representing a collection of evaluable items.

    Attributes:
        values (List[Evaluable]): A list of evaluable items.

    """

    def __init__(self, values: List[Evaluable]):
        self.values = values
        self.value = values  # Make this consistent later...

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

    def bak_evaluate(
        self,
        fact_collection: FactCollection,
        start_entity_var_id_mapping: Dict[str, Any]
        | List[Dict[str, Any]] = {},
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
        return Collection([
            value._evaluate(
                fact_collection,
                start_entity_var_id_mapping=start_entity_var_id_mapping,
            )
            for value in self.values
        ])

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
        start_entity_var_id_mapping: Dict[str, Any]
        | List[Dict[str, Any]] = [],
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


class Size(Evaluable, TreeMixin, Aggregation):
    """
    Represents a size operation on a collection, like a ``len`` in Python. The difference
    is that we don't count NULL values.

    Attributes:
        collection (Collection): The collection whose size is to be evaluated.

    """

    def __init__(self, collect: Collect):
        self.collect = collect

    @property
    def children(self):
        yield self.collect

    def __repr__(self) -> str:
        return f"Size({self.collect})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.collect.tree())
        return t

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection_list: ProjectionList = ProjectionList([])
    ) -> Literal:
        collection: Collection = self.collect._evaluate(fact_collection, projection_list=projection_list)
        return Literal(
            value=len([i for i in collection.values if not isinstance(i, NullResult)])
        )
    
    def disaggregate(self):
        return self.collect.disaggregate()
        


class Collect(Evaluable, TreeMixin, Aggregation):
    """
    A class that represents a collection of objects based on an attribute lookup.

    Attributes:
        object_attribute_lookup (ObjectAttributeLookup): The attribute lookup object
            used to collect instances.

    """

    def __init__(self, object_attribute_lookup: ObjectAttributeLookup):
        self.object_attribute_lookup = object_attribute_lookup
    
    def disaggregate(self):
        return self.object_attribute_lookup

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
        projection_list: ProjectionList,
    ) -> Collection:
        """ """
        result: Collection = Collection(
            values=[
                self.object_attribute_lookup._evaluate(
                    fact_collection, projection=projection
                )
                for projection in projection_list
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
        projection_list: ProjectionList = ProjectionList([]),
    ) -> ProjectionList:
        match_clause_results: ProjectionList = self.match_clause._evaluate(fact_collection, projection_list=projection_list)
        return_clause_results: ProjectionList = self.return_clause._evaluate(fact_collection, projection_list=match_clause_results)
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
        return (
            f"{self.__class__.__name__}({self.left_side}, {self.right_side})"
        )


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
        return (
            f"{self.__class__.__name__}({self.left_side}, {self.right_side})"
        )

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
        projection: Projection,
    ) -> Projection:
        return projection[self.name]


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
        projection: Projection,
    ) -> Literal:
        left_value = self.left_side._evaluate(
            fact_collection,
            projection=projection,
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(
            fact_collection,
            projection=projection,
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
        start_entity_var_id_mapping: Dict[str, str]
        | List[Dict[str, Any]] = {},
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
        start_entity_var_id_mapping: Dict[str, str]
        | List[Dict[str, Any]] = {},
    ) -> Any:
        if isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(
                f"Error while evaluating GreaterThan: {self}"
            )
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
        start_entity_var_id_mapping: Dict[str, str]
        | List[Dict[str, Any]] = {},
    ) -> Any:
        if isinstance(start_entity_var_id_mapping, list):
            raise EvaluationError(
                f"Error while evaluating Subtraction: {self}"
            )
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
        start_entity_var_id_mapping: Dict[str, str]
        | List[Dict[str, Any]] = {},
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
        start_entity_var_id_mapping: Dict[str, str]
        | List[Dict[str, Any]] = {},
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
        projection: Projection,
    ) -> Literal:
        """TODO: Need to handle the case where the attribute is `None`"""
        one_query: QueryValueOfNodeAttribute = QueryValueOfNodeAttribute(
            node_id=projection[self.object],
            attribute=self.attribute,
        )
        value: Any = fact_collection.query(one_query)
        return value


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

    def disaggregate(self) -> Alias:
        return Alias(reference=self.reference.disaggregate(), alias=self.alias)

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
        projection: Projection,
    ) -> Projection:
        """Get the value of the reference and assign it to the alias."""
        result = self.reference._evaluate(  # pylint: disable=protected-access
            fact_collection,
            projection=projection,
        )
        mapping: Projection = Projection(projection={self.alias: result})
        return mapping

    @property
    def children(self):
        yield self.reference
        yield self.alias

class ReturnProjection(Evaluable, TreeMixin):
    """ """

    def __init__(self, lookups: List[Alias]):
        self.lookups = lookups

    def __repr__(self):
        return f"ReturnProjection({self.lookups})"

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
        projection: Projection,
    ) -> Projection:
        # import pdb; pdb.set_trace()
        result: Projection = Projection({
            lookup.alias: projection[lookup.reference]
            for lookup in self.lookups
        })
        return result

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
        projection: Projection,
    ) -> Projection:
        result: Projection = Projection({
            lookup.alias: lookup.reference._evaluate(
                fact_collection,
                projection=projection,
            )
            for lookup in self.lookups
        })
        return result


class WithClause(Evaluable, TreeMixin):
    """
    Represents a WITH clause in a query, which is used to manage projections and aggregations.

    Attributes:
        object_as_series (ObjectAsSeries): The object representing the series of projections.

    """

    def __init__(self, lookups: ObjectAsSeries):
        self.lookups = lookups
        self.incoming_projection_list: ProjectionList = ProjectionList([])

    def __repr__(self):
        return f"WithClause({self.lookups})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.lookups.tree())
        return t

    def disaggregated_with_clause(self) -> WithClause:
        disaggregated_with_clause: WithClause = WithClause(
            lookups=ObjectAsSeries(
                lookups=[ # Need to change this to walk until we find Aggregation
                    alias.disaggregate() if alias.is_aggregation else alias
                    for alias in self.lookups.lookups
                ]
            )
        )
        return disaggregated_with_clause

    @property
    def children(self) -> Generator[ObjectAsSeries]:
        """
        Generator that yields the children of the current node.

        Yields:
        Projection: The object as a series.
        """

        yield self.lookups

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection_list: ProjectionList,
    ) -> ProjectionList:
        if self.is_aggregation:  # Skipping for now
            result: ProjectionList = self._evaluate_aggregation(
                fact_collection, projection_list
            )
        else:
            result: ProjectionList = self._evaluate_non_aggregation(
                fact_collection, projection_list
            )
        return result

    def aggregated_aliases(self) -> List[Alias]:
        aggregation_alias_list: List[Alias] = []
        for alias in self.lookups.lookups:
            if alias.is_aggregation:
                aggregation_alias_list.append(alias)
        return aggregation_alias_list

    def non_aggregated_aliases(self) -> List[Alias]:
        non_aggregation_alias_list: List[Alias] = []
        for alias in self.lookups.lookups:
            if not alias.is_aggregation:
                non_aggregation_alias_list.append(alias)
        return non_aggregation_alias_list

    def _evaluate_aggregation(
        self,
        fact_collection: FactCollection,
        projection_list: ProjectionList,
    ) -> ProjectionList:
        # Get the aliases that have aggregations and the ones that don't
        self.incoming_projection_list = projection_list

        aggregated_alias_list: List[Alias] = self.aggregated_aliases()

        non_aggregated_alias_list: List[Alias] = self.non_aggregated_aliases()
        non_aggregated_projection_list: ProjectionList = ProjectionList([])

        for projection in projection_list:
            sub_projection: Projection = Projection({})
            for alias in non_aggregated_alias_list:
                sub_projection.update(
                    alias._evaluate(fact_collection, projection=projection)
                )
            non_aggregated_projection_list += sub_projection

        non_aggregated_projection_list.unique()

        disaggregated_with_clause: WithClause = (
            self.disaggregated_with_clause()
        )

        disaggregated_projection_solutions: ProjectionList = (
            disaggregated_with_clause._evaluate(
                fact_collection, projection_list=projection_list
            )
        )

        bucketed: Dict[Projection, ProjectionList] = {}
        for projection in non_aggregated_projection_list:
            bucketed[projection] = ProjectionList([])

        for solution in disaggregated_projection_solutions.zip(
            self.incoming_projection_list
        ):
            for p, v in bucketed.items():
                if p < solution:
                    v += solution

        # zip together the disaggregated_projection_solutions wtih the self.incoming_projection_list

        # for each ProjectionList in the values of bucketed:
        #    for each alias in aggregated_alias_list:
        #        evaluate the alias relative to the ProjectionList
        for bucket, projection_list in bucketed.items():
            aggregation_evaluations = {}
            for alias in aggregated_alias_list:
                aggregation: Evaluable = alias.reference # getattr(alias.reference, dispatch_dict[alias.reference.__class__])
                evaluation = aggregation._evaluate(
                    fact_collection, projection_list=projection_list
                )
                aggregation_evaluations[alias.alias] = evaluation
            bucket.projection.update(aggregation_evaluations)

        solutions: ProjectionList = ProjectionList(
            projection_list=list(bucketed.keys())
        )

        return solutions

    def _evaluate_non_aggregation(
        self,
        fact_collection: FactCollection,
        projection_list: ProjectionList,
    ) -> ProjectionList:
        new_projection_list: ProjectionList = ProjectionList([])
        for projection in projection_list:
            new_projection: Projection = Projection({})
            for lookup in self.lookups.lookups:
                value: Any = lookup.reference._evaluate(
                    fact_collection=fact_collection,
                    projection=projection,
                )
                new_projection[lookup.alias] = value
            new_projection_list += new_projection
        return new_projection_list


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
        return (
            f"Match({self.pattern}, {self.where_clause}, {self.with_clause})"
        )

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
        projection_list: ProjectionList,
    ) -> ProjectionList:
        """First, evaluate the pattern;
        Then evaluate the WithClause;
        Then the WhereClause;
        Send to ReturnClause
        """
        pattern_step_input: ProjectionList = get_all_substitutions(
            fact_collection, self.pattern, projection_list
        )
        pattern_step_output: ProjectionList = ProjectionList(projection_list=[
            projection for projection in pattern_step_input if 
            self.pattern._evaluate(
                fact_collection, 
                projection=projection
            )
        ]
        )

        if self.with_clause:
            projection_list_step: ProjectionList = self.with_clause._evaluate(
                fact_collection, pattern_step_output
            )
        if self.where_clause:
            pass
        return projection_list_step


class Return(TreeMixin):
    """
    The Return class represents a RETURN clause in a Cypher query. It is used to specify
    which projections (columns) should be returned from the query.

    Attributes:
        projection (Projection): The projection node that specifies the columns to be returned.

    """

    def __init__(self, projection: ObjectAsSeries):
        self.projection = projection

    def __repr__(self):
        return f"Return[{self.projection}]"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        t.add(self.projection.tree())
        return t

    @property
    def children(self):
        yield self.projection

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection_list: ProjectionList,
    ) -> ProjectionList:
        out: ProjectionList = ProjectionList(
            projection_list=[
                self.projection._evaluate(
                    fact_collection,
                    projection=incoming_projection,
                )
                for incoming_projection in projection_list
            ]
        )
        return out


class BakProjection(TreeMixin):
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
        name_label: NodeNameLabel | None = None,
        mapping_set: MappingSet | None = None,
    ) -> None:
        self.name_label: NodeNameLabel = name_label or NodeNameLabel()
        self.mapping_set: MappingSet = mapping_set or MappingSet([])

    def _evaluate(
        self,
        fact_collection: FactCollection,
        projection: Projection,
    ) -> Literal:
        if hasattr(self, "name_label") and hasattr(self.name_label, "name"):
            query: QueryNodeLabel = QueryNodeLabel(
                projection[self.name_label.name]
            )
            node_label: str = fact_collection.query(query)
            if node_label != self.name_label.label:
                return Literal(False)

        for mapping in self.mapping_set.mappings:
            attribute: str = mapping.key
            node_id: str = projection[self.name_label.name]
            attribute_query: QueryValueOfNodeAttribute = (
                QueryValueOfNodeAttribute(node_id=node_id, attribute=attribute)
            )
            attribute_value: Literal = fact_collection.query(attribute_query)
            if attribute_value != mapping.value:
                return Literal(False)

        return Literal(True)

    def __repr__(self):
        return f"Node({self.name_label}, {self.mapping_set})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
        if self.name_label:
            t.add(self.name_label.tree())
        if self.mapping_set:
            t.add(self.mapping_set.tree())
        return t

    @property
    def children(self) -> Generator[NodeNameLabel | Mapping]:
        if self.name_label:
            yield self.name_label
        if self.mapping_set:
            for mapping in self.mapping_set.mappings:
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
        t: Tree = Tree(self.__class__.__name__)
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
    ) -> None:
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
        projection: Projection,
    ) -> Literal:
        """Assume that self.steps is exactly (Node, Relationship, Node,)."""
        source_node_eval: Literal = self.source_node._evaluate(
            fact_collection=fact_collection, projection=projection
        )

        if not self.target_node:
            return source_node_eval

        target_node_eval: Literal = self.target_node._evaluate(
            fact_collection=fact_collection, projection=projection
        )
        source_node_id: str = projection[self.source_node.name_label.name]
        target_node_id: str = projection[self.target_node.name_label.name]
        relationship_id: str = projection[
            self.relationship.relationship.name_label.name
        ]
        relationship_source_node_query: QuerySourceNodeOfRelationship = (
            QuerySourceNodeOfRelationship(relationship_id=relationship_id)
        )
        relationship_target_node_query: QueryTargetNodeOfRelationship = (
            QueryTargetNodeOfRelationship(relationship_id=relationship_id)
        )
        actual_source_node_id: str = fact_collection.query(
            relationship_source_node_query
        )
        actual_target_node_id: str = fact_collection.query(
            relationship_target_node_query
        )
        # TODO: Looks like relationship is evaluating to True regardless!
        out = Literal(
            source_node_eval
            and target_node_eval
            and actual_source_node_id == source_node_id
            and actual_target_node_id == target_node_id
        )
        return out
        # If this is a degenerate chain (no target node), we're done.

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
        projection: Projection,
    ) -> Literal:
        evaluation: Any = self.predicate._evaluate(
            fact_collection,
            projection=projection,
        )
        if not isinstance(evaluation, Literal) or not isinstance(
            evaluation.value, bool
        ):
            raise ValueError(
                "Expected WHERE clause to evaluate to Literal[bool]."
            )
        return evaluation

    def __repr__(self) -> str:
        return f"Where({self.predicate})"

    def tree(self) -> Tree:
        t: Tree = Tree(self.__class__.__name__)
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
        projection: Projection,
    ) -> Any:
        left_value = self.left_side._evaluate(  # pylint: disable=protected-access
            fact_collection,
            projection=projection,
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(  # pylint: disable=protected-access
            fact_collection,
            projection=projection,
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
        projection: Projection,
    ) -> Any:
        left_value = self.left_side._evaluate(  # pylint: disable=protected-access
            fact_collection,
            projection=projection,
        )  # pylint: disable=protected-access
        right_value = self.right_side._evaluate(  # pylint: disable=protected-access
            fact_collection,
            projection=projection,
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
        projection: Projection,
    ) -> Any:
        return Literal(
            not self.argument._evaluate(
                fact_collection,
                projection=projection,
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

    def __repr__(self) -> str:
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
        projection: Projection,
    ) -> bool:
        evaluation: bool = all(
            relationship._evaluate(fact_collection, projection=projection)
            for relationship in self.relationships
        )
        return evaluation

    def free_variables(
        self, projection: Projection
    ) -> Dict[str, List[TreeMixin]]:
        free_variable_dict: Dict[str, List[TreeMixin]] = (
            collections.defaultdict(list)
        )
        for variable_name, vertex in self.get_node_variables():
            if variable_name not in projection.projection.keys():
                free_variable_dict[variable_name].append(vertex)
        return free_variable_dict


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
        t: Tree = Tree(self.__class__.__name__)
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
        start_entity_var_id_mapping: Dict[str, str]
        | List[Dict[str, Any]] = {},
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

    def __hash__(self) -> int:
        return hash(self.value)

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
        projection: Projection,
    ) -> Literal:
        return self

    def __eq__(self, other) -> Literal:
        return Literal(
            isinstance(other, Literal) and self.value == other.value
        )

    def __and__(self, other) -> bool:
        if not isinstance(
            other,
            (
                Literal,
                bool,
            ),
        ):
            raise ValueError()
        other_value = other if isinstance(other, bool) else other.value
        return self.value and other_value

    def __or__(self, other) -> bool:
        if not isinstance(
            other,
            (
                Literal,
                bool,
            ),
        ):
            raise ValueError()
        other_value = other if isinstance(other, bool) else other.value
        return self.value or other_value

    def __add__(self, other: Literal) -> int | float:
        if not isinstance(
            self.value,
            (
                int,
                float,
            ),
        ) or not isinstance(
            other.value,
            (
                int,
                float,
            ),
        ):
            raise ValueError()
        return self.value + other.value
