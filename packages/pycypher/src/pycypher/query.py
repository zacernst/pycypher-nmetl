"""
Query Module (query.py)
========================

The ``query.py`` module in the `pycypher` library defines the classes used to query the ``FactCollection``
for specific types of information. These queries are designed to retrieve data about nodes, relationships,
and their attributes based on defined criteria.
"""

from __future__ import annotations


class Query:  # pylint: disable=too-few-public-methods
    """Abstract base class for all query types.

    This class serves as the foundation for all query objects used to
    retrieve information from FactCollection instances.
    """


class NullResult:  # pylint: disable=too-few-public-methods
    """Represents an empty or null query result.

    This class is used as a placeholder when a query returns no results
    or when a value is not found in the fact collection.

    Attributes:
        query: The original query that produced this null result.
    """

    def __init__(self, query):
        """Initialize a NullResult with the originating query.

        Args:
            query: The query object that produced this null result.
        """
        self.query = query

    def __repr__(self):
        """Return string representation of the null result.

        Returns:
            String representation indicating this is a null result.
        """
        return "NullResult"

    def _evaluate(self, *args, **kwargs):
        """Evaluate method that returns self for null results.

        Args:
            *args: Variable positional arguments (ignored).
            **kwargs: Variable keyword arguments (ignored).

        Returns:
            Self, maintaining the null result through evaluation chains.
        """
        return self

    def __bool__(self) -> bool:
        """Make this falsey"""
        return False


class QueryValueOfNodeAttribute(Query):
    """Query for retrieving the value of a specific node attribute.

    This query type is used to get the value of a named attribute
    for a specific node in the graph.

    Attributes:
        node_id: Identifier of the node to query.
        attribute: Name of the attribute to retrieve.
    """  # pylint: disable=too-few-public-methods

    def __init__(self, node_id: str, attribute: str):
        """Initialize a node attribute value query.

        Args:
            node_id: Identifier of the node to query.
            attribute: Name of the attribute to retrieve.
        """
        self.node_id = node_id
        self.attribute = attribute

    def __repr__(self):
        return f"QueryValueOfNodeAttribute({self.node_id}, {self.attribute})"


class QueryValueOfRelationshipAttribute(Query):
    """Query for retrieving the value of a specific relationship attribute.

    This query type is used to get the value of a named attribute
    for a specific relationship in the graph.

    Attributes:
        relationship_id: Identifier of the relationship to query.
        attribute: Name of the attribute to retrieve.
    """  # pylint: disable=too-few-public-methods

    def __init__(self, relationship_id: str, attribute: str):
        """Initialize a relationship attribute value query.

        Args:
            relationship_id: Identifier of the relationship to query.
            attribute: Name of the attribute to retrieve.
        """
        self.relationship_id = relationship_id
        self.attribute = attribute

    def __repr__(self):
        return f"QueryValueOfRelationshipAttribute({self.relationship_id}, {self.attribute})"


class QueryNodeLabel(Query):
    """Query for retrieving the label of a specific node.

    This query type is used to get the label assigned to a specific
    node in the graph.

    Attributes:
        node_id: Identifier of the node to query.
    """  # pylint: disable=too-few-public-methods

    def __init__(self, node_id: str):
        """Initialize a node label query.

        Args:
            node_id: Identifier of the node to query.
        """
        self.node_id = node_id

    def __repr__(self):
        return f"QueryNodeLabel({self.node_id})"


class QueryRelationshipLabel(Query):
    """Query for retrieving the label of a specific relationship.

    This query type is used to get the label assigned to a specific
    relationship in the graph.

    Attributes:
        relationship_id: Identifier of the relationship to query.
    """  # pylint: disable=too-few-public-methods

    def __init__(self, relationship_id: str):
        """Initialize a relationship label query.

        Args:
            relationship_id: Identifier of the relationship to query.
        """
        self.relationship_id = relationship_id

    def __repr__(self):
        return f"QueryRelationshipLabel({self.relationship_id})"


class QueryRelationshipsWithSourceNode(Query):
    """Not implemented"""  # pylint: disable=too-few-public-methods

    def __init__(self, node_id: str):
        self.node_id = node_id

    def __repr__(self):
        return f"QueryRelationshipsWithSourceNode({self.node_id})"


class QueryRelationshipsWithTargetNode(Query):
    """Not implemented"""  # pylint: disable=too-few-public-methods

    def __init__(self, node_id: str):
        self.node_id = node_id

    def __repr__(self):
        return f"QueryRelationshipsWithTargetNode({self.node_id})"


class QueryTargetNodeOfRelationship(Query):
    """Not implemented"""  # pylint: disable=too-few-public-methods

    def __init__(self, relationship_id: str):
        self.relationship_id = relationship_id

    def __repr__(self):
        return f"QueryTargetNodeOfRelationship({self.relationship_id})"


class QuerySourceNodeOfRelationship(Query):
    """Not implemented"""  # pylint: disable=too-few-public-methods

    def __init__(self, relationship_id: str):
        self.relationship_id = relationship_id

    def __repr__(self):
        return f"QuerySourceNodeOfRelationship({self.relationship_id})"
