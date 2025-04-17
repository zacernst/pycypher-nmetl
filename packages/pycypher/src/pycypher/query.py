"""
Query Module (query.py)
========================

The ``query.py`` module in the `pycypher` library defines the classes used to query the ``FactCollection``
for specific types of information. These queries are designed to retrieve data about nodes, relationships,
and their attributes based on defined criteria.
"""

from __future__ import annotations


class Query:  # pylint: disable=too-few-public-methods
    """Abstract base class."""


class NullResult:  # pylint: disable=too-few-public-methods
    """Stands in for a result that's empty."""

    def __init__(self, query):
        self.query = query

    def __repr__(self):
        return "NullResult"

    def _evaluate(self, *args, **kwargs):
        return self


class QueryValueOfNodeAttribute(Query):
    """Ask what the value of a specific attribute is for a specific node."""  # pylint: disable=too-few-public-methods

    def __init__(self, node_id: str, attribute: str):
        self.node_id = node_id
        self.attribute = attribute

    def __repr__(self):
        return f"QueryValueOfNodeAttribute({self.node_id}, {self.attribute})"


class QueryValueOfRelationshipAttribute(Query):
    """Not implemented"""  # pylint: disable=too-few-public-methods

    def __init__(self, relationship_id: str, attribute: str):
        self.relationship_id = relationship_id
        self.attribute = attribute

    def __repr__(self):
        return f"QueryValueOfRelationshipAttribute({self.relationship_id}, {self.attribute})"


class QueryNodeLabel(Query):
    """Not implemented"""  # pylint: disable=too-few-public-methods

    def __init__(self, node_id: str):
        self.node_id = node_id

    def __repr__(self):
        return f"QueryNodeLabel({self.node_id})"


class QueryRelationshipLabel(Query):
    """Not implemented"""  # pylint: disable=too-few-public-methods

    def __init__(self, relationship_id: str):
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
