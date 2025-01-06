"""Queries are searches for facts..."""

from __future__ import annotations


class Query:
    pass


class QueryValueOfNodeAttribute(Query):
    def __init__(self, node_id: str, attribute: str):
        self.node_id = node_id
        self.attribute = attribute


class QueryValueOfRelationshipAttribute(Query):
    """Not implemented"""

    def __init__(self, relationship_id: str, attribute: str):
        self.relationship_id = relationship_id
        self.attribute = attribute
