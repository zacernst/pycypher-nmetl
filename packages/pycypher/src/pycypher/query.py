"""
Query Module (query.py)
========================

The ``query.py`` module in the `pycypher` library defines the classes used to query the ``FactCollection`` for specific types of information. These queries are designed to retrieve data about nodes, relationships, and their attributes based on defined criteria.

Core Concepts
-------------

*   **Queries:** A query represents a request for specific information from a ``FactCollection``. Queries are used to retrieve facts that match certain conditions.
*   **Fact Collection:** The ``FactCollection`` is the data structure that stores the facts.
*   **Nodes:** Entities within the graph.
*   **Relationships:** Connections or associations between nodes.
*   **Attributes:** Properties associated with nodes or relationships.
* **NullResult**: A placeholder object returned when a query finds no matching facts.

Key Classes
-----------

1.  ``Query`` (Abstract Base Class)
    --------------------------------

    *   **Purpose:** The abstract base class for all query types. It defines a common interface for query objects.
    *   **Responsibilities:**
        *   Serves as a parent class for specific query implementations.

2.  ``NullResult``
    ----------------

    *   **Purpose:** Represents the absence of a result for a query.
    *   **Responsibilities:**
        *   Indicates that a query found no facts matching the specified criteria.
    * **Attributes:**
        * `query`: A reference to the query that produced this result.
    *   **Key Methods:**
        *   ``__init__(self, query: Query)``: Initializes a ``NullResult`` instance.
        *  ``__repr__(self) -> str``: Returns a string representation of the object.
        * ``_evaluate(self, *args, **kwargs)``: a placeholder method for evaluating the query.

3.  ``QueryValueOfNodeAttribute``
    ------------------------------

    *   **Purpose:** A query to retrieve the value of a specific attribute for a given node.
    *   **Responsibilities:**
        *   Finds a fact in the ``FactCollection`` where a node has a certain attribute with a certain value.
    *   **Attributes:**
        *   ``node_id`` (``str``): The ID of the node.
        *   ``attribute`` (``str``): The name of the attribute.
    *   **Key Methods:**
        *   ``__init__(self, node_id: str, attribute: str)``: Initializes the query.
        *   ``__repr__(self) -> str``: Returns a string representation of the query.

4.  ``QueryValueOfRelationshipAttribute``
    --------------------------------------

    *   **Purpose:** A query to retrieve the value of a specific attribute for a given relationship.
    *   **Responsibilities:**
        *   Finds a fact in the ``FactCollection`` where a relationship has a certain attribute with a certain value.
    *   **Attributes:**
        *   ``relationship_id`` (``str``): The ID of the relationship.
        *   ``attribute`` (``str``): The name of the attribute.
    *   **Key Methods:**
        *   ``__init__(self, relationship_id: str, attribute: str)``: Initializes the query.
        *   ``__repr__(self) -> str``: Returns a string representation of the query.

5.  ``QueryNodeLabel``
    -------------------

    *   **Purpose:** A query to retrieve the label of a given node.
    *   **Responsibilities:**
        *   Finds a fact in the ``FactCollection`` where a node has a specific label.
    *   **Attributes:**
        *   ``node_id`` (``str``): The ID of the node.
    *   **Key Methods:**
        *   ``__init__(self, node_id: str)``: Initializes the query.
        *   ``__repr__(self) -> str``: Returns a string representation of the query.

6.  ``QueryRelationshipLabel``
    --------------------------

    *   **Purpose:** A query to retrieve the label of a given relationship.
    *   **Responsibilities:**
        *   Finds a fact in the ``FactCollection`` where a relationship has a specific label.
    *   **Attributes:**
        *   ``relationship_id`` (``str``): The ID of the relationship.
    *   **Key Methods:**
        *   ``__init__(self, relationship_id: str)``: Initializes the query.
        *   ``__repr__(self) -> str``: Returns a string representation of the query.

7. ``QueryRelationshipsWithSourceNode``
   -----------------------------------

    *   **Purpose:** A query to retrieve all relationships where a given node is the source.
    *   **Responsibilities:**
        *   Finds facts in the ``FactCollection`` where a given node is the source of a relationship.
    *   **Attributes:**
        *   ``node_id`` (``str``): The ID of the source node.
    *   **Key Methods:**
        *   ``__init__(self, node_id: str)``: Initializes the query.
        *   ``__repr__(self) -> str``: Returns a string representation of the query.

8. ``QueryRelationshipsWithTargetNode``
    -----------------------------------

    *   **Purpose:** A query to retrieve all relationships where a given node is the target.
    *   **Responsibilities:**
        *   Finds facts in the ``FactCollection`` where a given node is the target of a relationship.
    *   **Attributes:**
        *   ``node_id`` (``str``): The ID of the target node.
    *   **Key Methods:**
        *   ``__init__(self, node_id: str)``: Initializes the query.
        *   ``__repr__(self) -> str``: Returns a string representation of the query.

9. ``QueryTargetNodeOfRelationship``
   ---------------------------------

    *   **Purpose:** A query to retrieve the target node of a specific relationship.
    *   **Responsibilities:**
        *   Finds facts in the ``FactCollection`` where a given relationship has a specific target node.
    *   **Attributes:**
        *   ``relationship_id`` (``str``): The ID of the relationship.
    *   **Key Methods:**
        *   ``__init__(self, relationship_id: str)``: Initializes the query.
        *   ``__repr__(self) -> str``: Returns a string representation of the query.

10. ``QuerySourceNodeOfRelationship``
    ---------------------------------

    *   **Purpose:** A query to retrieve the source node of a specific relationship.
    *   **Responsibilities:**
        *   Finds facts in the ``FactCollection`` where a given relationship has a specific source node.
    *   **Attributes:**
        *   ``relationship_id`` (``str``): The ID of the relationship.
    *   **Key Methods:**
        *   ``__init__(self, relationship_id: str)``: Initializes the query.
        *   ``__repr__(self) -> str``: Returns a string representation of the query.

Workflow
--------

1.  **Query Creation:** A query object is created, specifying the type of information being requested (e.g., `QueryNodeLabel`, `QueryValueOfNodeAttribute`).
2.  **Query Execution:** The query object is passed to the ``query()`` method of the ``FactCollection``.
3.  **Fact Matching:** The ``FactCollection`` finds the facts that match the query's criteria.
4.  **Result Retrieval:** The result is returned, or `NullResult` if no match is found.

Key Features
------------

*   **Extensibility:** The module is designed to be extended with new query types by creating new subclasses of ``Query``.
*   **Targeted Information:** Queries are focused on retrieving specific types of data.
* **NullResult**: Indicates when no results are found.

Use Cases
---------

*   **Trigger Evaluation:** Determining if the constraints for a trigger have been met.
*   **Data Retrieval:** Retrieving specific information about the graph data for analysis.
*   **Graph Exploration:** Exploring the relationships and properties of nodes in the graph.
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
