"""
Solver Module (solver.py)
==========================

The ``solver.py`` module in the `pycypher` library defines a set of classes for representing and evaluating constraints within a graph-like data structure. These constraints are used to determine if certain conditions are met within the graph, particularly in the context of triggering reactive behaviors (e.g., with ``CypherTrigger``).

Core Concepts
-------------

*   **Constraints:**  A constraint is a condition that must be satisfied for a certain action to occur. In this module, constraints define rules about nodes, relationships, and their properties.
*   **Predicate:** A predicate is a function that returns a boolean value (True or False) based on some input.
* **Triggers**: A Trigger is an object that fires when all of its constraints are satisfied.
*   **Graph-Like Data:** The constraints operate on data that can be represented as a graph, consisting of nodes, relationships, and their attributes.
*   **Reactive Behavior:** Constraints are used to implement reactive behavior, where actions are triggered automatically when specific conditions are met in the graph data.

Key Classes
-----------

1.  ``Constraint`` (Abstract Base Class)
    --------------------------------------

    *   **Purpose:** This is the abstract base class for all constraint types. It defines a common interface for constraint objects.
    *   **Responsibilities:**
        *   Serves as a parent class for all specific constraint implementations.
        *   Provides a `trigger` attribute to link the constraint back to the trigger that contains it.
    *   **Key Methods:**
        *   ``__init__(self, trigger: Optional["CypherTrigger"] = None)``: Initializes the constraint and optionally associates it with a trigger.
            *   **Parameters:**
                *   ``trigger`` (``Optional["CypherTrigger"]``): The trigger this constraint belongs to (if any).

2.  ``IsTrue``
    -----------

    *   **Purpose:** Represents a constraint that checks if a given predicate is true.
    *   **Responsibilities:**
        *   Evaluates a predicate to determine if the constraint is satisfied.
    *   **Attributes:**
        *   ``predicate`` (``Predicate``): The predicate to be evaluated.
    *   **Key Methods:**
        *   ``__init__(self, predicate: "Predicate", **kwargs)``: Initializes the constraint with a predicate.
            *   **Parameters:**
                *   ``predicate`` (``Predicate``): The predicate to be evaluated.
        *   ``__repr__(self) -> str``: Returns a string representation of the constraint.
        *   ``__eq__(self, other: Any) -> bool``: Checks if this constraint is equal to another object.

3.  ``ConstraintVariableRefersToSpecificObject``
    ------------------------------------------------

    *   **Purpose:** Represents a constraint that checks if a variable refers to a specific object (node ID).
    *   **Responsibilities:**
        *   Determines if a variable in a Cypher-like query refers to a specific node ID.
    *   **Attributes:**
        *   ``variable`` (``str``): The name of the variable.
        *   ``node_id`` (``str``): The ID of the node that the variable should refer to.
    *   **Key Methods:**
        *   ``__init__(self, variable: str, node_id: str, **kwargs)``: Initializes the constraint.
            *   **Parameters:**
                *   ``variable`` (``str``): The variable name.
                *   ``node_id`` (``str``): The node ID.
        *   ``__repr__(self) -> str``: Returns a string representation of the constraint.
        *   ``__eq__(self, other: Any) -> bool``: Checks if this constraint is equal to another object.

4.  ``ConstraintNodeHasLabel``
    ---------------------------

    *   **Purpose:** Represents a constraint that a node must have a specific label.
    *   **Responsibilities:**
        *   Determines if a given node has a particular label.
    *   **Attributes:**
        *   ``variable`` (``str``): The name of the variable referring to the node.
        *   ``label`` (``str``): The label that the node must have.
    *   **Key Methods:**
        *   ``__init__(self, variable: str, label: str, **kwargs)``: Initializes the constraint.
            *   **Parameters:**
                *   ``variable`` (``str``): The variable name.
                *   ``label`` (``str``): The required label.
        *   ``__repr__(self) -> str``: Returns a string representation of the constraint.
        *   ``__hash__(self) -> int``: Returns a hash value for the constraint.
        *   ``__eq__(self, other: Any) -> bool``: Checks if this constraint is equal to another object.

5.  ``ConstraintRelationshipHasSourceNode``
    ----------------------------------------

    *   **Purpose:** Represents a constraint that a relationship must have a specific source node.
    *   **Responsibilities:**
        *   Determines if a relationship has a particular source node.
    *   **Attributes:**
        *   ``variable`` (``str``): The name of the variable referring to the source node.
        *   ``relationship_name`` (``str``): The name of the variable referring to the relationship.
    *   **Key Methods:**
        *   ``__init__(self, source_node_name: str, relationship_name: str, **kwargs)``: Initializes the constraint.
            *   **Parameters:**
                * ``source_node_name`` (``str``): The variable referring to the source node.
                *   ``relationship_name`` (``str``): The variable referring to the relationship.
        *   ``__repr__(self) -> str``: Returns a string representation of the constraint.
        *   ``__hash__(self) -> int``: Returns a hash value for the constraint.
        *   ``__eq__(self, other: Any) -> bool``: Checks if this constraint is equal to another object.

6.  ``ConstraintRelationshipHasTargetNode``
    ----------------------------------------

    *   **Purpose:** Represents a constraint that a relationship must have a specific target node.
    *   **Responsibilities:**
        *   Determines if a relationship has a particular target node.
    *   **Attributes:**
        *   ``variable`` (``str``): The variable referring to the target node.
        *   ``relationship_name`` (``str``): The variable referring to the relationship.
    *   **Key Methods:**
        *   ``__init__(self, target_node_name: str, relationship_name: str, **kwargs)``: Initializes the constraint.
            *   **Parameters:**
                * ``target_node_name`` (``str``): The variable referring to the target node.
                *   ``relationship_name`` (``str``): The variable referring to the relationship.
        *   ``__repr__(self) -> str``: Returns a string representation of the constraint.
        *   ``__hash__(self) -> int``: Returns a hash value for the constraint.
        *   ``__eq__(self, other: Any) -> bool``: Checks if this constraint is equal to another object.

7.  ``ConstraintRelationshipHasLabel``
    -----------------------------------

    *   **Purpose:** Represents a constraint that a relationship must have a specific label.
    *   **Responsibilities:**
        *   Determines if a relationship has a particular label.
    *   **Attributes:**
        *   ``relationship_name`` (``str``): The variable referring to the relationship.
        *   ``label`` (``str``): The required label for the relationship.
    *   **Key Methods:**
        *   ``__init__(self, relationship_name: str, label: str, **kwargs)``: Initializes the constraint.
            *   **Parameters:**
                *   ``relationship_name`` (``str``): The relationship name.
                *   ``label`` (``str``): The required label.
        *   ``__repr__(self) -> str``: Returns a string representation of the constraint.
        *   ``__hash__(self) -> int``: Returns a hash value for the constraint.
        *   ``__eq__(self, other: Any) -> bool``: Checks if this constraint is equal to another object.

8.  ``ConstraintNodeHasAttributeWithValue``
    ----------------------------------------

    *   **Purpose:** Represents a constraint that a node must have a specific attribute with a given value.
    *   **Responsibilities:**
        *   Determines if a node has a particular attribute with a certain value.
    *   **Attributes:**
        *   ``variable`` (``str``): The variable referring to the node.
        *   ``attribute`` (``str``): The attribute that must be present.
        *   ``value`` (``Any``): The value that the attribute must have.
    *   **Key Methods:**
        *   ``__init__(self, variable: str, attribute: str, value: Any, **kwargs)``: Initializes the constraint.
            *   **Parameters:**
                *   ``variable`` (``str``): The node variable.
                *   ``attribute`` (``str``): The attribute name.
                *   ``value`` (``Any``): The attribute's required value.
        *   ``__repr__(self) -> str``: Returns a string representation of the constraint.
        *   ``__hash__(self) -> int``: Returns a hash value for the constraint.
        *   ``__eq__(self, other: Any) -> bool``: Checks if this constraint is equal to another object.

Workflow
--------

1.  **Constraint Definition:** Constraint objects are created, specifying the conditions that must be met. These are usually created in the `CypherTrigger` definition.
2. **Constraint Evaluation**: When a new fact is added, the constraint objects are checked against the fact.
3.  **Constraint Satisfaction:** When all the constraints are met, a trigger is activated.

Key Features
------------

*   **Extensibility:** The module is designed to be extended with new constraint types by creating new subclasses of ``Constraint``.
*   **Composability:** Complex conditions can be expressed by combining multiple constraint objects.
*   **Cypher-Like Syntax:** Many of the constraint types align with concepts found in the Cypher query language, making it easier to define and understand constraints in a graph context.
* **Equality Comparison**: Most of the constraint types define `__eq__`, which allows them to be used in sets and dictionaries.
* **Hashing**: Most of the constraint types define `__hash__`, which allows them to be used in sets and dictionaries.

Use Cases
---------

*   **Trigger Conditions:** Defining the conditions that must be met for a trigger to fire.
*   **Data Validation:** Enforcing rules about the data in the graph (e.g., ensuring that certain attributes are present or have specific values).
*   **Pattern Matching:** Defining complex patterns that must exist in the graph.

"""

from __future__ import annotations

from typing import Any, Optional


class Constraint:
    """
    A base class used to represent a Constraint.

    This class currently does not have any attributes or methods.

    Attributes
    ----------
    None

    Methods
    -------
    None
    """

    def __init__(self, trigger: Optional["CypherTrigger"] = None):  # type: ignore
        self.trigger = trigger


class IsTrue(Constraint):
    """
    A constraint that checks if a given predicate is true.

    Attributes:
        predicate (Predicate): The predicate to be evaluated.

    Methods:
        __repr__() -> str: Returns a string representation of the IsTrue instance.
    """

    def __init__(self, predicate: "Predicate", **kwargs):  # type: ignore
        self.predicate = predicate  # type: ignore
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"IsTrue({self.predicate})"  # type: ignore

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, IsTrue) and self.predicate == other.predicate


class ConstraintVariableRefersToSpecificObject(Constraint):
    """
    A constraint that checks if a given predicate refers to a specific object.

    Attributes:
        predicate (Predicate): The predicate to be evaluated.

    Methods:
        __repr__() -> str: Returns a string representation of the IsTrue instance.
    """

    def __init__(self, variable: str, node_id: str, **kwargs):
        self.variable = variable
        self.node_id = node_id
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return f"ConstraintVariableRefersToSpecificObject: {self.variable} -> {self.node_id}"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintVariableRefersToSpecificObject)
            and self.variable == other.variable
            and self.node_id == other.node_id
        )


class ConstraintNodeHasLabel(Constraint):
    """
    A class to represent a constraint that a node must have a specific label.

    Attributes:
    -----------
    node_id : str
        The identifier of the node.
    label : str
        The label that the node must have.

    Methods:
    --------
    __repr__():
        Returns a string representation of the constraint.
    __hash__() -> int:
        Returns a hash value for the constraint.
    __eq__(other: Any) -> bool:
        Checks if this constraint is equal to another constraint.
    """

    def __init__(self, variable: str, label: str, **kwargs):
        self.variable = variable
        self.label = label
        super().__init__(**kwargs)

    def __repr__(self):
        return f"ConstraintNodeHasLabel: {self.variable} {self.label}"

    def __hash__(self) -> int:
        return hash(
            str("HasLabel") + self.variable.__str__() + self.label.__str__()
        )  # type: ignore

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintNodeHasLabel)
            and self.variable == other.variable
            and self.label == other.label
        )


class ConstraintRelationshipHasSourceNode(Constraint):
    """
    A constraint that ensures a relationship has a specific source node.

    Attributes:
        source_node_name (str): The name of the source node.
        relationship_name (str): The name of the relationship.

    Methods:
        __repr__(): Returns a string representation of the constraint.
        __hash__(): Returns a hash value for the constraint.
        __eq__(other: Any): Checks equality between this constraint and another object.
    """

    def __init__(
        self, source_node_name: str, relationship_name: str, **kwargs
    ):
        self.variable = source_node_name
        self.relationship_name = relationship_name
        super().__init__(**kwargs)

    def __repr__(self):
        return f"ConstraintRelationshipHasSourceNode: {self.relationship_name} {self.variable}"

    def __hash__(self) -> int:
        return hash(
            "RelationshipHasSourceNode"
            + self.relationship_name.__str__()
            + self.variable.__str__()
        )

    def __eq__(self, other: Any) -> bool:
        """
        Check if this instance is equal to another instance.

        Args:
            other (Any): The other instance to compare against.

        Returns:
            bool: True if the other instance is of type ConstraintRelationshipHasSourceNode
                  and has the same source_node_name and relationship_name, False otherwise.
        """
        return (
            isinstance(other, ConstraintRelationshipHasSourceNode)
            and self.variable == other.variable
            and self.relationship_name == other.relationship_name
        )


class ConstraintRelationshipHasTargetNode(Constraint):
    """
    A constraint that ensures a relationship has a specific target node.

    Attributes:
        target_node_name (str): The name of the target node.
        relationship_name (str): The name of the relationship.

    Methods:
        __repr__(): Returns a string representation of the constraint.
        __hash__(): Returns a hash value for the constraint.
        __eq__(other: Any): Checks equality between this constraint and another object.
    """

    def __init__(
        self, target_node_name: str, relationship_name: str, **kwargs
    ):
        self.variable = target_node_name
        self.relationship_name = relationship_name
        super().__init__(**kwargs)

    def __repr__(self):
        return f"ConstraintRelationshipHasTargetNode: {self.relationship_name} {self.variable}"

    def __hash__(self) -> int:
        return hash(
            "RelationshipHasTargetNode"
            + self.relationship_name.__str__()
            + self.variable.__str__()
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintRelationshipHasTargetNode)
            and self.variable == other.variable
            and self.relationship_name == other.relationship_name
        )


class ConstraintRelationshipHasLabel(Constraint):
    """
    A constraint that specifies a relationship must have a certain label.

    Attributes:
        relationship_name (str): The name of the relationship.
        label (str): The label that the relationship must have.

    Methods:
        __repr__(): Returns a string representation of the constraint.
        __hash__(): Returns a hash value for the constraint.
        __eq__(other: Any): Checks if this constraint is equal to another constraint.
    """

    def __init__(self, relationship_name: str, label: str, **kwargs):
        self.relationship_name = relationship_name
        self.label = label
        super().__init__(**kwargs)

    def __repr__(self):
        return f"ConstraintRelationshipHasLabel: {self.relationship_name} {self.label}"

    def __hash__(self) -> int:
        return hash(
            "RelationshipHasLabel"
            + self.relationship_name.__str__()
            + self.label.__str__()
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintRelationshipHasLabel)
            and self.relationship_name == other.relationship_name
            and self.label == other.label
        )


class ConstraintNodeHasAttributeWithValue(Constraint):
    """
    A constraint that checks if a node has a specific attribute with a given value.

    Attributes:
        variable (str): The ID of the node.
        attribute (str): The attribute to check.
        value (Any): The value that the attribute should have.

    Methods:
        __repr__(): Returns a string representation of the constraint.
        __hash__(): Returns a hash value for the constraint.
        __eq__(other: Any): Checks if this constraint is equal to another constraint.
    """

    def __init__(self, variable: str, attribute: str, value: Any, **kwargs):
        self.variable = variable
        self.attribute = attribute
        self.value = value
        super().__init__(**kwargs)

    def __repr__(self):
        return f"ConstraintNodeHasAttributeWithValue: [{self.variable}] {self.attribute}: {self.value}"

    def __hash__(self) -> int:
        return hash(
            "HasAttributeWithValue"
            + self.variable
            + self.attribute
            + str(self.value)
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ConstraintNodeHasAttributeWithValue)
            and self.variable == other.variable
            and self.attribute == other.attribute
            and self.value == other.value
        )  # noqa: E501
