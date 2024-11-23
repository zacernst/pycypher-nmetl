from __future__ import annotations

import collections
from abc import abstractmethod
from typing import Generator, Optional, Tuple, Type

from constraint import Domain, Problem

from pycypher.exceptions import CypherParsingError
from pycypher.logger import LOGGER
from pycypher.parser import CypherParser

if __name__ == "__main__":
    from functools import partial

    from pycypher.fact import (
        FactCollection,
        FactNodeHasAttributeWithValue,
        FactNodeHasLabel,
        FactNodeRelatedToNode,
    )
    from pycypher.parser import Literal
    from pycypher.solver import (
        ConstraintNodeHasAttributeWithValue,
        ConstraintNodeHasLabel,
    )

    statements = [
        "MATCH (n:Thing) WHERE n.key = 2, n.foo = 3 RETURN n.foobar, n.baz",
        'MATCH (n:Thing {key1: "value", key2: 5}) WHERE n.key = 2, n.foo = 3 RETURN n.foobar, n.baz',
        'MATCH (n {key1: "value", key2: 5})-[r:MyRelationship]->(m:OtherThing {key3: "hithere"}) WHERE n.key > 2, n.foo = 3 RETURN n.foobar, n.baz',
        'MATCH (n {key1: "value", key2: 5})-[r:MyRelationship]->(m:OtherThing {key3: "hithere"}) WHERE n.key > 2, n.foo = 3 RETURN n.foobar, n.baz AS whatever',
        'MATCH (n {key1: "value", key2: 5})-[r:MyRelationship]->(m:OtherThing {key3: "hithere"})<-[s]-(m), (n)-[r]->(o) WHERE n.key > 2, n.foo = 3 RETURN n.foobar, n.baz AS whatever',
    ]

    for statement in statements:
        result = CypherParser(statement)
        print(statement, result)

    ################################
    ### Build FactCollection
    ################################

    fact1 = FactNodeHasLabel("1", "Thing")
    fact2 = FactNodeHasAttributeWithValue("1", "key", Literal("2"))
    fact3 = FactNodeRelatedToNode("1", "2", "MyRelationship")
    fact4 = FactNodeHasLabel("2", "OtherThing")
    fact5 = FactNodeHasAttributeWithValue("2", "key", Literal(5))

    fact_collection = FactCollection([fact1, fact2, fact3, fact4, fact5])

    ###########################################
    ### Define Cypher Query
    ###########################################

    cypher_statement = """MATCH (n:Thing {key: 2}) RETURN n.key"""

    ###########################################
    ### Parse Cypher Query
    ###########################################

    result = CypherParser(cypher_statement)

    ###########################################
    ### Gather constraints
    ###########################################
    constraints = result.parsed.aggregated_constraints

    # Get all the labels
    node_labels = set()
    for constraint in constraints:
        if isinstance(constraint, ConstraintNodeHasLabel):
            node_labels.add(constraint.label)
    # Get list of all nodes in constraints
    node_variables = set()
    for constraint in constraints:
        if isinstance(constraint, ConstraintNodeHasLabel):
            node_variables.add(constraint.node_id)
        elif isinstance(constraint, ConstraintNodeHasAttributeWithValue):
            node_variables.add(constraint.node_id)
        elif isinstance(constraint, FactNodeRelatedToNode):
            node_variables.add(constraint.node1_id)
            node_variables.add(constraint.node2_id)
    # Get list of all relationships in constraints
    relationship_labels = set()
    for constraint in constraints:
        if isinstance(
            constraint, FactNodeRelatedToNode
        ):  # This is borked; relationship not in constraints
            relationship_labels.add(constraint.relationship_label)
    # Get list of all attributes in constraints
    attributes = set()
    for constraint in constraints:
        if isinstance(constraint, ConstraintNodeHasAttributeWithValue):
            attributes.add(constraint.attribute)

    #############################
    ### Facts
    #############################

    node_label_domain = Domain(set())
    node_domain = Domain(set())
    # relationship_label_domain = Domain(set())
    # attribute_domain = Domain(set())

    label_domain_dict = collections.defaultdict(set)

    for fact in fact_collection:
        if isinstance(fact, FactNodeHasLabel):
            if fact.node_id not in node_domain:
                node_domain.append(fact.node_id)
            if fact.label not in node_label_domain:
                label_domain_dict[fact.label].add(fact.node_id)
        else:
            pass

    # I think we have to reify relationships. Ugh.
    ################################################
    ### Define the Problem()
    ################################################

    problem = Problem()

    for node_id in node_variables:
        problem.addVariable(node_id, node_domain)
    # for relationship_label in relationship_labels:
    #     problem.addVariable(relationship_labels, relationship_label_domain)
    # for attribute in attributes:
    #     problem.addVariable(attribute, attribute_domain)
    # for label in node_labels:
    #     problem.addVariable(label, node_label_domain)

    def node_has_label(node_id=None, label=None):
        LOGGER.debug(f"Checking if {node_id} has label {label}")
        answer = FactNodeHasLabel(node_id, label) in fact_collection.facts
        LOGGER.debug(f"Answer: {answer}")
        return answer

    # Turn these into partial functions with `node_id` the remaining argument
    def node_has_attribute_with_value(
        node_id=None, attribute=None, value=None
    ):
        if not isinstance(value, Literal):
            value = Literal(value)
        LOGGER.debug(
            f"Checking if {node_id} has attribute {attribute} with value {value}"
        )
        obj = FactNodeHasAttributeWithValue(
            node_id=node_id, attribute=attribute, value=value
        )
        answer = obj in fact_collection.facts
        LOGGER.debug(f"Answer: {answer}")
        return answer

    # attempt = partial(node_has_label, ('n', 'Thing',))

    # Loop over constraints, creating partial functions and adding them as constraints
    for constraint in constraints:
        if isinstance(constraint, ConstraintNodeHasLabel):
            problem.addConstraint(
                partial(node_has_label, label=constraint.label),
                [constraint.node_id],
            )
        if isinstance(
            constraint, ConstraintNodeHasAttributeWithValue
        ):  # This doesn't work
            problem.addConstraint(
                partial(
                    node_has_attribute_with_value,
                    attribute=constraint.attribute,
                    value=constraint.value,
                ),
                [constraint.node_id],
            )
