from __future__ import annotations

from functools import partial

import rich
import collections
from typing import Any, Dict, List, Tuple, Type

import ply.yacc as yacc  # type: ignore
from constraint import Constraint, Domain, Problem
from pycypher.cypher_lexer import *

# from pycypher.solver import Constraint
from pycypher.exceptions import CypherParsingError
from pycypher.fact import (
    FactCollection,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactNodeRelatedToNode,
)
from pycypher.logger import LOGGER
from pycypher.node_classes import (
    Alias,
    And,
    Cypher,
    Equals,
    GreaterThan,
    LessThan,
    Literal,
    Mapping,
    MappingSet,
    Match,
    Node,
    NodeNameLabel,
    ObjectAttributeLookup,
    Projection,
    Query,
    Relationship,
    RelationshipChain,
    RelationshipChainList,
    RelationshipLeftRight,
    RelationshipRightLeft,
    Return,
    Where,
)
from pycypher.solver import (
    ConstraintNodeHasAttributeWithValue,
    ConstraintNodeHasLabel,
)
from pycypher.tree_mixin import TreeMixin

start = "cypher"


def p_string(p: yacc.YaccProduction):
    """string : DQUOTE WORD DQUOTE"""


def p_cypher(p: List[TreeMixin]):
    """cypher : query"""
    if len(p) == 2:
        p[0] = Cypher(p[1])
    else:
        raise Exception(
            "Parser only accepts one query, and no update clauses (for now)."
        )


def p_query(p: Tuple[yacc.YaccProduction, Match, Return]):
    """query : match_pattern return"""
    p[0] = Query(p[1], p[2])


def p_name_label(
    p: Tuple[yacc.YaccProduction, str] | Tuple[yacc.YaccProduction, str, str],
):
    """name_label : WORD
    | WORD COLON WORD"""
    p[0] = NodeNameLabel(p[1], p[3] if len(p) == 4 else None)


def p_mapping_list(p: List[TreeMixin]):
    """mapping_list : WORD COLON literal
    | mapping_list COMMA WORD COLON literal
    """
    if len(p) == 4:
        p[0] = MappingSet([Mapping(p[1], p[3])])
    elif len(p) == 6:
        p[0] = p[1]
        p[0].mappings.append(Mapping(p[3], p[5]))
    else:
        raise Exception("What?")


def p_node(p: yacc.YaccProduction):
    """node : LPAREN name_label RPAREN
    | LPAREN name_label LCURLY mapping_list RCURLY RPAREN
    """
    p[0] = Node(p[2])
    if len(p) == 7:
        p[0].mapping_list = p[4]


def p_alias(p: yacc.YaccProduction):
    """alias : WORD AS WORD
    | object_attribute_lookup AS WORD"""
    p[0] = Alias(p[1], p[3])


def p_literal(p: yacc.YaccProduction):
    """literal : INTEGER
    | FLOAT
    | DQUOTE WORD DQUOTE
    """
    p[0] = Literal(p[1]) if len(p) == 2 else Literal(p[2])


def p_relationship(p: yacc.YaccProduction):
    """relationship : LSQUARE WORD RSQUARE
    | LSQUARE name_label RSQUARE"""
    if isinstance(p[2], NodeNameLabel):
        p[0] = Relationship(p[2])
    else:
        p[0] = Relationship(NodeNameLabel(p[2]))


def p_left_right(p: yacc.YaccProduction):
    """left_right : DASH relationship DASH GREATERTHAN"""
    p[0] = RelationshipLeftRight(p[2])


def p_right_left(p: yacc.YaccProduction):
    """right_left : LESSTHAN DASH relationship DASH"""
    p[0] = RelationshipRightLeft(p[3])


def p_incomplete_relationship_chain(p: yacc.YaccProduction):
    """incomplete_relationship_chain : node left_right
    | node right_left
    | incomplete_relationship_chain node left_right
    | incomplete_relationship_chain node right_left
    """
    relationship_chain = RelationshipChain([])
    p[0] = relationship_chain
    if len(p) == 3:
        relationship_chain.steps = [p[1], p[2]]
    elif len(p) == 4:
        relationship_chain.steps = p[1].steps + [p[2], p[3]]
    else:
        pass


def p_relationship_chain(p: yacc.YaccProduction):
    """relationship_chain : incomplete_relationship_chain node"""
    p[0] = RelationshipChain(p[1].steps + [p[2]])


def p_relationship_chain_list(p: yacc.YaccProduction):
    """relationship_chain_list : relationship_chain
    | relationship_chain_list COMMA relationship_chain"""
    if len(p) == 2:
        p[0] = RelationshipChainList([p[1]])
    else:
        p[0] = p[1]
        p[0].relationships.append(p[3])


def p_match_pattern(p: yacc.YaccProduction):
    """match_pattern : MATCH node
    | MATCH relationship_chain_list
    | MATCH relationship_chain_list where
    | MATCH node where
    """
    if len(p) == 3:
        p[0] = Match(p[2])
    elif len(p) == 4:
        p[0] = Match(p[2], p[3])


def p_binary_operator(p: Tuple[yacc.YaccProduction, str]):
    """binary_operator : EQUALS
    | LESSTHAN
    | GREATERTHAN"""
    p[0] = p[1]


def p_predicate(p: yacc.YaccProduction):
    """predicate : object_attribute_lookup binary_operator literal
    | object_attribute_lookup binary_operator object_attribute_lookup"""
    predicate_dispatch_dict: Dict[str, Type[TreeMixin]] = {
        "=": Equals,
        "<": LessThan,
        ">": GreaterThan,
    }
    p[0] = predicate_dispatch_dict[p[2]](p[1], p[2], p[3])


def p_object_attribute_lookup(p: yacc.YaccProduction):
    """object_attribute_lookup : WORD DOT WORD"""
    p[0] = ObjectAttributeLookup(p[1], p[3])


def p_where(p: yacc.YaccProduction):
    """where : WHERE predicate
    | where COMMA predicate"""
    if len(p) == 3:
        p[0] = Where(p[2])
    else:
        p[0] = Where(And(p[1].predicate, p[3]))


def p_projection(p: yacc.YaccProduction):
    """projection : object_attribute_lookup
    | alias
    | projection COMMA alias
    | projection COMMA object_attribute_lookup"""
    if len(p) == 2:
        p[0] = Projection([p[1]])
    else:
        p[0] = p[1]
        p[0].lookups.append(p[3])


def p_return(p: yacc.YaccProduction):
    """return : RETURN projection"""
    p[0] = Return(p[2])


CYPHER: yacc.LRParser = yacc.yacc()


class CypherParser:
    def __init__(self, cypher_text: str):
        self.cypher_text = cypher_text
        self.parsed: TreeMixin | None = CYPHER.parse(self.cypher_text)
        if not self.parsed:
            raise CypherParsingError(f"Error parsing {self.cypher_text}")
        [_ for _ in self.parsed.walk()]
        self.parsed.gather_constraints()

    def __repr__(self) -> str:
        return self.parsed.__str__()
        
    @property
    def node_labels(self) -> set[str]: 
        # Get all the labels
        node_labels = set()
        for constraint in self.parsed.aggregated_constraints:
            if isinstance(constraint, ConstraintNodeHasLabel):
                node_labels.add(constraint.label)
        return node_labels
        
    @property
    def node_variables(self) -> set[str]: 
        node_variables = set()
        for constraint in self.parsed.aggregated_constraints:
            if isinstance(constraint, ConstraintNodeHasLabel):
                node_variables.add(constraint.node_id)
            elif isinstance(constraint, ConstraintNodeHasAttributeWithValue):
                node_variables.add(constraint.node_id)
            elif isinstance(constraint, FactNodeRelatedToNode):
                node_variables.add(constraint.node1_id)
                node_variables.add(constraint.node2_id)
        return node_variables
    
    @property
    def relationship_labels(self) -> set[str]:
        relationship_labels = set()
        for constraint in self.parsed.aggregated_constraints:
            if isinstance(
                constraint, FactNodeRelatedToNode
            ):  # This is borked; relationship not in constraints
                relationship_labels.add(constraint.relationship_label)
        return relationship_labels
    
    @property
    def attributes(self) -> set[str]:
        attributes = set()
        for constraint in self.parsed.aggregated_constraints:
            if isinstance(constraint, ConstraintNodeHasAttributeWithValue):
                attributes.add(constraint.attribute)
        return attributes
    
    def solutions(self, fact_collection: FactCollection) -> List[Dict[str, Any]]:
        
        def _node_has_label(node_id=None, label=None):
            LOGGER.debug(f"Checking if {node_id} has label {label}")
            answer = FactNodeHasLabel(node_id, label) in fact_collection.facts
            LOGGER.debug(f"Answer: {answer}")
            return answer

        # Turn these into partial functions with `node_id` the remaining argument
        def _node_has_attribute_with_value(
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

        def _set_up_problem(constraints: List[Constraint]) -> Dict:
            problem = Problem()
            node_domain = Domain(set())
            node_label_domain = Domain(set())
            label_domain_dict = collections.defaultdict(set)

            for fact in fact_collection:
                if isinstance(fact, FactNodeHasLabel):
                    if fact.node_id not in node_domain:
                        node_domain.append(fact.node_id)
                    if fact.label not in node_label_domain:
                        label_domain_dict[fact.label].add(fact.node_id)

            for node_id in self.node_variables:
                problem.addVariable(node_id, node_domain)
            
            # Loop over constraints, creating partial functions and adding them as constraints
            for constraint in constraints:
                if isinstance(constraint, ConstraintNodeHasLabel):
                    problem.addConstraint(
                        partial(_node_has_label, label=constraint.label),
                        [constraint.node_id],
                    )
                if isinstance(
                    constraint, ConstraintNodeHasAttributeWithValue
                ):  # This doesn't work
                    problem.addConstraint(
                        partial(
                            _node_has_attribute_with_value,
                            attribute=constraint.attribute,
                            value=constraint.value,
                        ),
                        [constraint.node_id],
                    )
            return problem

        problem = _set_up_problem(self.parsed.aggregated_constraints)
        solutions = problem.getSolutions()
        return solutions


if __name__ == "__main__":

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

    parsed = CypherParser(cypher_statement)
    instances = parsed.solutions(fact_collection)
    rich.print(instances)