"""Main parser module."""

from __future__ import annotations

from functools import partial
from typing import Any, Dict, List, Tuple, Type

import ply.yacc as yacc  # type: ignore
from constraint import Domain, Problem

from pycypher.cypher_lexer import *  # pylint: disable=wildcard-import,unused-wildcard-import
from pycypher.fact import (
    FactCollection,
    FactNodeHasAttributeWithValue,
    FactNodeHasLabel,
    FactRelationshipHasLabel,
    FactRelationshipHasSourceNode,
    FactRelationshipHasTargetNode,
)
from pycypher.logger import LOGGER
from pycypher.node_classes import Addition  # pylint: disable=unused-import
from pycypher.node_classes import (
    Aggregation,
    Alias,
    AliasedName,
    And,
    Collect,
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
    ObjectAsSeries,
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
    WithClause,
)
from pycypher.shims import Shim
from pycypher.solver import (
    ConstraintNodeHasAttributeWithValue,
    ConstraintNodeHasLabel,
    ConstraintRelationshipHasLabel,
    ConstraintRelationshipHasSourceNode,
    ConstraintRelationshipHasTargetNode,
)
from pycypher.tree_mixin import TreeMixin

start = "cypher"


def p_cypher(p: List[TreeMixin]):
    """cypher : query"""
    if len(p) == 2:
        p[0] = Cypher(p[1])
    else:
        raise NotImplementedError(
            "Parser only accepts one query, and no update clauses (for now)."
        )


def p_query(p: Tuple[yacc.YaccProduction, Match, Return]):
    """query : match_pattern return"""
    p[0] = Query(p[1], p[2])


def p_string(p: yacc.YaccProduction):
    """string : STRING"""
    p[0] = p[1]


def p_integer(p: yacc.YaccProduction):
    """integer : INTEGER"""
    p[0] = int(p[1])


def p_float(p: yacc.YaccProduction):
    """float : FLOAT"""
    p[0] = float(p[1])


def p_name_label(
    p: Tuple[yacc.YaccProduction, str] | Tuple[yacc.YaccProduction, str, str],
):
    """name_label : WORD
    | WORD COLON WORD
    | COLON WORD"""
    if len(p) == 2:
        p[0] = NodeNameLabel(p[1], None)
    elif len(p) == 4:
        p[0] = NodeNameLabel(p[1], p[3])
    elif len(p) == 3:
        p[0] = NodeNameLabel(None, p[2])
    else:
        raise ValueError("What?")


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
        raise ValueError("What?")


def p_node(p: yacc.YaccProduction):
    """node : LPAREN name_label RPAREN
    | LPAREN name_label LCURLY mapping_list RCURLY RPAREN
    | LPAREN RPAREN
    | LPAREN WORD RPAREN
    """
    if len(p) == 4 and isinstance(p[2], NodeNameLabel):
        node_name_label = p[2]
        mapping_list = MappingSet([])
    elif len(p) == 3:
        node_name_label = NodeNameLabel(None, None)
        mapping_list = MappingSet([])
    elif len(p) == 4:
        node_name_label = NodeNameLabel(p[2], None)
        mapping_list = MappingSet([])
    elif len(p) == 7 and isinstance(p[2], NodeNameLabel):
        node_name_label: NodeNameLabel = p[2]
        mapping_list = p[4]
    else:
        raise ValueError("What?")
    p[0] = Node(node_name_label, mapping_list)


def p_alias(p: yacc.YaccProduction):
    """alias : WORD AS WORD
    | object_attribute_lookup AS WORD
    | aggregation AS WORD"""
    p[0] = Alias(p[1], p[3])


def p_literal(p: yacc.YaccProduction):
    """literal : integer
    | float
    | STRING
    """
    p[0] = Literal(p[1])


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


# This might be redundant with alias
# def p_obj_as(p: yacc.YaccProduction):
#     """obj_as : object_attribute_lookup AS WORD
#     | WORD AS WORD"""
#     if isinstance(p[1], ObjectAttributeLookup):
#         p[0] = ObjectAs(p[1], p[3])
#     else:
#         import pdb; pdb.set_trace()
#         p[0] = ObjectAs(ObjectAttributeLookup(p[1], None), p[3])


def p_with_as_series(p: yacc.YaccProduction):
    """with_as_series : alias
    | with_as_series COMMA alias"""
    if len(p) == 2:
        p[0] = ObjectAsSeries([p[1]])
    else:
        object_attribute_lookup_list = p[1].object_attribute_lookup_list
        object_attribute_lookup_list.append(p[3])
        p[0] = ObjectAsSeries(object_attribute_lookup_list)


def p_collect(p: yacc.YaccProduction):
    """collect : COLLECT LPAREN object_attribute_lookup RPAREN"""
    p[0] = Collect(object_attribute_lookup=p[3])


def p_aggregation(p: yacc.YaccProduction):
    """aggregation : collect"""
    p[0] = Aggregation(aggregation=p[1])


def p_with_clause(p: yacc.YaccProduction):
    """with_clause : WITH with_as_series"""
    p[0] = WithClause(p[2])


def p_match_pattern(p: yacc.YaccProduction):
    """match_pattern : MATCH node
    | MATCH relationship_chain_list
    | MATCH relationship_chain_list with_clause
    | MATCH relationship_chain_list where
    | MATCH relationship_chain_list with_clause where
    | MATCH node where
    | MATCH node with_clause where
    """
    if len(p) == 3:
        p[0] = Match(p[2])
    elif len(p) == 4 and isinstance(p[3], WithClause):
        p[0] = Match(p[2], None, p[3])
    elif len(p) == 5:
        p[0] = Match(p[2], p[4], p[3])
    else:
        p[0] = Match(p[2], p[3], None)


def p_binary_operator(p: Tuple[yacc.YaccProduction, str]):
    """binary_operator : EQUALS
    | LESSTHAN
    | GREATERTHAN
    | OR
    | AND"""
    p[0] = p[1]


def p_binary_function(p: Tuple[yacc.YaccProduction, str]):
    """binary_function : ADDITION"""
    p[0] = p[1]


def p_aliased_name(p: yacc.YaccProduction):
    """aliased_name : WORD"""
    p[0] = AliasedName(p[1])


def p_predicate(p: yacc.YaccProduction):
    """predicate : object_attribute_lookup binary_operator literal
    | object_attribute_lookup binary_operator object_attribute_lookup
    | aliased_name binary_operator literal
    | object_attribute_lookup binary_operator binary_expression"""
    predicate_dispatch_dict: Dict[str, Type[TreeMixin]] = {
        "=": Equals,
        "<": LessThan,
        ">": GreaterThan,
    }
    p[0] = predicate_dispatch_dict[p[2]](p[1], p[3])


def p_binary_expression(p: yacc.YaccProduction):
    """binary_expression : object_attribute_lookup binary_function literal
    | object_attribute_lookup binary_function object_attribute_lookup
    | aliased_name binary_function literal
    | literal binary_function literal"""
    function_dispatch_dict: Dict[str, Type[TreeMixin]] = {
        "+": Addition,
    }
    p[0] = function_dispatch_dict[p[2]](p[1], p[3])


def p_object_attribute_lookup(p: yacc.YaccProduction):
    """object_attribute_lookup : WORD DOT WORD
    | WORD"""
    if len(p) == 4:
        p[0] = ObjectAttributeLookup(p[1], p[3])
    else:
        p[0] = ObjectAttributeLookup(p[1], None)


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


CYPHER: yacc.LRParser = yacc.yacc()  # type: ignore


class CypherParser:
    """The main class of the ``pycypher`` package.

    This class is responsible for parsing Cypher queries and
    returning the solutions to those queries. It creates the
    AST for the Cypher query and then generates a constraint
    satisfaction problem which solves the query.
    """

    def __init__(self, cypher_text: str):
        self.cypher_text = cypher_text
        self.parsed: TreeMixin = CYPHER.parse(self.cypher_text)
        [_ for _ in self.parsed.walk()]  # pylint: disable=expression-not-assigned
        # self.parsed.gather_constraints()
        self.parsed.trigger_gather_constraints_to_match()

    def __repr__(self) -> str:
        return self.parsed.__str__()

    # @property
    # def node_labels(self) -> set[str]:
    #     # Get all the labels
    #     node_labels = set()
    #     for constraint in self.parsed.aggregated_constraints:
    #         if isinstance(constraint, ConstraintNodeHasLabel):
    #             node_labels.add(constraint.label)
    #     return node_labels

    # @property
    # def node_variables(self) -> set[str]:
    #     node_variables = set()
    #     for constraint in self.parsed.aggregated_constraints:
    #         if isinstance(constraint, ConstraintNodeHasLabel):
    #             node_variables.add(constraint.node_id)
    #         elif isinstance(constraint, ConstraintNodeHasAttributeWithValue):
    #             node_variables.add(constraint.node_id)
    #         elif 0 and isinstance(constraint, FactNodeRelatedToNode):
    #             node_variables.add(constraint.node1_id)
    #             node_variables.add(constraint.node2_id)
    #     return node_variables

    # @property
    # def relationship_variables(self) -> set[str]:
    #     relationship_variables = set()
    #     for constraint in self.parsed.aggregated_constraints:
    #         if isinstance(
    #             constraint,
    #             (
    #                 ConstraintRelationshipHasSourceNode,
    #                 ConstraintRelationshipHasLabel,
    #             ),
    #         ):
    #             relationship_variables.add(constraint.relationship_name)
    #     return relationship_variables

    # @property
    # def relationship_labels(self) -> set[str]:
    #     relationship_labels = set()
    #     for constraint in self.parsed.aggregated_constraints:
    #         if isinstance(
    #             constraint, FactNodeRelatedToNode
    #         ):  # This is borked; relationship not in constraints
    #             relationship_labels.add(constraint.relationship_label)
    #     return relationship_labels

    # @property
    # def attributes(self) -> set[str]:
    #     attributes = set()
    #     for constraint in self.parsed.aggregated_constraints:
    #         if isinstance(constraint, ConstraintNodeHasAttributeWithValue):
    #             attributes.add(constraint.attribute)
    #     return attributes

    # def solutions(
    #     self, fact_collection: FactCollection | Shim
    # ) -> List[Dict[str, Any]]:
    #     """Almost. But we can't do all the constraints at once. Have to start with
    #     the Match clause, then the With clause to rename etc., and then the Where clause.
    #     """

    #     def _set_up_problem(parsed_cypher) -> Problem:
    #         constraints = parsed_cypher.aggregated_constraints
    #         problem = Problem()
    #         node_domain = Domain(set())
    #         relationship_domain = Domain(set())

    #         # Get domains for nodes and relationships
    #         for fact in fact_collection:
    #             if isinstance(fact, FactNodeHasLabel):
    #                 if fact.node_id not in node_domain:
    #                     LOGGER.debug("fact.node_id: %s", fact.node_id)
    #                     node_domain.append(fact.node_id)
    #             elif isinstance(fact, FactRelationshipHasLabel):
    #                 if fact.relationship_id not in relationship_domain:
    #                     relationship_domain.append(fact.relationship_id)
    #             elif isinstance(
    #                 fact,
    #                 (
    #                     FactRelationshipHasSourceNode,
    #                     FactRelationshipHasTargetNode,
    #                 ),
    #             ):
    #                 if fact.relationship_id not in relationship_domain:
    #                     relationship_domain.append(fact.relationship_id)
    #             else:
    #                 pass

    #         # Assign variables to domains
    #         for constraint in constraints:
    #             if isinstance(constraint, ConstraintNodeHasLabel):
    #                 problem.addVariable(constraint.node_id, node_domain)
    #             elif 0 and isinstance(
    #                 constraint, ConstraintNodeHasAttributeWithValue
    #             ):
    #                 problem.addVariable(constraint.node_id, node_domain)
    #             elif (  # pylint: disable=protected-access
    #                 isinstance(constraint, ConstraintRelationshipHasSourceNode)
    #                 and constraint.relationship_name not in problem._variables
    #             ):
    #                 problem.addVariable(
    #                     constraint.relationship_name, relationship_domain
    #                 )
    #             elif (  # pylint: disable=protected-access
    #                 isinstance(constraint, ConstraintRelationshipHasSourceNode)
    #                 and constraint.relationship_name not in problem._variables
    #             ):
    #                 problem.addVariable(
    #                     constraint.relationship_name, relationship_domain
    #                 )
    #             elif (  # pylint: disable=protected-access
    #                 isinstance(constraint, ConstraintRelationshipHasTargetNode)
    #                 and constraint.relationship_name not in problem._variables
    #             ):
    #                 problem.addVariable(
    #                     constraint.relationship_name, relationship_domain
    #                 )
    #             elif (  # pylint: disable=protected-access
    #                 isinstance(constraint, ConstraintRelationshipHasLabel)
    #                 and constraint.relationship_name not in problem._variables
    #             ):
    #                 problem.addVariable(
    #                     constraint.relationship_name, relationship_domain
    #                 )
    #             elif (  # pylint: disable=protected-access
    #                 isinstance(constraint, ConstraintNodeHasAttributeWithValue)
    #                 and constraint.node_id not in problem._variables
    #             ):
    #                 problem.addVariable(constraint.node_id, node_domain)
    #             else:
    #                 pass

    #         # Add constraints to problem definition
    #         def _f(x, y):
    #             answer = (
    #                 FactRelationshipHasSourceNode(
    #                     relationship_id=x, source_node_id=y
    #                 )
    #                 in fact_collection
    #             )
    #             LOGGER.debug("answer _f: %s for x: %s, y: %s", answer, x, y)
    #             return answer

    #         def _g(node_id, node_label=None):
    #             answer = (
    #                 FactNodeHasLabel(node_id=node_id, node_label=node_label)
    #                 in fact_collection
    #             )
    #             LOGGER.debug(
    #                 "answer _g: %s for node_id: %s, node_label: %s",
    #                 answer,
    #                 node_id,
    #                 node_label,
    #             )
    #             return answer

    #         def _h(relationship_id, relationship_label=None):
    #             answer = (
    #                 FactRelationshipHasLabel(
    #                     relationship_id=relationship_id,
    #                     relationship_label=relationship_label,
    #                 )
    #                 in fact_collection
    #             )
    #             LOGGER.debug(
    #                 "answer _h: %s for relationship_id: %s, relationship_label: %s",
    #                 answer,
    #                 relationship_id,
    #                 relationship_label,
    #             )
    #             return answer

    #         def _i(node_id, attribute=None, value=None):
    #             answer = (
    #                 FactNodeHasAttributeWithValue(
    #                     node_id=node_id, attribute=attribute, value=value
    #                 )
    #                 in fact_collection
    #             )
    #             LOGGER.debug(
    #                 "answer _i: %s for node_id: %s, attribute: %s, value: %s",
    #                 answer,
    #                 node_id,
    #                 attribute,
    #                 value,
    #             )
    #             return answer

    #         for constraint in constraints:
    #             if isinstance(constraint, ConstraintNodeHasLabel):
    #                 LOGGER.debug("Adding constraint: %s", constraint)
    #                 problem.addConstraint(
    #                     partial(_g, node_label=constraint.label),
    #                     [
    #                         constraint.node_id,
    #                     ],
    #                 )
    #             elif isinstance(
    #                 constraint, ConstraintNodeHasAttributeWithValue
    #             ):
    #                 LOGGER.debug("Adding constraint: %s", constraint)
    #                 problem.addConstraint(
    #                     partial(
    #                         _i,
    #                         attribute=constraint.attribute,
    #                         value=constraint.value,
    #                     ),
    #                     [
    #                         constraint.node_id,
    #                     ],
    #                 )
    #             elif isinstance(constraint, ConstraintRelationshipHasLabel):
    #                 LOGGER.debug("Adding constraint: %s", constraint)
    #                 problem.addConstraint(
    #                     partial(_h, relationship_label=constraint.label),
    #                     [
    #                         constraint.relationship_name,
    #                     ],
    #                 )
    #             elif isinstance(
    #                 constraint, ConstraintRelationshipHasSourceNode
    #             ):
    #                 LOGGER.debug("Adding constraint: %s", constraint)
    #                 problem.addConstraint(
    #                     _f,
    #                     [
    #                         constraint.relationship_name,
    #                         constraint.source_node_name,
    #                     ],
    #                 )
    #             elif isinstance(
    #                 constraint, ConstraintRelationshipHasTargetNode
    #             ):
    #                 LOGGER.debug("Adding constraint: %s", constraint)
    #                 problem.addConstraint(
    #                     lambda x, y: FactRelationshipHasTargetNode(
    #                         relationship_id=x, target_node_id=y
    #                     )
    #                     in fact_collection,
    #                     [
    #                         constraint.relationship_name,
    #                         constraint.target_node_name,
    #                     ],
    #                 )
    #             else:
    #                 pass  # Add more constraints if necessary
    #         return problem

    #     fact_collection = (
    #         fact_collection
    #         if isinstance(fact_collection, FactCollection)
    #         else fact_collection.make_fact_collection()
    #     )
    #     problem = _set_up_problem(self.parsed)
    #     solutions = problem.getSolutions()
    #     return solutions


if __name__ == "__main__":
    pass
