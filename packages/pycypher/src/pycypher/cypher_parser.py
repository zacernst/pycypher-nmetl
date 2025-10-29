"""Main parser module."""

# type: ignore
from __future__ import annotations

from typing import Any, Dict, List, Tuple, Type

from ply import yacc  # type: ignore
from pycypher.cypher_lexer import *  # noqa: F403
from pycypher.fact_collection import FactCollection
from pycypher.node_classes import Alias  # Aggregation,
from pycypher.node_classes import (
    Addition,
    AliasedName,
    And,
    Collect,
    Collection,
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
    Not,
    ObjectAsSeries,
    ObjectAttributeLookup,
    Or,
    Query,
    Relationship,
    RelationshipChain,
    RelationshipChainList,
    RelationshipLeftRight,
    RelationshipRightLeft,
    Return,
    ReturnProjection,
    Size,
    Where,
    WithClause,
)
from pycypher.solutions import ProjectionList
from pycypher.tree_mixin import TreeMixin
from rich.tree import Tree

START_SYMBOL = "cypher"  # pylint: disable=invalid-name


def p_cypher(p: List[TreeMixin]):
    """
    cypher : query
    """
    if len(p) == 2:
        p[0] = Cypher(p[1])
    else:
        raise NotImplementedError(
            "Parser only accepts one query, and no update clauses (for now)."
        )


# | relationship_chain_list  Taken from p_query docstring below
def p_query(p: Tuple[yacc.YaccProduction, Match, Return]):
    """
    query : match_pattern return
    """
    if len(p) == 3:
        p[0] = Query(p[1], p[2])
    else:
        p[0] = p[1]
        assert False, "Shouldn't get here"


def p_string(p: yacc.YaccProduction):
    """
    string : STRING
    """
    p[0] = p[1]


def p_integer(p: yacc.YaccProduction):
    """
    integer : INTEGER
    """
    p[0] = int(p[1])


def p_float(p: yacc.YaccProduction):
    """
    float : FLOAT
    """
    p[0] = float(p[1])


def p_name_label(
    p: Tuple[yacc.YaccProduction, str] | Tuple[yacc.YaccProduction, str, str],
):
    """
    name_label : WORD
                | WORD COLON WORD
                | COLON WORD
    """
    if len(p) == 2:
        p[0] = NodeNameLabel(p[1], None)
    elif len(p) == 4:
        p[0] = NodeNameLabel(p[1], p[3])
    elif len(p) == 3:
        p[0] = NodeNameLabel(None, p[2])
    else:
        raise ValueError("What?")


def p_mapping_list(p: List[TreeMixin]):
    """
    mapping_list : WORD COLON literal
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
    """
    node : LPAREN name_label RPAREN
        | LPAREN name_label LCURLY mapping_list RCURLY RPAREN
        | LPAREN RPAREN
        | LPAREN WORD RPAREN
    """
    if len(p) == 4 and isinstance(p[2], NodeNameLabel):
        name_label = p[2]
        mapping_list = MappingSet([])
    elif len(p) == 3:
        name_label = NodeNameLabel(None, None)
        mapping_list = MappingSet([])
    elif len(p) == 4:
        name_label = NodeNameLabel(p[2], None)
        mapping_list: MappingSet = MappingSet([])
    elif len(p) == 7 and isinstance(p[2], NodeNameLabel):
        name_label: NodeNameLabel = p[2]
        mapping_list = p[4]
    else:
        raise ValueError("What?")
    p[0] = Node(name_label, mapping_list)


def p_alias(p: yacc.YaccProduction):
    """
    alias : WORD AS WORD
    | collect AS WORD
    | object_attribute_lookup AS WORD
    | size AS WORD"""
    p[0] = Alias(p[1], p[3])


def p_literal(p: yacc.YaccProduction):
    """literal : integer
    | float
    | STRING
    """
    p[0] = Literal(p[1])


def p_collection(p: yacc.YaccProduction):
    """list : LSQUARE incomplete_list RSQUARE
    | LSQUARE RSQUARE
    """
    if len(p) == 4:
        p[0] = Collection(values=[p[2]])
    elif len(p) == 3:
        p[0] = Collection(values=[])
    else:
        raise ValueError("What?")


def p_incomplete_collection(p: yacc.YaccProduction):
    """incomplete_list : literal
    | incomplete_list COMMA literal
    """
    if len(p) == 2:
        p[0] = [p[1]]
    elif len(p) == 4:
        p[0] = p[1]
        p[0].append(p[3])
    else:
        raise ValueError("What?")


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
    if len(p) == 3:
        p[0] = [p[1], p[2]]
    elif len(p) == 4:
        p[0] = p[1] + [p[2]] + [p[3]]
    else:
        raise ValueError("This should never happen.")


def p_relationship_chain(p: yacc.YaccProduction):
    """relationship_chain : incomplete_relationship_chain node
    | node"""
    if len(p) == 2:
        p[0] = RelationshipChain(
            source_node=p[1], relationship=None, target_node=None
        )
    elif len(p) == 3:
        p[0] = RelationshipChain(
            source_node=p[1][0], relationship=p[1][1], target_node=p[2]
        )
    else:
        raise ValueError("What?")


def p_relationship_chain_list(p: yacc.YaccProduction):
    """relationship_chain_list : relationship_chain
    | relationship_chain_list COMMA relationship_chain"""
    if len(p) == 2:
        p[0] = RelationshipChainList([p[1]])
    else:
        p[0] = p[1]
        p[0].relationships.append(p[3])


def p_with_as_series(p: yacc.YaccProduction):
    """
    with_as_series : alias
    | with_as_series COMMA alias
    """
    if len(p) == 2:
        p[0] = ObjectAsSeries([p[1]])
    else:
        lookups = p[1].lookups
        lookups.append(p[3])
        p[0] = ObjectAsSeries(lookups)


def p_collect(p: yacc.YaccProduction):
    """
    collect : COLLECT LPAREN object_attribute_lookup RPAREN
    """
    if isinstance(p[3], ObjectAttributeLookup):
        p[0] = Collect(object_attribute_lookup=p[3])
    else:
        raise ValueError(
            "We're assuming for now that the collect is on an object attribute lookup."
        )


def p_size(p: yacc.YaccProduction) -> None:
    """
    size : SIZE LPAREN collect RPAREN
    | SIZE LPAREN WORD RPAREN
    """
    p[0] = Size(collect=p[3])


# def p_aggregation(p: yacc.YaccProduction):
#     """
#     aggregation : collect
#     | DISTINCT aggregation
#     """
#     if len(p) == 2:
#         p[0] = Aggregation(aggregation=p[1])
#     else:
#         p[0] = Distinct(p[2])


def p_with_clause(p: yacc.YaccProduction):
    """
    with_clause : WITH with_as_series
    """
    p[0] = WithClause(p[2])


# Removed MATCH node from below
def p_match_pattern(p: yacc.YaccProduction):
    """
    match_pattern : MATCH relationship_chain_list
    | MATCH relationship_chain_list with_clause
    | MATCH relationship_chain_list where
    | MATCH relationship_chain_list with_clause where
    """
    if len(p) == 3:
        p[0] = Match(p[2])
    elif len(p) == 4 and isinstance(p[3], WithClause):
        p[0] = Match(p[2], where_clause=None, with_clause=p[3])
    elif len(p) == 5:
        p[0] = Match(p[2], where_clause=p[4], with_clause=p[3])
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


def p_predicate_argument(p: yacc.YaccProduction) -> None:
    """predicate_argument : object_attribute_lookup
    | literal
    | binary_expression
    | predicate
    | size
    | LPAREN predicate_argument RPAREN
    """
    if len(p) == 2:
        p[0] = p[1]
    elif len(p) == 4:
        p[0] = p[2]
    else:
        raise Exception("This should never happen")


def p_predicate(p: yacc.YaccProduction):
    """predicate : predicate_argument binary_operator predicate_argument
    | NOT predicate_argument
    """

    predicate_dispatch_dict: Dict[str, Type[TreeMixin]] = {
        "=": Equals,
        "<": LessThan,
        ">": GreaterThan,
        "OR": Or,
        "AND": And,
        "NOT": Not,
    }

    # """predicate : object_attribute_lookup binary_operator literal
    # | literal binary_operator object_attribute_lookup
    # | literal binary_operator literal
    # | object_attribute_lookup binary_operator object_attribute_lookup
    # | object_attribute_lookup binary_operator binary_expression
    # | predicate binary_operator predicate
    # | NOT predicate
    # """
    if len(p) == 4:
        p[0] = predicate_dispatch_dict[p[2]](p[1], p[3])
    elif len(p) == 3:
        p[0] = predicate_dispatch_dict["NOT"](p[2])
    else:
        raise ValueError("This should never happen")


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
    | WORD"""  # Need to get rid of this because it's misleading
    if len(p) == 4:
        p[0] = ObjectAttributeLookup(p[1], p[3])
    else:
        p[0] = AliasedName(p[1])


def p_where(p: yacc.YaccProduction):
    """where : WHERE predicate
    | where COMMA predicate"""
    if len(p) == 3:
        p[0] = Where(p[2])
    else:
        p[0] = Where(And(p[1].predicate, p[3]))


def p_return_projection(p: yacc.YaccProduction):
    """return_projection : object_attribute_lookup
    | alias
    | return_projection COMMA alias
    | return_projection COMMA object_attribute_lookup"""
    if len(p) == 2:
        p[0] = ReturnProjection([p[1]])
    else:
        p[0] = p[1]
        p[0].lookups.append(p[3])


def p_return(p: yacc.YaccProduction):
    """return : RETURN return_projection"""
    p[0] = Return(p[2])


CYPHER_PARSER_INSTANCE: yacc.LRParser = yacc.yacc()  # type: ignore


class CypherParser:
    """Parser for Cypher query language."""

    def __init__(self, cypher_query: str):
        """Initialize parser with a Cypher query string.

        Args:
            cypher_query: The Cypher query to parse
        """
        self.cypher_query = cypher_query
        self.parse_tree = None
        self.parse()

    def parse(self) -> Any:
        """Parse the Cypher query and return the parse tree."""
        parsed_result = CYPHER_PARSER_INSTANCE.parse(self.cypher_query)
        self.parse_tree = parsed_result
        return parsed_result

    def evaluate(self, *args, **kwargs) -> Any:
        """Evaluate the parse tree with given arguments."""
        if self.parse_tree is None:
            raise ValueError(
                "Parse tree is not available. Please parse the query first."
            )
        return self.parse_tree.evaluate(*args, **kwargs)

    def _evaluate(self, *args, **kwargs) -> Any:
        """Internal method to evaluate the parse tree with given arguments."""
        if self.parse_tree is None:
            raise ValueError(
                "Parse tree is not available. Please parse the query first."
            )
        return self.parse_tree._evaluate(*args, **kwargs)
