from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, Generator, List, Optional, Tuple, Type

import ply.lex as lex  # type: ignore
import ply.yacc as yacc  # type: ignore
from rich import print as rprint
from rich.tree import Tree

from pycypher.exceptions import CypherParsingError
from pycypher.logger import LOGGER


class State:
    pass


class Constraint:
    @abstractmethod
    def eval(self, *args, **kwargs) -> Any | None:  # type: ignore
        pass


class IsTrue(Constraint):
    """
    Class to represent a constraint that merely says that a ``Predicate`` is true.
    """

    def __init__(self, predicate: Predicate):
        self.predicate = predicate

    def eval(self, *_) -> bool | None:
        pass

    def __repr__(self) -> str:
        return f"IsTrue({self.predicate})"


class HasLabel(Constraint):
    def __init__(self, node_id: str, label: str):
        self.node_id = node_id
        self.label = label

    def __repr__(self):
        return f"HasLabel: {self.node_id} {self.label}"

    def eval(self, state: State) -> bool | None:
        pass

    def __hash__(self) -> int:
        return hash("HasLabel" + self.node.__str__() + self.label.__str__())

    def __eq__(self, other: Any) -> bool:
        return self.node == other.node and self.label == other.label


class HasAttributeWithValue(Constraint):
    def __init__(self, node_id: str, attribute: str, value: Any):
        self.node_id = node_id
        self.attribute = attribute
        self.value = value

    def __repr__(self):
        return f"HasAttributeWithValue: [{self.node_id}] {self.attribute}: {self.value}"

    def __hash__(self) -> int:
        return hash(
            "HasAttributeWithValue"
            + self.node_id
            + self.attribute
            + str(self.value)
        )

    def __eq__(self, other: Any) -> bool:
        return (
            self.node_id == other.node_id
            and self.attribute == other.attribute
            and self.value == other.value
        )  # noqa: E501


class AtomicFact:
    def satisfies(self, constraint: Constraint) -> bool:
        return self._satisfies(constraint)


class NodeHasLabel(AtomicFact):
    def __init__(self, node_id: str, node_label: str):
        self.node_id = node_id
        self.label = node_label

    def __repr__(self):
        return f"NodeHasLabel: {self.node_id} {self.label}"

    def _satisfies(self, constraint: Constraint) -> bool:
        if not isinstance(constraint, HasLabel):
            return False
        return bool(
            constraint.node_id == self.node_id
            and constraint.label == self.label
        )


class NodeHasAttributeWithValue(AtomicFact):
    def __init__(self, node_id: str, attribute: str, value: Any):
        self.node_id = node_id
        self.attribute = attribute
        self.value = value

    def __repr__(self):
        return f"NodeHasAttributeWithValue: {self.node_id} {self.attribute} {self.value}"

    def _satisfies(self, constraint: Constraint) -> bool:
        if not isinstance(constraint, HasAttributeWithValue):
            return False
        return bool(
            constraint.node_id == self.node_id
            and constraint.attribute == self.attribute
            and constraint.value == self.value
        )


class NodeRelatedToNode(AtomicFact):
    def __init__(self, node1_id: str, node2_id: str, relationship_label: str):
        self.node1_id = node1_id
        self.node2_id = node2_id
        self.relationship_label = relationship_label

    def __repr__(self):
        return f"NodeRelatedToNode: {self.node1_id} {self.relationship_label} {self.node2_id}"

    def _satisfies(self, constraint: Constraint) -> bool:
        if not isinstance(constraint, NodeRelatedToNode):
            return False
        return bool(
            constraint.node1_id == self.node1_id
            and constraint.node2_id == self.node2_id
            and constraint.relationship_label == self.relationship_label
        )


class FactCollection:
    def __init__(self, facts: List[AtomicFact]):
        self.facts = facts

    def __repr__(self):
        return f"FactCollection: {len(self.facts)}"

    def satisfies(self, constraint: Constraint) -> bool:
        return any(fact.satisfies(constraint) for fact in self.facts)

    def __len__(self):
        return len(self.facts)


tokens = [
    "COLON",
    "COMMA",
    "DASH",
    "DIVIDE",
    "DOT",
    "DQUOTE",
    "EQUALS",
    "FLOAT",
    "GREATERTHAN",
    "ID",
    "INTEGER",
    "LCURLY",
    "LESSTHAN",
    "LPAREN",
    "LSQUARE",
    "PLUS",
    "RCURLY",
    "RPAREN",
    "RSQUARE",
    "STAR",
    "WORD",
]

# Regular expression rules for simple tokens
t_COLON = r":"
t_COMMA = r","
t_DASH = r"-"
t_DIVIDE = r"/"
t_DOT = r"\."
t_DQUOTE = r'"'
t_EQUALS = r"="
t_FLOAT = r"\d+\.\d+"
t_GREATERTHAN = r">"
t_INTEGER = r"\d+"
t_LCURLY = r"\{"
t_LESSTHAN = r"<"
t_LPAREN = r"\("
t_LSQUARE = r"\["
t_PLUS = r"\+"
t_RCURLY = r"\}"
t_RPAREN = r"\)"
t_RSQUARE = r"\]"
t_STAR = r"\*"

reserved = {
    "AND": "AND",
    "AS": "AS",
    "IF": "IF",
    "MATCH": "MATCH",
    "NOT": "NOT",
    "OR": "OR",
    "RETURN": "RETURN",
    "THEN": "THEN",
    "WHERE": "WHERE",
}

tokens = tokens + list(reserved.values())


def t_WORD(t: lex.LexToken) -> Any:
    r"[a-zA-Z_][a-zA-Z_0-9]*"
    t.type = reserved.get(t.value, "WORD")  # Check for reserved words
    return t


t_ignore = " \t"
lexer = lex.lex()


class TreeMixin:
    parent = None

    def print_tree(self):
        rprint(self.tree())

    @property
    def children(self) -> Generator[TreeMixin | str | None]:
        yield None

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        for child in self.children:
            if child is None:
                continue
            t.add(child.tree())
        return t

    def walk(self) -> Generator[TreeMixin]:
        for child in self.children:
            if child is None:
                continue
            yield child
            if not hasattr(child, "walk"):
                continue
            child.parent = self
            for i in child.walk():
                yield i


class Cypher(TreeMixin):
    def __init__(self, cypher: TreeMixin):
        self.cypher = cypher
        self.aggregated_constraints: List[Constraint] = []

    def gather_constraints(self) -> None:
        for node in self.walk():
            LOGGER.debug(f"Walking node: {node}")
            self.aggregated_constraints += getattr(node, "constraints", [])
        LOGGER.info(f"Got constraints: {self.aggregated_constraints}")

    @property
    def children(self) -> Generator[TreeMixin]:
        yield self.cypher

    def __repr__(self) -> str:
        return f"Cypher({self.cypher})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.cypher.tree())
        return t


class Query(TreeMixin):
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
        t = Tree(self.__class__.__name__)
        t.add(self.match_clause.tree())
        t.add(self.return_clause.tree())
        return t


class Predicate(TreeMixin):
    def __init__(
        self, left_side: TreeMixin, operator: TreeMixin, right_side: TreeMixin
    ):
        self.left_side = left_side
        self.operator = operator
        self.right_side = right_side

    @property
    def children(self) -> Generator[TreeMixin]:
        yield self.left_side
        yield self.right_side

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.left_side}, {self.right_side})"
        )

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class Equals(Predicate):
    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class LessThan(Predicate):
    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class GreaterThan(Predicate):
    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class ObjectAttributeLookup(TreeMixin):
    def __init__(self, object: str, attribute: str):
        self.object = object
        self.attribute = attribute

    def __repr__(self) -> str:
        return f"ObjectAttributeLookup({self.object}, {self.attribute})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.object)
        t.add(self.attribute)
        return t

    @property
    def children(self) -> Generator[str]:
        yield self.object
        yield self.attribute


class Alias(TreeMixin):
    def __init__(self, reference: str, alias: str):
        self.reference = reference
        self.alias = alias

    def __repr__(self):
        return f"Alias({self.reference}, {self.alias})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(
            self.reference.tree()
            if isinstance(self.reference, ObjectAttributeLookup)
            else self.reference
        )
        t.add(self.alias)
        return t

    @property
    def children(self):
        yield self.reference
        yield self.alias


class Match(TreeMixin):
    def __init__(self, pattern: TreeMixin, where: Optional[TreeMixin] = None):
        self.pattern = pattern
        self.where = where

    def __repr__(self) -> str:
        return (
            f"Match({self.pattern})"
            if not self.where
            else f"Match({self.pattern}, {self.where})"
        )

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.pattern.tree())
        if self.where:
            t.add(self.where.tree())
        return t

    @property
    def children(self):
        yield self.pattern
        if self.where:
            yield self.where


class Return(TreeMixin):
    def __init__(self, node: Projection):
        self.projection = node

    def __repr__(self):
        return f"Return({self.projection})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.projection.tree())
        return t

    @property
    def children(self):
        yield self.projection


class Projection(TreeMixin):
    def __init__(self, lookups: List[ObjectAttributeLookup] | None = None):
        self.lookups = lookups or []

    def __repr__(self):
        return f"Projection({self.lookups})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        for lookup in self.lookups:
            t.add(lookup.tree())
        return t

    @property
    def children(self):
        for lookup in self.lookups:
            yield lookup


class NodeNameLabel(TreeMixin):
    def __init__(self, name: str, label: Optional[str] = None):
        self.name = name
        self.label = label

    def __repr__(self):
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


class Node(TreeMixin):
    def __init__(
        self,
        node_name_label: NodeNameLabel,
        mapping_list: Optional[List[Mapping]] = None,
    ):
        self.node_name_label = node_name_label
        self.mapping_list: List[Mapping] | List[None] = mapping_list or []

    @property
    def constraints(self):
        constraint_list: List[Constraint] = []
        if self.node_name_label.label:
            constraint_list.append(
                HasLabel(self.node_name_label.name, self.node_name_label.label)
            )
        if self.mapping_list:
            for mapping in self.mapping_list.mappings:
                constraint_list.append(
                    HasAttributeWithValue(
                        self.node_name_label.name, mapping.key, mapping.value
                    )
                )
        return constraint_list

    def __repr__(self):
        return f"Node({self.node_name_label}, {self.mapping_list})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        if self.node_name_label:
            t.add(self.node_name_label.tree())
        if self.mapping_list:
            t.add(self.mapping_list.tree())
        return t

    @property
    def children(self) -> Generator[NodeNameLabel | Mapping]:
        if self.node_name_label:
            yield self.node_name_label
        if self.mapping_list:
            for mapping in self.mapping_list.mappings:
                yield mapping


class Relationship(TreeMixin):
    def __init__(self, name: str | NodeNameLabel):
        self.name = name

    def __repr__(self):
        return f"Relationship({self.name})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.name.tree())
        return t

    @property
    def children(self):
        yield self.name


class Mapping(TreeMixin):  # This is not complete
    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value

    def __repr__(self):
        return f"Mapping({self.key}:{self.value})"

    def tree(self):
        t = Tree(self.__class__.__name__)
        t.add(self.key)
        t.add(str(self.value))
        return t

    @property
    def constraints(self):
        return [
            HasAttributeWithValue(
                self.parent.node_name_label.name, self.key, self.value
            )
        ]

    @property
    def children(self):
        yield self.key
        yield self.value


class MappingSet(TreeMixin):
    def __init__(self, mappings: List[Mapping]):
        self.mappings: List[Mapping] = mappings

    def __repr__(self) -> str:
        return f"MappingSet({self.mappings})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        for mapping in self.mappings:
            t.add(mapping.tree())
        return t

    @property
    def children(self) -> Generator[Mapping]:
        for mapping in self.mappings:
            yield mapping


class MatchList(TreeMixin):  # Not yet being used
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
    def __init__(self, relationship: Relationship):
        self.relationship = relationship

    def __repr__(self) -> str:
        return f"LeftRight({self.relationship})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.relationship.tree())
        return t

    @property
    def children(self) -> Generator[Relationship]:
        yield self.relationship


class RelationshipRightLeft(TreeMixin):
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


class RelationshipChain(TreeMixin):
    def __init__(self, steps: List[TreeMixin]):
        self.steps = steps

    def __repr__(self):
        return f"RelationshipChain({self.steps})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        for step in self.steps:
            t.add(step.tree())
        return t

    @property
    def children(self) -> Generator[TreeMixin]:
        for step in self.steps:
            yield step


class Where(TreeMixin):
    def __init__(self, predicate: Predicate):
        self.predicate = predicate

    def __repr__(self):
        return f"Where({self.predicate})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.predicate.tree())
        return t

    @property
    def constraints(self):
        return [IsTrue(self.predicate)]


class BinaryBoolean(TreeMixin):
    def __init__(
        self,
        left_side: Predicate | Literal,
        operator: str,
        right_side: Predicate | Literal,
    ):
        self.left_side = left_side
        self.operator = operator
        self.right_side = right_side

    def __repr__(self):
        return (
            f"{self.__class__.__name__}({self.left_side}, {self.right_side})"
        )

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class And(BinaryBoolean):
    def __init__(
        self, left_side: Predicate | Literal, right_side: Predicate | Literal
    ):
        self.left_side = left_side
        self.right_side = right_side

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class Or(BinaryBoolean):
    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        t.add(self.left_side.tree())
        t.add(self.right_side.tree())
        return t


class Literal(TreeMixin):
    def __init__(self, value: Any):
        self.value = value

    def __repr__(self):
        return f"Literal({self.value})"

    def _tree(self):
        t = Tree(self.__class__.__name__)
        t.add(str(self.value))
        return t


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


class RelationshipChainList(TreeMixin):
    def __init__(self, relationships: List[RelationshipChain]):
        self.relationships = relationships

    def __repr__(self):
        return f"RelationshipChainList({self.relationships})"

    def tree(self) -> Tree:
        t = Tree(self.__class__.__name__)
        for relationship in self.relationships:
            t.add(relationship.tree())
        return t

    @property
    def children(self) -> Generator[RelationshipChain]:
        for relationship in self.relationships:
            yield relationship


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


if __name__ == "__main__":
    # @cypher_condition("MATCH (n) RETURN n.foo")
    # def foo(x):
    #     return x + 1

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
    
    # Constraint satisfaction

    fact1 = NodeHasLabel("1", "Thing")
    fact2 = NodeHasAttributeWithValue("1", "key", 2)
    fact3 = NodeRelatedToNode("1", "2", "MyRelationship")
    fact4 = NodeHasLabel("2", "OtherThing")
    fact5 = NodeHasAttributeWithValue("2", "key", 5)

    fact_collection = FactCollection([fact1, fact2, fact3, fact4, fact5])

    node_id_set = set()
    attribute_dict = {}  # {attribute: {node_id: value}}
    relationship_dict = {} # {relationship: [{'source': node1_id, 'target': node2_id} ...]}
    for fact in fact_collection.facts:
        if isinstance(fact, NodeHasLabel):
            node_id_set.add(fact.node_id)
        elif isinstance(fact, NodeHasAttributeWithValue):
            node_id_set.add(fact.node_id)
            if fact.attribute not in attribute_dict:
                attribute_dict[fact.attribute] = {}
            attribute_dict[fact.attribute][fact.node_id] = fact.value
        elif isinstance(fact, NodeRelatedToNode):
            node_id_set.add(fact.node1_id)
            node_id_set.add(fact.node2_id)
            if fact.relationship_label not in relationship_dict:
                relationship_dict[fact.relationship_label] = []
            relationship_dict[fact.relationship_label].append(
                {"source": fact.node1_id, "target": fact.node2_id})
    
    cypher_statement = """MATCH (n:Thing {key1: "value", key2: 5})-[r:MyRelationship]->(m:OtherThing {key3: "hithere"}) WHERE n.key = 2, n.foo = 3 RETURN n.foobar, n.baz"""
    result = CypherParser(cypher_statement)
    constraints = result.parsed.aggregated_constraints

    # Get list of all nodes in constraints
    node_ids = set()
    for constraint in constraints:
        if isinstance(constraint, HasLabel):
            node_ids.add(constraint.node_id)
        elif isinstance(constraint, HasAttributeWithValue):
            node_ids.add(constraint.node_id)
        elif isinstance(constraint, NodeRelatedToNode):
            node_ids.add(constraint.node1_id)
            node_ids.add(constraint.node2_id)
    # Get list of all relationships in constraints
    relationship_labels = set()
    for constraint in constraints:
        if isinstance(constraint, NodeRelatedToNode):
            relationship_labels.add(constraint.relationship_label)
    # Get list of all attributes in constraints
    attributes = set()
    for constraint in constraints:
        if isinstance(constraint, HasAttributeWithValue):
            attributes.add(constraint.attribute)

'''
>>> problem = Problem()
>>> problem.addVariables(["a", "b"], [1, 2])
>>> def func(a, b):
...     return b > a
>>> problem.addConstraint(FunctionConstraint(func), ["a", "b"])
>>> problem.getSolution()
{'a': 1, 'b': 2}
'''



    print(fact_collection.satisfies(HasLabel("1", "Thing")))
    print(fact_collection.satisfies(HasLabel("2", "OtherThing")))
    print(fact_collection.satisfies(HasLabel("2", "NoThisIsFalse")))
