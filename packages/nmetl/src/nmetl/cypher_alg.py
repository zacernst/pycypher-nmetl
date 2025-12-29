# Translate Cypher statements to relational algebra expressions for use in SQL queries.

from __future__ import annotations
from enum import Enum


class JoinType(Enum):
    FULL_INNER = 'full_inner'
    LEFT_INNER = 'left_inner'
    RIGHT_INNER = 'right_inner'
    LEFT_OUTER = 'left_outer'
    RIGHT_OUTER = 'right_outer'
    FULL_OUTER = 'full_outer'


class Relationship:
    def __init__(self, source_node: Node, target_node: Node, rel_type: str) -> None:
        self.source_node = source_node
        self.target_node = target_node


class Node:
    def __init__(self, label: str = "", properties: dict = {}) -> None:
        self.label = label
        self.properties = properties


class MatchConjunction:
    def __init__(self, patterns: list[Relationship]) -> None:
        self.patterns = patterns


class Join:
    def __init__(self, left: Table, right: Table, join_type: JoinType = JoinType.FULL_INNER) -> None:
        self.left = left
        self.right = right
        self.join_type = join_type


class Table:
    '''Otherwise known as a table.'''
    def __init__(self, name: str) -> None:
        self.name = name


class CharacteristicTable(Table):
    '''Special `Relations` that characterize specific graph elements like nodes or relationships.'''
    def __init__(self, element: Node | Relationship) -> None:
        super().__init__(name=f"Characteristic({element})")
        self.element = element


class Context:
    '''This will be carried across the translation process to maintain state.'''
    def __init__(self) -> None:
        self.variable_mappings: dict[str, Table] = {}
        self.characteristic_tables: list[CharacteristicTable] =  []



if __name__ == "__main__":
    cypher_query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name"
    ra_expression = cypher_to_relational_algebra(cypher_query)
    print("Cypher Query:")
    print(cypher_query)
    print("\nRelational Algebra Expression:")
    print(ra_expression)
