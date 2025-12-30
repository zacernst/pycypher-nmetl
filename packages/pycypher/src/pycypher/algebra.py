from __future__ import annotations
from abc import ABC, abstractmethod
from pydantic import field_validator, BaseModel
from typing import Any, List, Optional
from enum import Enum
import copy
import random
import hashlib
import rich
import pandas as pd

# ID_KEY: str = "__id__"
# VARIABLE_PREFIX: str = "__var__"


def random_hash() -> str:
    return hashlib.md5(bytes(str(random.random()), encoding='utf-8')).hexdigest()


class JoinType(str, Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class Algebraic(BaseModel):
    pass

    # @abstractmethod
    # def to_pandas(self, context: Context) -> pd.DataFrame:
    #     raise NotImplementedError(
    #         "Subclasses must implement to_pandas method"
    #     )


class Table(Algebraic):
    identifier: str = ''
    column_name_to_hash: dict[str, str] = {}
    hash_to_column_name: dict[str, str] = {}

    @field_validator("identifier", mode="after")
    @classmethod
    def set_identifier(cls, v: str) -> str:
        if v == '':
            return random_hash()
        return v


class EntityTable(Table):
    entity_type: str
    attributes: List[str]
    entity_identifier_attribute: str
    variables_to_columns: dict[str, str] = {}

    def __init__(self, **data: Any):
        super().__init__(**data)
        for attribute in self.attributes:
            column_hash: str = random_hash()
            self.column_name_to_hash[attribute] = column_hash
            self.hash_to_column_name[column_hash] = attribute
    
    def to_pandas(self, context: Context) -> pd.DataFrame:
        df: pd.DataFrame = context.obj_map[self.entity_type].rename(
            mapper=self.column_name_to_hash, axis=1,
        )
        return df


class RelationshipTable(Table):
    relationship_type: str
    source_entity_type: str
    target_entity_type: str
    attributes: List[str]
    relationship_identifier_attribute: str




class Context(BaseModel):
    entity_tables: List[EntityTable]
    relationship_tables: List[RelationshipTable]
    obj_map: dict[str, Any] = {}

    def get_entity_table(self, entity_type: str) -> EntityTable:
        for entity_table in self.entity_tables:
            if entity_table.entity_type == entity_type:
                return entity_table
        else:
            raise ValueError(f"Entity table for type {entity_type} not found")



class Node(BaseModel):
    variable: str
    label: str
    properties: dict = {}


    def to_algebra(self, context: Context) -> EntityTable:
        entity_table: Table = context.get_entity_table(self.label)
        entity_table.variables_to_columns[self.variable] = entity_table.column_name_to_hash[entity_table.entity_identifier_attribute]
        return entity_table
    
    def __str__(self) -> str:
        return f"({self.label}:{self.variable})"




    

class ConvertableToAlgebra(ABC, BaseModel):
    @abstractmethod
    def to_algebra(self, context: Context) -> Algebraic:
        raise NotImplementedError(
            "Subclasses must implement to_algebra method"
        )


class Join(Algebraic):
    left: Algebraic
    right: Algebraic
    join_type: JoinType = JoinType.INNER
    left_on: str
    right_on: str
    variable_list: List[str] = []


    def to_pandas(self, context: Context) -> pd.DataFrame:
        left_df: pd.DataFrame = self.left.to_pandas(context)
        right_df: pd.DataFrame = self.right.to_pandas(context)
        if self.join_type == JoinType.INNER:
            import pdb; pdb.set_trace()
            merged_df: pd.DataFrame = pd.merge(
                left_df,
                right_df,
                how="inner",
                left_on=self.left_on,
                right_on=self.right_on,
                suffixes=("_left", "_right"),
            )
        else:
            raise ValueError(f"Unsupported join type: {self.join_type}")
        return merged_df


class Filter(Algebraic):
    table: Table
    condition: str





class Relationship(ConvertableToAlgebra):
    variable: str
    label: str
    properties: Optional[dict] = None
    source_node: Node
    target_node: Node

    def to_algebra(self, context: Context) -> Algebraic:
        relationship_table: Table = context.get_relationship_table(self.label)
        source_table: Table = self.source_node.to_algebra(context)
        target_table: Algebraic = self.target_node.to_algebra(context)
        left_join = Join(
            left=source_table,
            right=relationship_table,
            left_on=source_table.variables_to_columns[self.source_node.variable],
            right_on=relationship_table.column_name_to_hash[relationship_table.source_entity_type],
            join_type=JoinType.INNER,
        )

        right_join = Join(
            left=left_join,
            right=target_table,
            left_on='target_node',
            right_on=f"{VARIABLE_PREFIX}{self.target_node.variable}",
            join_type=JoinType.INNER,
            variable_list=[self.variable] + getattr(source_table, 'variable_list', []) + getattr(target_table, 'variable_list', [])
        )
        return right_join
    
    def __str__(self) -> str:
        return f"({self.source_node.variable})-[:{self.label}]->({self.target_node.variable})"


if __name__ == "__main__":
    entity_table: EntityTable = EntityTable(
        name="Person", entity_type="Person", attributes=["name", "age"],
        entity_identifier_attribute="name"
    )
    person_node: Node = Node(
        variable="p", label="Person", properties={}
    )
    person_df = pd.DataFrame(
        data=[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}], columns=["name", "age"]
    )
    context: Context = Context(
        entity_tables=[entity_table], relationship_tables=[],
        obj_map={"Person": person_df},
    )
    obj = person_node.to_algebra(context)
    rich.print(obj)
    df = obj.to_pandas(context)
    # person_df: pd.DataFrame = pd.DataFrame(
    #     data=[{"name": "Alice", "age": 30, "city": "Cairo"}], columns=["name", "age", "city"]
    # )

    # city_df: pd.DataFrame = pd.DataFrame(
    #     data=[{"name": "Cairo", "population": 100}], columns=["name", "population"]
    # )

    # lives_in_df: pd.DataFrame = pd.DataFrame(
    #     data=[{"source_name": "Alice", "target_name": "Cairo"}],

    # )

    # person_table = EntityTable(
    #     name="Person", entity_type="Person", attributes=["name", "age"],
    #     identifier_column_name="name"
    # )

    # city_table = EntityTable(
    #     name="City", entity_type="City", attributes=["name", "population"],
    #     identifier_column_name="name"
    # )

    # lives_in_table = RelationshipTable(
    #     name="LIVES_IN",
    #     relationship_type="LIVES_IN",
    #     source_entity_type="Person",
    #     target_entity_type="City",
    #     attributes=[],
    #     obj=lives_in_df,
    #     variable_list=["r"],
    # )

    # person_node = Node(
    #     variable="p", label="Person", properties={}
    # )

    # city_node = Node(
    #     variable="c", label="City", properties={}
    # )

    # relationship = Relationship(
    #     variable="r",
    #     label="LIVES_IN",
    #     source_node=person_node,
    #     target_node=city_node,
    # )
    # context = Context(
    #     entity_tables=[person_table, city_table], relationship_tables=[lives_in_table],
    #     obj_map={"Person": person_df, "City": city_df, "LIVES_IN": lives_in_df},
    # )
    # # obj: Algebraic = relationship.to_algebra(context)
    # # print(obj)
    
    # import rich
    # obj: Algebraic = relationship.to_algebra(context)
    # rich.print(obj)
    # print(obj.to_pandas(context))